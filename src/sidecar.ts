// Tiny in-process HTTP sidecar for the flow dashboard.
//  - GET  /            → dashboard/index.html (served from disk)
//  - GET  /snapshot    → current flow model directly (debug convenience)
//  - GET  /healthz     → { ok, upstream, rev, nodes }
//  - /*   /api/*       → proxied to the OpenCode service with Basic auth read
//                        (and watched) from the service registration file.
//
// Runs on 127.0.0.1 only. Port falls back +1..+20 when the preferred port is
// taken (e.g. by the older standalone bridge).
import http from "node:http"
import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { Readable } from "node:stream"

const SERVICE_FILE =
  process.env.OPENCODE_SERVICE_JSON ||
  path.join(process.env.XDG_STATE_HOME ?? path.join(os.homedir(), ".local", "state"), "opencode", "service.json")

interface ServiceInfo {
  url?: string
  password?: string
}

function loadService(): ServiceInfo {
  try {
    return JSON.parse(fs.readFileSync(SERVICE_FILE, "utf8")) as ServiceInfo
  } catch {
    return {}
  }
}

export interface SidecarOptions {
  port: number
  /** Plugin location directory — used to locate dashboard/index.html. */
  directory: string
  meta?: () => { rev: number; nodes: number }
}

export interface Sidecar {
  url?: string
  port?: number
  close(): void
}

function dashboardCandidates(directory: string): string[] {
  let fromModule: string | undefined
  try {
    fromModule = path.resolve(path.dirname(new URL(import.meta.url).pathname), "..", "dashboard", "index.html")
  } catch {}
  const list = [
    process.env.OPENCODE_FLOW_DASHBOARD,
    fromModule,
    path.join(directory, "opencode-visual-flow", "dashboard", "index.html"),
    path.join(directory, ".opencode", "plugins", "opencode-visual-flow", "dashboard", "index.html"),
  ].filter((p): p is string => typeof p === "string" && p.length > 0)
  return [...new Set(list)]
}

export function startSidecar(opts: SidecarOptions): Sidecar {
  const state = loadService()
  const marker = path.join(path.dirname(SERVICE_FILE), "flow-dashboard.json")
  let reloadTimer: ReturnType<typeof setTimeout> | undefined
  let watcher: fs.FSWatcher | undefined

  const dashboardPath = dashboardCandidates(opts.directory).find((p) => {
    try {
      return fs.existsSync(p)
    } catch {
      return false
    }
  })

  const authHeader = (): string | undefined =>
    state.password === undefined ? undefined : "Basic " + Buffer.from(`opencode:${state.password}`).toString("base64")

  const handler = async (req: http.IncomingMessage, res: http.ServerResponse) => {
    const url = new URL(req.url ?? "/", "http://sidecar")
    const p = url.pathname

    if (p === "/" || p === "/index.html") {
      if (!dashboardPath) {
        res.writeHead(500, { "content-type": "text/plain" })
        res.end("flow: dashboard/index.html not found (set OPENCODE_FLOW_DASHBOARD)")
        return
      }
      try {
        const html = fs.readFileSync(dashboardPath)
        res.writeHead(200, { "content-type": "text/html; charset=utf-8", "cache-control": "no-store" })
        res.end(html)
      } catch (e) {
        res.writeHead(500, { "content-type": "text/plain" })
        res.end("flow: " + String((e as Error).message ?? e))
      }
      return
    }

    if (p === "/healthz") {
      const meta = opts.meta?.() ?? { rev: 0, nodes: 0 }
      res.writeHead(200, { "content-type": "application/json" })
      res.end(JSON.stringify({ ok: true, upstream: state.url ?? null, ...meta }))
      return
    }

    if (p === "/snapshot") {
      res.writeHead(200, { "content-type": "application/json" })
      res.end(JSON.stringify(opts.meta ? opts.meta() : { rev: 0, nodes: 0 }))
      return
    }

    if (p.startsWith("/api/")) {
      if (!state.url) {
        res.writeHead(502, { "content-type": "text/plain" })
        res.end("flow: opencode service not discovered (missing " + SERVICE_FILE + ")")
        return
      }

      // RPC routes are location-scoped: default the plugin's own location so
      // the browser never needs to know where the plugin is loaded.
      let target = req.url ?? "/"
      if (p.startsWith("/api/rpc/") && !url.searchParams.has("location[directory]")) {
        const u2 = new URL(target, "http://sidecar")
        u2.searchParams.set("location[directory]", opts.directory)
        target = u2.pathname + u2.search
      }

      const chunks: Buffer[] = []
      for await (const c of req) chunks.push(c as Buffer)
      const body = chunks.length ? Buffer.concat(chunks) : undefined

      const headers: Record<string, string> = { accept: req.headers.accept ?? "*/*" }
      const auth = authHeader()
      if (auth) headers.authorization = auth
      if (body) headers["content-type"] = req.headers["content-type"] ?? "application/json"

      const ac = new AbortController()
      res.on("close", () => ac.abort())
      let upstream: Response
      try {
        upstream = await fetch(state.url.replace(/\/+$/, "") + target, {
          method: req.method,
          headers,
          body: body && req.method !== "GET" && req.method !== "HEAD" ? new Uint8Array(body) : undefined,
          signal: ac.signal,
          redirect: "follow",
        })
      } catch (e) {
        if (!res.headersSent) {
          res.writeHead(502, { "content-type": "text/plain" })
          res.end("flow: upstream unreachable: " + String((e as Error).message ?? e))
        } else {
          res.destroy()
        }
        return
      }

      const out: Record<string, string> = {
        "content-type": upstream.headers.get("content-type") ?? "text/plain",
      }
      if (p === "/api/event") {
        out["cache-control"] = "no-cache"
        out["x-accel-buffering"] = "no"
        out["connection"] = "keep-alive"
      }
      res.writeHead(upstream.status, out)
      if (upstream.body) {
        Readable.fromWeb(upstream.body as any)
          .on("error", () => res.destroy())
          .pipe(res)
      } else {
        res.end()
      }
      return
    }

    res.writeHead(404, { "content-type": "text/plain" })
    res.end("flow: not found")
  }

  const server = http.createServer(handler)
  const server6 = http.createServer(handler)   // IPv6 loopback (localhost may resolve to ::1)
  server6.on("error", () => {})                // no IPv6 support -> IPv4 only, fine

  // Watch the service registration for url/password rotation.
  try {
    watcher = fs.watch(path.dirname(SERVICE_FILE), (_event, filename) => {
      if (filename && filename !== "service.json") return
      clearTimeout(reloadTimer)
      reloadTimer = setTimeout(() => Object.assign(state, loadService()), 250)
    })
  } catch {
    // state dir not watchable — reload happens on demand via /healthz checks
  }

  // Listen with port fallback.
  const listen = (): Promise<number> =>
    new Promise((resolve, reject) => {
      const ports: number[] = []
      for (let i = 0; i <= 20; i++) ports.push(opts.port + i)
      const attempt = (i: number) => {
        if (i >= ports.length) {
          reject(new Error("flow: no free port in " + ports[0] + ".." + ports[ports.length - 1]))
          return
        }
        const port = ports[i]
        const onError = (err: NodeJS.ErrnoException) => {
          server.removeListener("listening", onListening)
          if (err.code === "EADDRINUSE") attempt(i + 1)
          else reject(err)
        }
        const onListening = () => {
          server.removeListener("error", onError)
          resolve(port)
        }
        server.once("error", onError)
        server.once("listening", onListening)
        server.listen(port, "127.0.0.1")
      }
      attempt(0)
    })

  const handle: Sidecar = {
    close() {
      clearTimeout(reloadTimer)
      watcher?.close()
      try {
        fs.unlinkSync(marker)
      } catch {}
      try {
        server.close()
      } catch {}
      try {
        server6.close()
      } catch {}
    },
  }

  listen()
    .then((port) => {
      handle.port = port
      handle.url = `http://127.0.0.1:${port}`
      try {
        server6.listen(port, "::1")
      } catch {}
      console.log(`[flow] dashboard at ${handle.url}${dashboardPath ? "" : " (warning: dashboard html not found)"}`)
      try {
        fs.writeFileSync(
          marker,
          JSON.stringify({ url: handle.url, port, pid: process.pid, at: Date.now() }, null, 2),
        )
      } catch {}
    })
    .catch((e) => {
      console.error("[flow] sidecar failed:", e instanceof Error ? e.message : e)
    })

  return handle
}