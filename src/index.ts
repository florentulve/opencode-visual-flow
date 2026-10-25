// opencode-visual-flow — materializes agent flow, cooperation, and task
// progression in an HTML dashboard.
//
// Inside the OpenCode server process this plugin:
//   1. hydrates a persisted flow model from ctx.storage,
//   2. backfills sessions from ctx.session.list(),
//   3. folds the live event stream (ctx.event.subscribe) into the model,
//   4. exposes the model over RPC (flow.snapshot / flow.node / flow.changed),
//   5. registers the `flow_task` tool agents use to declare task progression,
//   6. serves the dashboard + API proxy via a local HTTP sidecar.
import { Plugin } from "@opencode/plugin"
import { Flow } from "./rpc"
import { FlowStore } from "./flow"
import { serviceEndpoint, startSidecar } from "./sidecar"

export default Plugin.define({
  id: "opencode-visual-flow",
  async setup(ctx) {
    const store = new FlowStore()

    // 1. hydrate persisted model (survives restarts)
    try {
      const saved = await ctx.storage.get("model")
      if (saved) store.hydrate(saved)
    } catch (e) {
      console.error("[flow] hydrate failed:", e)
    }

    // 2. seed from session list across ALL locations the service hosts
    try {
      const sessions: any[] = []
      let cursor: string | undefined
      for (;;) {
        const page: any = await ctx.session.list({
          limit: 200,
          order: "desc",
          ...(cursor ? { cursor } : {}),
        } as any)
        sessions.push(...(page?.data ?? []))
        cursor = page?.cursor?.next ?? undefined
        if (!cursor) break
      }
      // server truth on what is running right now (guards against missed
      // events during reloads — sessions mid-run are marked running)
      let active: Record<string, unknown> = {}
      let activeKnown = false
      try {
        const ep = serviceEndpoint()
        if (ep.url) {
          const r = await fetch(ep.url.replace(/\/+$/, "") + "/api/session/active", {
            headers: ep.authorization ? { authorization: ep.authorization } : {},
            signal: AbortSignal.timeout(5000),
          })
          if (r.ok) {
            active = (await r.json())?.data ?? {}
            activeKnown = true
          }
        }
      } catch (e) {
        console.error("[flow] active fetch failed:", e)
      }
      store.seed(sessions, active, activeKnown)
    } catch (e) {
      console.error("[flow] seed failed:", e)
    }

    // throttled dirty handling: RPC change event + durable save
    let notifyTimer: ReturnType<typeof setTimeout> | undefined
    let saveTimer: ReturnType<typeof setTimeout> | undefined
    let rpcReg: Awaited<ReturnType<typeof ctx.rpc.register>> | undefined
    store.onDirty = () => {
      if (notifyTimer === undefined) {
        notifyTimer = setTimeout(() => {
          notifyTimer = undefined
          rpcReg?.events.emit("changed", { rev: store.model.rev }).catch(() => {})
        }, 150)
      }
      if (saveTimer === undefined) {
        saveTimer = setTimeout(() => {
          saveTimer = undefined
          ctx.storage.set("model", store.snapshot()).catch((e) => console.error("[flow] save failed:", e))
        }, 2000)
      }
    }

    // live activity streaming: text deltas buffered per session, flushed ~120ms
    const textBuf = new Map<string, string>()
    let flushTimer: ReturnType<typeof setTimeout> | undefined
    const flushText = () => {
      flushTimer = undefined
      for (const [sid, chunk] of textBuf) {
        if (chunk) {
          rpcReg?.events.emit("activity", { sessionID: sid, kind: "text", data: { text: chunk } }).catch(() => {})
        }
      }
      textBuf.clear()
    }
    store.onActivity = (sid, kind, data) => {
      if (kind === "text") {
        textBuf.set(sid, (textBuf.get(sid) ?? "") + String(data?.delta ?? ""))
        if (flushTimer === undefined) flushTimer = setTimeout(flushText, 120)
        return
      }
      rpcReg?.events.emit("activity", { sessionID: sid, kind, data: data ?? {} }).catch(() => {})
    }

    // 3. RPC surface
    rpcReg = await ctx.rpc.register(Flow, {
      snapshot: async () => store.snapshot(),
      node: async (input, context) => {
        const id = (input as any)?.id
        const node = typeof id === "string" ? store.node(id) : undefined
        if (!node) return context.error("not_found", "unknown session", { id: String(id ?? "") })
        return node
      },
    })

    // 4. flow_task tool (effective name: flow_task)
    const toolReg = await ctx.tool.transform((editor) => {
      editor.namespace({
        name: "flow",
        description: "Shared agent-flow dashboard: declare and progress cooperative tasks",
      })
      editor.add({
        name: "task",
        description:
          "Report task progression to the shared flow dashboard. Call when you START a task, on meaningful PROGRESS, and when you finish (done) or give up (fail). Keep `task` short and stable across calls so updates merge into one card.",
        input: {
          type: "object",
          properties: {
            action: { type: "string", enum: ["start", "progress", "done", "fail"] },
            task: { type: "string", description: "Short stable task title, e.g. 'implement reducer'" },
            detail: { type: "string", description: "One-line current status or result" },
          },
          required: ["action", "task"],
          additionalProperties: false,
        },
        options: { namespace: "flow" },
        execute: async (input, context) => {
          const i = input as { action: string; task: string; detail?: string }
          store.task(context.sessionID, i.action, i.task, i.detail)
          return { content: `flow: task "${i.task}" → ${i.action}` }
        },
      })
    })

    // 5. event subscription
    const ac = new AbortController()
    void (async () => {
      try {
        for await (const ev of ctx.event.subscribe({ signal: ac.signal })) {
          store.apply(ev)
        }
      } catch (e) {
        if (!ac.signal.aborted) console.error("[flow] event loop ended:", e)
      }
    })()

    // 6. sidecar: serves the dashboard and proxies /api/* to this service
    const sidecar = startSidecar({
      port: (ctx.options as any)?.port ?? 7337,
      directory: ctx.location.directory,
      meta: () => ({ rev: store.model.rev, nodes: Object.keys(store.model.nodes).length }),
      snapshot: () => store.snapshot(),
    })

    console.log(
      `[flow] loaded rev=${store.model.rev} nodes=${Object.keys(store.model.nodes).length} ` +
        `location=${ctx.location.directory} dashboard=${sidecar.url ?? "unavailable"}`,
    )

    return async () => {
      ac.abort()
      clearTimeout(flushTimer)
      try {
        await toolReg.dispose()
      } catch {}
      try {
        await rpcReg?.dispose()
      } catch {}
      sidecar.close()
      try {
        await ctx.storage.set("model", store.snapshot())
      } catch {}
    }
  },
})