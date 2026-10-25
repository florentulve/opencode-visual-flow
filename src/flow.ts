// Flow model + reducer: folds the OpenCode V2 event stream into a small
// dashboard graph (sessions as lanes, subagent spawn edges, tool activity
// ticks, usage, verdicts, and flow_task-declared tasks).
//
// Pure and synchronous — event handling never touches the network, so the
// same store can be unit-tested by replaying captured events.

export type FlowStatus =
  | "queued"
  | "running"
  | "waiting"
  | "idle"
  | "done"
  | "failed"
  | "interrupted"

export interface FlowTask {
  task: string
  state: "active" | "done" | "failed"
  detail?: string
  at: number
}

export interface FlowNode {
  id: string
  parentID?: string
  title: string
  agent: string
  model: string
  dir?: string
  status: FlowStatus
  createdAt: number
  updatedAt: number
  startedAt?: number
  endedAt?: number
  error?: string
  cost: number
  tokens: { input: number; output: number; reasoning: number; cacheRead: number; cacheWrite: number }
  steps: number
  toolsRun: number
  toolsFailed: number
  permissions: number
  inbox: number
  current?: string
  lastText?: string
  verdict?: string
  flags: string[]
  tasks: FlowTask[]
}

export interface FlowTick {
  id: string
  sessionID: string
  name: string
  status: "running" | "ok" | "error"
  at: number
  ms?: number
  detail?: string
}

export interface FlowEdge {
  from: string
  to: string
  at: number
}

export interface FlowModel {
  rev: number
  nodes: Record<string, FlowNode>
  edges: FlowEdge[]
  ticks: FlowTick[]
  errors: string[]
}

const MAX_TICKS = 1200
const MAX_ERRORS = 20
const MAX_FLAGS = 6
const MAX_TASKS = 24
const MAX_TEXT = 400
const MAX_PENDING = 256

const REVIEW_AGENT = /review|critic|synthes/i
const RUNNINGISH: ReadonlySet<FlowStatus> = new Set(["running", "waiting", "queued"])

function zeroTokens() {
  return { input: 0, output: 0, reasoning: 0, cacheRead: 0, cacheWrite: 0 }
}

function blankNode(id: string, at: number): FlowNode {
  return {
    id,
    title: id,
    agent: "?",
    model: "?",
    status: "queued",
    createdAt: at,
    updatedAt: at,
    cost: 0,
    tokens: zeroTokens(),
    steps: 0,
    toolsRun: 0,
    toolsFailed: 0,
    permissions: 0,
    inbox: 0,
    flags: [],
    tasks: [],
  }
}

// Summarize a tool input into a short human-readable detail string.
export function detailOf(name: string | undefined, input: any): string | undefined {
  const i = input ?? {}
  const pick = (...keys: string[]): string | undefined => {
    for (const k of keys) {
      const v = i[k]
      if (typeof v === "string" && v.trim()) return v
    }
    return undefined
  }
  let d: string | undefined
  switch (name) {
    case "bash":
      d = pick("command")
      break
    case "read":
    case "write":
    case "edit":
    case "list":
      d = pick("filePath", "file_path", "path")
      break
    case "grep":
    case "glob":
      d = pick("pattern")
      break
    case "task":
    case "subagent":
      d = pick("description", "agent", "prompt")
      break
    case "webfetch":
      d = pick("url")
      break
    case "websearch":
      d = pick("query")
      break
    default:
      d = pick("description", "prompt", "command", "filePath", "file_path", "pattern", "query", "url", "agent")
  }
  if (d === undefined) {
    for (const v of Object.values(i)) {
      if (typeof v === "string" && v.trim()) {
        d = v
        break
      }
    }
  }
  return d ? d.replace(/\s+/g, " ").trim().slice(0, 120) : undefined
}

// Pull a VERDICT line and ⚑/FLAG lines out of reviewer/critic output.
export function extractVerdict(text: string): { verdict?: string; flags: string[] } {
  let verdict: string | undefined
  const flags: string[] = []
  for (const raw of text.split(/\n/)) {
    const line = raw.trim()
    if (!verdict && /^VERDICT\b/i.test(line)) verdict = line.slice(0, 160)
    else if (/^⚑/.test(line) || /^FLAG\b/i.test(line)) {
      if (flags.length < MAX_FLAGS) flags.push(line.slice(0, 160))
    }
  }
  return { verdict, flags }
}

function actionText(name: string | undefined, detail: string | undefined): string {
  const n = name ?? "tool"
  return detail ? `${n}: ${detail}` : n
}

export class FlowStore {
  readonly model: FlowModel = { rev: 0, nodes: {}, edges: [], ticks: [], errors: [] }

  // Called (throttled by the owner) after every state change.
  onDirty?: () => void
  // Called on live per-session activity — used to stream hooked-in feeds.
  // kind: "tool" | "text" | "step" | "status"
  onActivity?: (sessionID: string, kind: "tool" | "text" | "step" | "status", data: any) => void

  private pendingInput = new Map<string, { name?: string; text?: string; input?: any }>()
  private known = new Set<string>()

  constructor() {}

  node(id: string): FlowNode | undefined {
    return this.model.nodes[id]
  }

  snapshot(): FlowModel {
    return this.model
  }

  // Loosely-validated import of a persisted model.
  hydrate(raw: unknown) {
    if (!raw || typeof raw !== "object") return
    const m = raw as Partial<FlowModel>
    if (typeof m.rev === "number") this.model.rev = m.rev
    if (m.nodes && typeof m.nodes === "object") {
      for (const [id, n] of Object.entries(m.nodes)) {
        if (!n || typeof n !== "object") continue
        const node = n as FlowNode
        node.id = id
        node.flags ??= []
        node.tasks ??= []
        node.tokens ??= zeroTokens()
        this.model.nodes[id] = node
      }
    }
    if (Array.isArray(m.edges)) this.model.edges = m.edges.slice(-400)
    if (Array.isArray(m.ticks)) this.model.ticks = m.ticks.slice(-MAX_TICKS)
    if (Array.isArray(m.errors)) this.model.errors = m.errors.slice(-MAX_ERRORS)
    this.known = new Set(Object.keys(this.model.nodes))
  }

  // Backfill: sessions that predate this plugin load. Stored task/verdict/
  // lastText state wins; identity + usage come from the session info.
  seed(list: any[], _active: Record<string, unknown> = {}) {
    for (const s of list) {
      if (!s?.id || typeof s.id !== "string") continue
      const at = s.time?.created ?? Date.now()
      const stored = this.model.nodes[s.id]
      const node = stored ?? blankNode(s.id, at)
      if (!stored) this.model.nodes[s.id] = node
      if (s.parentID) {
        node.parentID = s.parentID
        this.addEdge(s.parentID, s.id, at)
      }
      if (s.location?.directory) node.dir = String(s.location.directory)
      if (s.title) node.title = s.title
      if (s.agent) node.agent = s.agent
      if (s.model) node.model = `${s.model.providerID}/${s.model.id}`
      if (s.time?.created) node.createdAt = s.time.created
      if (s.time?.updated) node.updatedAt = Math.max(node.updatedAt, s.time.updated)
      if (typeof s.cost === "number") node.cost = s.cost
      if (s.tokens && typeof s.tokens === "object") {
        node.tokens = {
          input: s.tokens.input ?? node.tokens.input ?? 0,
          output: s.tokens.output ?? node.tokens.output ?? 0,
          reasoning: s.tokens.reasoning ?? node.tokens.reasoning ?? 0,
          cacheRead: s.tokens.cache?.read ?? node.tokens.cacheRead ?? 0,
          cacheWrite: s.tokens.cache?.write ?? node.tokens.cacheWrite ?? 0,
        }
      }
      if (s.outcome === "succeeded") node.status = "done"
      else if (s.outcome === "failed") node.status = "failed"
      else if (s.outcome === "interrupted") node.status = "interrupted"
      else if (!stored || node.status === "queued") node.status = "idle"
      this.known.add(s.id)
    }
    this.bump()
  }

  // flow_task tool entry: cooperative task declarations.
  task(sessionID: string, action: string, task: string, detail?: string) {
    const node = this.ensure(sessionID, Date.now())
    const at = Date.now()
    const key = String(task).trim().toLowerCase()
    let entry = node.tasks.find((x) => x.task.trim().toLowerCase() === key)
    if (!entry) {
      entry = { task: String(task), state: "active", at }
      node.tasks.push(entry)
    }
    if (action === "start") entry.state = "active"
    else if (action === "progress") entry.state = entry.state === "failed" ? "failed" : "active"
    else if (action === "done") entry.state = "done"
    else if (action === "fail") entry.state = "failed"
    entry.detail = detail
    entry.at = at
    if (node.tasks.length > MAX_TASKS) node.tasks.splice(0, node.tasks.length - MAX_TASKS)
    this.bump()
  }

  // The single event entrypoint. Unknown event types are ignored.
  // Events from ALL locations are folded — the dashboard shows every session
  // the service hosts, tagged with its project directory.
  apply(ev: any) {
    const t = ev?.type
    if (typeof t !== "string") return
    if (t.startsWith("rpc.")) return

    const data = ev.data ?? {}
    const at = typeof ev.created === "number" ? ev.created : Date.now()
    const sid: string | undefined = data.sessionID
    let handled = false

    switch (t) {
      case "session.created": {
        const node = this.ensure(data.sessionID, at)
        const dir = data.location?.directory ?? ev.location?.directory
        if (dir) node.dir = String(dir)
        if (data.parentID) {
          node.parentID = data.parentID
          this.addEdge(data.parentID, data.sessionID, at)
        }
        if (data.title) node.title = data.title
        if (data.agent) node.agent = data.agent
        if (data.model) node.model = `${data.model.providerID}/${data.model.id}`
        this.known.add(data.sessionID)
        handled = true
        break
      }
      case "session.renamed": {
        const node = sid ? this.model.nodes[sid] : undefined
        if (node && data.title) {
          node.title = data.title
          handled = true
        }
        break
      }
      case "session.execution.started": {
        if (!sid || !this.known.has(sid)) break
        const node = this.ensure(sid, at)
        node.status = "running"
        node.startedAt = at
        this.onActivity?.(sid, "status", { status: "running" })
        handled = true
        break
      }
      case "session.execution.succeeded": {
        if (!sid) break
        const node = this.ensure(sid, at)
        node.status = "done"
        node.endedAt = at
        this.onActivity?.(sid, "status", { status: "done" })
        handled = true
        break
      }
      case "session.execution.failed": {
        if (!sid) break
        const node = this.ensure(sid, at)
        node.status = "failed"
        node.endedAt = at
        if (data.error?.message) node.error = String(data.error.message).slice(0, 200)
        this.onActivity?.(sid, "status", { status: "failed" })
        handled = true
        break
      }
      case "session.execution.interrupted": {
        if (!sid) break
        const node = this.ensure(sid, at)
        node.status = "interrupted"
        node.endedAt = at
        this.onActivity?.(sid, "status", { status: "interrupted" })
        handled = true
        break
      }
      case "session.idle": {
        if (!sid) break
        const node = this.model.nodes[sid]
        if (node && RUNNINGISH.has(node.status)) {
          node.status = "idle"
          this.onActivity?.(sid, "status", { status: "idle" })
          handled = true
        }
        break
      }
      case "session.status.updated": {
        if (!sid) break
        const node = this.model.nodes[sid]
        if (!node) break
        const st = data.status
        const kind = typeof st === "string" ? st : st?.type
        if (kind === "idle") {
          if (RUNNINGISH.has(node.status)) {
            node.status = "idle"
            handled = true
          }
        } else if (typeof kind === "string" && /^(busy|running|active|retry|working)$/i.test(kind)) {
          if (RUNNINGISH.has(node.status) || node.status === "idle") {
            node.status = "running"
            handled = true
          }
        }
        break
      }
      case "session.step.started": {
        if (!sid || !this.known.has(sid)) break
        const node = this.ensure(sid, at)
        node.steps++
        if (node.status !== "running" && node.status !== "done" && node.status !== "failed") node.status = "running"
        this.onActivity?.(sid, "step", { phase: "started" })
        handled = true
        break
      }
      case "session.step.failed": {
        if (!sid) break
        const node = this.model.nodes[sid]
        if (node && data.error?.message) {
          node.error = String(data.error.message).slice(0, 200)
          this.model.errors.push(`[${node.agent}] ${node.error}`)
          if (this.model.errors.length > MAX_ERRORS) this.model.errors.splice(0, this.model.errors.length - MAX_ERRORS)
          this.onActivity?.(sid, "step", { phase: "failed", error: node.error })
          handled = true
        }
        break
      }
      case "session.tool.input.started": {
        if (!sid || !this.known.has(sid)) break
        const node = this.ensure(sid, at)
        this.pendingInput.set(data.id, { name: data.name })
        if (this.pendingInput.size > MAX_PENDING) {
          const oldest = this.pendingInput.keys().next().value
          if (oldest !== undefined) this.pendingInput.delete(oldest)
        }
        let tick = this.tick(data.id)
        if (!tick) tick = this.pushTick(data.id, sid, data.name ?? "tool", at)
        else tick.status = "running"
        node.updatedAt = at
        this.onActivity?.(sid, "tool", { id: data.id, name: tick.name, status: "running", at })
        handled = true
        break
      }
      case "session.tool.input.ended": {
        const p = this.pendingInput.get(data.id)
        if (p) {
          p.text = typeof data.text === "string" ? data.text : undefined
          handled = true
        }
        break
      }
      case "session.tool.called": {
        if (!sid || !this.known.has(sid)) break
        const node = this.ensure(sid, at)
        let p = this.pendingInput.get(data.id)
        if (!p) {
          p = {}
          this.pendingInput.set(data.id, p)
        }
        p.input = data.input
        const detail = detailOf(p.name, data.input)
        const tick = this.tick(data.id) ?? this.pushTick(data.id, sid, p.name ?? "tool", at)
        if (detail) tick.detail = detail
        node.current = actionText(p.name ?? tick.name, detail)
        node.updatedAt = at
        this.onActivity?.(sid, "tool", { id: data.id, name: tick.name, detail, status: tick.status, at: tick.at })
        handled = true
        break
      }
      case "session.tool.progress": {
        const tick = this.tick(data.id)
        if (tick) {
          const meta = data.metadata ?? {}
          const d =
            typeof meta.summary === "string"
              ? meta.summary
              : typeof meta.output === "string"
                ? meta.output
                : typeof meta.description === "string"
                  ? meta.description
                  : undefined
          if (d) tick.detail = d.replace(/\s+/g, " ").trim().slice(0, 120)
          const node = this.model.nodes[tick.sessionID]
          if (node) node.current = actionText(tick.name, tick.detail)
          handled = true
        }
        break
      }
      case "session.tool.success": {
        if (!sid) break
        const node = this.model.nodes[sid]
        if (!node) break
        let tick = this.tick(data.id)
        if (!tick) {
          const p = this.pendingInput.get(data.id)
          tick = this.pushTick(data.id, sid, p?.name ?? "tool", at)
        }
        tick.status = "ok"
        tick.ms = Math.max(0, at - tick.at)
        node.toolsRun++
        node.updatedAt = at
        this.pendingInput.delete(data.id)
        this.onActivity?.(sid, "tool", { id: data.id, name: tick.name, status: "ok", ms: tick.ms, detail: tick.detail })
        handled = true
        break
      }
      case "session.tool.failed": {
        if (!sid) break
        const node = this.model.nodes[sid]
        if (!node) break
        let tick = this.tick(data.id)
        if (!tick) {
          const p = this.pendingInput.get(data.id)
          tick = this.pushTick(data.id, sid, p?.name ?? "tool", at)
        }
        tick.status = "error"
        tick.ms = Math.max(0, at - tick.at)
        if (data.error?.message) tick.detail = String(data.error.message).slice(0, 120)
        node.toolsFailed++
        node.updatedAt = at
        this.pendingInput.delete(data.id)
        this.onActivity?.(sid, "tool", { id: data.id, name: tick.name, status: "error", ms: tick.ms, detail: tick.detail })
        handled = true
        break
      }
      case "session.text.delta": {
        if (!sid) break
        const node = this.model.nodes[sid]
        if (!node) break
        if (typeof data.delta === "string") {
          node.lastText = (node.lastText ?? "") + data.delta
          if (node.lastText.length > MAX_TEXT) node.lastText = node.lastText.slice(-MAX_TEXT)
          node.updatedAt = at
          this.onActivity?.(sid, "text", { delta: data.delta })
          handled = true
        }
        break
      }
      case "session.text.ended": {
        if (!sid) break
        const node = this.model.nodes[sid]
        if (!node || typeof data.text !== "string") break
        node.lastText = data.text.slice(0, MAX_TEXT)
        if (REVIEW_AGENT.test(node.agent)) {
          const { verdict, flags } = extractVerdict(data.text)
          if (verdict) node.verdict = verdict
          if (flags.length) node.flags = flags
        }
        node.updatedAt = at
        handled = true
        break
      }
      case "session.usage.updated": {
        if (!sid) break
        const node = this.model.nodes[sid]
        if (!node) break
        if (typeof data.cost === "number") node.cost = data.cost
        const tk = data.tokens
        if (tk && typeof tk === "object") {
          node.tokens = {
            input: tk.input ?? node.tokens.input,
            output: tk.output ?? node.tokens.output,
            reasoning: tk.reasoning ?? node.tokens.reasoning,
            cacheRead: tk.cache?.read ?? node.tokens.cacheRead,
            cacheWrite: tk.cache?.write ?? node.tokens.cacheWrite,
          }
        }
        handled = true
        break
      }
      case "session.inbox.enqueued": {
        if (!sid) break
        const node = this.model.nodes[sid]
        if (!node) break
        node.inbox++
        node.updatedAt = at
        handled = true
        break
      }
      case "permission.asked": {
        if (!sid) break
        const node = this.ensure(sid, at)
        node.permissions++
        if (RUNNINGISH.has(node.status)) node.status = "waiting"
        this.onActivity?.(sid, "status", { status: "waiting" })
        handled = true
        break
      }
      case "permission.replied": {
        if (!sid) break
        const node = this.model.nodes[sid]
        if (node && node.status === "waiting") {
          node.status = "running"
          this.onActivity?.(sid, "status", { status: "running" })
          handled = true
        }
        break
      }
      case "session.deleted": {
        if (!sid || !this.model.nodes[sid]) break
        delete this.model.nodes[sid]
        this.known.delete(sid)
        this.model.edges = this.model.edges.filter((e) => e.from !== sid && e.to !== sid)
        handled = true
        break
      }
      default:
        break
    }

    if (handled) this.bump()
  }

  // ------------------------------------------------------------------ private

  private ensure(id: string, at: number): FlowNode {
    let n = this.model.nodes[id]
    if (!n) {
      n = blankNode(id, at)
      this.model.nodes[id] = n
    }
    return n
  }

  private addEdge(from: string, to: string, at: number) {
    if (this.model.edges.some((e) => e.from === from && e.to === to)) return
    this.model.edges.push({ from, to, at })
  }

  private tick(id: string): FlowTick | undefined {
    const ticks = this.model.ticks
    for (let i = ticks.length - 1; i >= 0; i--) {
      if (ticks[i].id === id) return ticks[i]
    }
    return undefined
  }

  private pushTick(id: string, sessionID: string, name: string, at: number): FlowTick {
    const tick: FlowTick = { id, sessionID, name, status: "running", at }
    this.model.ticks.push(tick)
    if (this.model.ticks.length > MAX_TICKS) this.model.ticks.splice(0, this.model.ticks.length - MAX_TICKS)
    return tick
  }

  private bump() {
    this.model.rev++
    this.onDirty?.()
  }
}