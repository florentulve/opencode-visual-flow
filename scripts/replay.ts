// Smoke test: replay captured V2 events (JSONL) through the FlowStore and
// print a summary. Usage: bun run scripts/replay.ts [events.jsonl]
//
// Each line is either the raw JSON event or "FIRST {json}" / "AGAIN {type}"
// as written by the event-capture probe.
import { FlowStore } from "../src/flow"

const file = process.argv[2] ?? "/tmp/opencode/explore/events.log"
const text = await Bun.file(file).text()

const store = new FlowStore()
let applied = 0
let skipped = 0
let unknown = 0
const seen = new Set<string>()

for (const raw of text.split(/\n/)) {
  const line = raw.trim()
  if (!line) continue
  let ev: any
  if (line.startsWith("{")) {
    try { ev = JSON.parse(line) } catch { skipped++; continue }
  } else {
    const m = line.match(/^(FIRST|AGAIN)\s+(.*)$/)
    if (!m) { skipped++; continue }
    if (m[1] === "AGAIN") { unknown++; continue }
    try { ev = JSON.parse(m[2]) } catch { skipped++; continue }
  }
  if (!ev || typeof ev.type !== "string") { skipped++; continue }
  const before = store.model.rev
  store.apply(ev)
  if (store.model.rev !== before) applied++
  seen.add(ev.type)
}

console.log(`events=${applied} skipped=${skipped} again=${unknown}`)
console.log("types handled:", [...seen].sort().join(" "))
const m = store.snapshot()
const ns = Object.values(m.nodes)
console.log(`\nmodel: rev=${m.rev} nodes=${ns.length} edges=${m.edges.length} ticks=${m.ticks.length} errors=${m.errors.length}`)
for (const n of ns.slice(0, 12)) {
  console.log(
    `  ${n.agent.padEnd(12)} ${n.status.padEnd(11)} steps=${String(n.steps).padStart(2)} tools=${String(n.toolsRun).padStart(2)} ` +
      `tok=${n.tokens.input + n.tokens.output} cost=${n.cost.toFixed(3)} ${n.title.slice(0, 44)}`,
  )
}
for (const e of m.edges) console.log(`  edge ${e.from.slice(-8)} → ${e.to.slice(-8)}`)

// exercise flow_task
const anyId = ns[0]?.id ?? "ses_test"
store.task(anyId, "start", "verify dashboard", "checking lanes")
store.task(anyId, "progress", "verify dashboard", "replaying events")
store.task(anyId, "done", "verify dashboard", "reducer ok")
console.log(`\nflow_task: ${JSON.stringify(ns[0]?.tasks ?? [])}`)