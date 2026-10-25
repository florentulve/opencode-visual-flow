// Shared RPC contract for the flow dashboard.
// The dashboard (browser) calls POST /api/rpc/flow/{method} and subscribes to
// `rpc.flow.changed` on the /api/event stream. The "./rpc" export also lets
// other clients import this contract via @opencode/client's client.rpc(Flow).
import { Rpc } from "@opencode/plugin/rpc"

const emptyObject = { type: "object", properties: {}, additionalProperties: false } as const

export const Flow = Rpc.define({
  id: "flow",
  methods: {
    snapshot: {
      input: emptyObject,
      output: { type: "object" } as const,
    },
    node: {
      input: {
        type: "object",
        properties: { id: { type: "string" } },
        required: ["id"],
        additionalProperties: false,
      } as const,
      output: { type: "object" } as const,
      errors: {
        not_found: {
          type: "object",
          properties: { id: { type: "string" } },
          required: ["id"],
          additionalProperties: false,
        } as const,
      },
    },
  },
  events: {
    changed: {
      schema: {
        type: "object",
        properties: { rev: { type: "integer" } },
        required: ["rev"],
        additionalProperties: false,
      } as const,
    },
    activity: {
      schema: {
        type: "object",
        properties: {
          sessionID: { type: "string" },
          kind: { type: "string", enum: ["tool", "text", "step", "status"] },
          data: { type: "object" },
        },
        required: ["sessionID", "kind", "data"],
        additionalProperties: false,
      } as const,
    },
  },
})