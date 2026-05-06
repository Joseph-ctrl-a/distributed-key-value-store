import { z } from 'zod'

const numberMapSchema = z.record(z.string(), z.number()).default({})

export const nodeStatusSchema = z.object({
  id: z.string(),
  role: z.string(),
  currentTerm: z.number(),
  leader: z.string().default(''),
  peers: z.array(z.string()).default([]),
  commitIndex: z.number(),
  lastApplied: z.number(),
  lastLogIndex: z.number(),
  nextIndex: numberMapSchema,
  matchIndex: numberMapSchema,
  blockedPeers: z.array(z.string()).nullish().transform((v) => v ?? []),
})

export const nodeSnapshotSchema = z.object({
  name: z.string(),
  raftAddr: z.string(),
  controlAddr: z.string(),
  running: z.boolean(),
  pid: z.number().optional(),
  status: nodeStatusSchema.optional(),
  error: z.string().optional(),
})

export const clusterSnapshotSchema = z.object({
  nodes: z.array(nodeSnapshotSchema),
  partitions: z.array(z.array(z.string())).nullable().optional().transform((groups) => groups ?? []),
})

export const eventSchema = z.object({
  id: z.number(),
  type: z.string(),
  timestamp: z.string(),
  node: z.string().optional(),
  message: z.string(),
  data: z.unknown().optional(),
})

export const envelopeSchema = z.object({
  type: z.string(),
  data: z.unknown(),
})

const writeResultSchema = z.object({
  index: z.number().optional(),
  term: z.number().optional(),
})

export const logEntrySchema = z.object({
  index:  z.number(),
  term:   z.number(),
  method: z.string(),
  params: z.array(z.string()),
})

export type LogEntry = z.infer<typeof logEntrySchema>

const errorSchema = z.object({
  error: z.string(),
  leader: z.string().optional(),
})

export type NodeStatus = z.infer<typeof nodeStatusSchema>
export type NodeSnapshot = z.infer<typeof nodeSnapshotSchema>
export type ClusterSnapshot = z.infer<typeof clusterSnapshotSchema>
export type ClusterEvent = z.infer<typeof eventSchema>
export type Envelope = z.infer<typeof envelopeSchema>
export type WriteResult = z.infer<typeof writeResultSchema>

type RequestOptions = {
  method?: 'GET' | 'POST' | 'DELETE'
  body?: unknown
}

const apiBase = () => (import.meta.env.VITE_API_BASE ?? '').replace(/\/$/, '')

async function request<T>(path: string, schema: z.ZodType<T>, options: RequestOptions = {}): Promise<T> {
  const response = await fetch(`${apiBase()}${path}`, {
    method: options.method ?? 'GET',
    headers: options.body ? { 'Content-Type': 'application/json' } : undefined,
    body: options.body ? JSON.stringify(options.body) : undefined,
  })

  const payload: unknown = await response.json()
  if (!response.ok) {
    const parsed = errorSchema.safeParse(payload)
    throw new Error(parsed.success ? parsed.data.error : `Request failed with ${response.status}`)
  }
  return schema.parse(payload)
}

export const api = {
  cluster: () => request('/api/cluster', clusterSnapshotSchema),
  events: () => request('/api/events', z.array(eventSchema)),
  startCluster: () => request('/api/cluster/start', clusterSnapshotSchema, { method: 'POST' }),
  stopCluster: () => request('/api/cluster/stop', clusterSnapshotSchema, { method: 'POST' }),
  startNode: (id: string) => request(`/api/nodes/${id}/start`, clusterSnapshotSchema, { method: 'POST' }),
  stopNode: (id: string) => request(`/api/nodes/${id}/stop`, clusterSnapshotSchema, { method: 'POST' }),
  applyPartitions: (groups: string[][]) =>
    request('/api/partitions', clusterSnapshotSchema, { method: 'POST', body: { groups } }),
  clearPartitions: () => request('/api/partitions', clusterSnapshotSchema, { method: 'DELETE' }),
  setKey: (key: string, value: string) =>
    request('/api/kv/set', writeResultSchema, { method: 'POST', body: { key, value } }),
  deleteKey: (key: string) => request('/api/kv/delete', writeResultSchema, { method: 'POST', body: { key } }),
  nodeKV:  (id: string) => request(`/api/nodes/${id}/kv`,  z.record(z.string(), z.string())),
  nodeLog: (id: string) => request(`/api/nodes/${id}/log`, z.array(logEntrySchema)),
}

export function websocketUrl() {
  const explicit = import.meta.env.VITE_WS_URL
  if (explicit) {
    return explicit
  }

  const base = apiBase()
  if (base.startsWith('http')) {
    return `${base.replace(/^http/, 'ws')}/api/ws`
  }

  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
  return `${protocol}//${window.location.host}/api/ws`
}

export function nodeByRaftAddress(cluster: ClusterSnapshot) {
  return new Map(cluster.nodes.map((node) => [node.raftAddr, node]))
}
