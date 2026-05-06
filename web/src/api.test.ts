import { describe, expect, it } from 'vitest'

import { clusterSnapshotSchema, eventSchema } from './api'

describe('api schemas', () => {
  it('normalizes missing partitions', () => {
    const parsed = clusterSnapshotSchema.parse({ nodes: [] })
    expect(parsed.partitions).toEqual([])
  })

  it('accepts simulator event envelopes', () => {
    const parsed = eventSchema.parse({
      id: 1,
      type: 'node.started',
      timestamp: new Date().toISOString(),
      node: 'node1',
      message: 'node process started',
    })
    expect(parsed.node).toBe('node1')
  })
})
