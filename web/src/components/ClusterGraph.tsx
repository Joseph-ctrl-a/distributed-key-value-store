import {
  Background,
  Controls,
  MarkerType,
  ReactFlow,
  type Edge,
  type Node as FlowNode,
} from '@xyflow/react'
import clsx from 'clsx'
import { useMemo } from 'react'

import { nodeByRaftAddress, type ClusterSnapshot, type NodeSnapshot } from '../api'

export function ClusterGraph({ cluster }: { cluster?: ClusterSnapshot }) {
  const flow = useMemo(() => buildFlow(cluster), [cluster])

  return (
    <div className="flow-shell">
      <ReactFlow nodes={flow.nodes} edges={flow.edges} fitView minZoom={0.75} maxZoom={1.2} proOptions={{ hideAttribution: true }}>
        <Background gap={18} size={1} />
        <Controls showInteractive={false} />
      </ReactFlow>
    </div>
  )
}

function buildFlow(cluster?: ClusterSnapshot): { nodes: FlowNode[]; edges: Edge[] } {
  const nodes = cluster?.nodes ?? []
  const positions = [
    { x: 250, y: 40 },
    { x: 70, y: 240 },
    { x: 430, y: 240 },
  ]
  const byAddress = cluster ? nodeByRaftAddress(cluster) : new Map<string, NodeSnapshot>()

  const flowNodes = nodes.map<FlowNode>((node, index) => ({
    id: node.name,
    position: positions[index] ?? { x: 180 * index, y: 120 },
    data: {
      label: (
        <div className="flow-node-label">
          <strong>{node.name}</strong>
          <span>{node.status?.role ?? (node.running ? 'starting' : 'stopped')}</span>
        </div>
      ),
    },
    className: clsx('flow-node', node.status?.role, !node.running && 'offline'),
  }))

  const flowEdges: Edge[] = []
  for (const source of nodes) {
    for (const peer of source.status?.peers ?? []) {
      const target = byAddress.get(peer)
      if (!target || source.name > target.name) {
        continue
      }
      const blocked = source.status?.blockedPeers.includes(peer) || target.status?.blockedPeers.includes(source.raftAddr)
      flowEdges.push({
        id: `${source.name}-${target.name}`,
        source: source.name,
        target: target.name,
        animated: !blocked && source.running && target.running,
        markerEnd: { type: MarkerType.ArrowClosed },
        className: clsx(blocked && 'edge-blocked'),
      })
    }
  }

  return { nodes: flowNodes, edges: flowEdges }
}
