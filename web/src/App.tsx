import { useMemo, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'

import { api, type ClusterEvent, type ClusterSnapshot, type NodeSnapshot } from './api'
import { useClusterStream } from './useClusterStream'
import { Header } from './components/Header/Header'
import { NodeCard } from './components/NodeCard/NodeCard'
import { NodePanel } from './components/NodePanel/NodePanel'
import { Topology } from './components/Topology/Topology'
import { EventLog } from './components/EventLog/EventLog'
import { KVConsole } from './components/KVConsole/KVConsole'
import { FaultControls } from './components/FaultControls/FaultControls'
import { ReplicationBars } from './components/ReplicationBars/ReplicationBars'
import styles from './App.module.css'

const roleRank = new Map([['leader', 0], ['candidate', 1], ['follower', 2]])

export function App() {
  const queryClient = useQueryClient()
  const clusterQuery = useQuery({ queryKey: ['cluster'], queryFn: api.cluster })
  const eventsQuery = useQuery({ queryKey: ['events'], queryFn: api.events, staleTime: 2_000 })
  const stream = useClusterStream()
  const [selectedNode, setSelectedNode] = useState<NodeSnapshot | null>(null)

  const cluster = clusterQuery.data
  const events = useMemo(
    () => mergeEvents(stream.events, eventsQuery.data ?? []),
    [eventsQuery.data, stream.events],
  )

  const updateCluster = (snapshot: ClusterSnapshot) => queryClient.setQueryData(['cluster'], snapshot)

  const startCluster  = useMutation({ mutationFn: api.startCluster,  onSuccess: updateCluster })
  const stopCluster   = useMutation({ mutationFn: api.stopCluster,   onSuccess: updateCluster })
  const startNode     = useMutation({ mutationFn: api.startNode,     onSuccess: updateCluster })
  const stopNode      = useMutation({ mutationFn: api.stopNode,      onSuccess: updateCluster })
  const applyParts    = useMutation({ mutationFn: api.applyPartitions, onSuccess: updateCluster })
  const clearParts    = useMutation({ mutationFn: api.clearPartitions, onSuccess: updateCluster })
  const setKey        = useMutation({ mutationFn: ({ key, value }: { key: string; value: string }) => api.setKey(key, value) })
  const deleteKey     = useMutation({ mutationFn: (key: string) => api.deleteKey(key) })

  const mutationError = (
    startCluster.error ?? stopCluster.error ?? startNode.error ?? stopNode.error ??
    applyParts.error ?? clearParts.error ?? setKey.error ?? deleteKey.error
  )

  const sortedNodes = (cluster?.nodes ?? []).slice().sort((a, b) => {
    const aRank = roleRank.get(a.status?.role ?? '') ?? 3
    const bRank = roleRank.get(b.status?.role ?? '') ?? 3
    return aRank - bRank || a.name.localeCompare(b.name)
  })

  // Keep the panel's node snapshot fresh as cluster data updates
  const liveSelectedNode = selectedNode
    ? (cluster?.nodes.find((n) => n.name === selectedNode.name) ?? selectedNode)
    : null

  return (
    <>
    {liveSelectedNode && (
      <NodePanel node={liveSelectedNode} onClose={() => setSelectedNode(null)} />
    )}
    <div className={styles.shell}>
      <Header
        cluster={cluster}
        streamStatus={stream.status}
        onStart={() => startCluster.mutate()}
        onStop={() => stopCluster.mutate()}
        busy={startCluster.isPending || stopCluster.isPending}
      />

      {mutationError && (
        <div className={styles.errorBanner}>
          ✕ {mutationError.message}
        </div>
      )}

      <div className={styles.body}>
        <div className={styles.left}>
          <div className={styles.nodeGrid}>
            {sortedNodes.map((node) => (
              <NodeCard
                key={node.name}
                node={node}
                onStart={() => startNode.mutate(node.name)}
                onStop={() => stopNode.mutate(node.name)}
                busy={startNode.isPending || stopNode.isPending}
                onSelect={() => setSelectedNode(node)}
                selected={selectedNode?.name === node.name}
              />
            ))}
          </div>

          <Topology cluster={cluster} />

          <ReplicationBars cluster={cluster} />
        </div>

        <div className={styles.right}>
          <EventLog events={events} loading={eventsQuery.isLoading && stream.events.length === 0} />
          <KVConsole
            onSet={(key, value) => setKey.mutate({ key, value })}
            onDelete={(key) => deleteKey.mutate(key)}
            busy={setKey.isPending || deleteKey.isPending}
          />
          <FaultControls
            cluster={cluster}
            onApply={(groups) => applyParts.mutate(groups)}
            onClear={() => clearParts.mutate()}
            busy={applyParts.isPending || clearParts.isPending}
          />
        </div>
      </div>
    </div>
    </>
  )
}

function mergeEvents(...groups: ClusterEvent[][]) {
  const merged = new Map<number, ClusterEvent>()
  for (const event of groups.flat()) merged.set(event.id, event)
  return [...merged.values()].sort((a, b) => b.id - a.id)
}
