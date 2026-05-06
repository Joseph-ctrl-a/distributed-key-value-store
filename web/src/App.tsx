import {
  Activity,
  Ban,
  CircleOff,
  Database,
  Moon,
  Network,
  Play,
  Radio,
  RotateCcw,
  Scissors,
  Server,
  ShieldCheck,
  Square,
  Sun,
  Trash2,
} from 'lucide-react'
import { lazy, Suspense, useMemo, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import clsx from 'clsx'
import type { ReactNode } from 'react'

import { api, type ClusterEvent, type ClusterSnapshot, type NodeSnapshot } from './api'
import { useTheme } from './theme'
import { useClusterStream } from './useClusterStream'

const ClusterGraph = lazy(() => import('./components/ClusterGraph').then((module) => ({ default: module.ClusterGraph })))
const ReplicationChart = lazy(() =>
  import('./components/ReplicationChart').then((module) => ({ default: module.ReplicationChart })),
)

const roleRank = new Map([
  ['leader', 0],
  ['candidate', 1],
  ['follower', 2],
])

export function App() {
  const queryClient = useQueryClient()
  const clusterQuery = useQuery({ queryKey: ['cluster'], queryFn: api.cluster })
  const eventsQuery = useQuery({ queryKey: ['events'], queryFn: api.events, staleTime: 2_000 })
  const stream = useClusterStream()

  const cluster = clusterQuery.data
  const events = useMemo(
    () => mergeEvents(stream.events, eventsQuery.data ?? []),
    [eventsQuery.data, stream.events],
  )

  const updateCluster = (snapshot: ClusterSnapshot) => queryClient.setQueryData(['cluster'], snapshot)

  const startCluster = useMutation({ mutationFn: api.startCluster, onSuccess: updateCluster })
  const stopCluster = useMutation({ mutationFn: api.stopCluster, onSuccess: updateCluster })
  const startNode = useMutation({ mutationFn: api.startNode, onSuccess: updateCluster })
  const stopNode = useMutation({ mutationFn: api.stopNode, onSuccess: updateCluster })
  const applyPartitions = useMutation({ mutationFn: api.applyPartitions, onSuccess: updateCluster })
  const clearPartitions = useMutation({ mutationFn: api.clearPartitions, onSuccess: updateCluster })
  const setKey = useMutation({ mutationFn: ({ key, value }: { key: string; value: string }) => api.setKey(key, value) })
  const deleteKey = useMutation({ mutationFn: (key: string) => api.deleteKey(key) })

  const mutationError =
    startCluster.error ??
    stopCluster.error ??
    startNode.error ??
    stopNode.error ??
    applyPartitions.error ??
    clearPartitions.error ??
    setKey.error ??
    deleteKey.error

  return (
    <main className="app-shell">
      <header className="topbar">
        <div>
          <p className="eyebrow">Distributed KV Store</p>
          <h1>Raft Simulator</h1>
        </div>
        <div className="topbar-actions">
          <SocketBadge status={stream.status} />
          <ThemeToggle />
          <button className="button primary" onClick={() => startCluster.mutate()} disabled={startCluster.isPending}>
            <Play size={16} />
            Start
          </button>
          <button className="button" onClick={() => stopCluster.mutate()} disabled={stopCluster.isPending}>
            <Square size={16} />
            Stop
          </button>
        </div>
      </header>

      {mutationError ? <p className="error-banner">{mutationError.message}</p> : null}

      <section className="summary-strip">
        <SummaryMetric label="Leader" value={leaderName(cluster)} icon={<ShieldCheck size={18} />} />
        <SummaryMetric label="Running" value={`${runningCount(cluster)} / ${cluster?.nodes.length ?? 3}`} icon={<Server size={18} />} />
        <SummaryMetric label="Term" value={String(maxTerm(cluster))} icon={<Activity size={18} />} />
        <SummaryMetric label="Partition" value={partitionLabel(cluster)} icon={<Network size={18} />} />
      </section>

      <section className="workspace-grid">
        <div className="graph-panel">
          <SectionHeader title="Topology" icon={<Radio size={18} />} />
          <Suspense fallback={<div className="flow-shell skeleton" />}>
            <ClusterGraph cluster={cluster} />
          </Suspense>
        </div>

        <div className="side-panel">
          <PartitionControls
            cluster={cluster}
            onApply={(groups) => applyPartitions.mutate(groups)}
            onClear={() => clearPartitions.mutate()}
            busy={applyPartitions.isPending || clearPartitions.isPending}
          />
          <KVConsole
            onSet={(key, value) => setKey.mutate({ key, value })}
            onDelete={(key) => deleteKey.mutate(key)}
            busy={setKey.isPending || deleteKey.isPending}
          />
        </div>
      </section>

      <section className="node-grid">
        {(cluster?.nodes ?? []).sort(compareNodes).map((node) => (
          <NodeCard
            key={node.name}
            node={node}
            onStart={() => startNode.mutate(node.name)}
            onStop={() => stopNode.mutate(node.name)}
            busy={startNode.isPending || stopNode.isPending}
          />
        ))}
        {!cluster && [1, 2, 3].map((item) => <div className="node-card skeleton" key={item} />)}
      </section>

      <section className="lower-grid">
        <div className="chart-panel">
          <SectionHeader title="Replication" icon={<Database size={18} />} />
          <Suspense fallback={<div className="chart-shell skeleton" />}>
            <ReplicationChart cluster={cluster} />
          </Suspense>
        </div>
        <EventTimeline events={events} loading={eventsQuery.isLoading && stream.events.length === 0} />
      </section>
    </main>
  )
}

function ThemeToggle() {
  const { mode, setMode } = useTheme()
  return (
    <div className="segmented" aria-label="Theme">
      {[
        { value: 'system', icon: <CircleOff size={15} />, label: 'System' },
        { value: 'light', icon: <Sun size={15} />, label: 'Light' },
        { value: 'dark', icon: <Moon size={15} />, label: 'Dark' },
      ].map((item) => (
        <button
          key={item.value}
          className={clsx('segment', mode === item.value && 'active')}
          onClick={() => setMode(item.value as 'system' | 'light' | 'dark')}
          title={item.label}
          aria-label={item.label}
        >
          {item.icon}
        </button>
      ))}
    </div>
  )
}

function SocketBadge({ status }: { status: 'connecting' | 'connected' | 'offline' }) {
  return <span className={clsx('socket-badge', status)}>{status}</span>
}

function SummaryMetric({ label, value, icon }: { label: string; value: string; icon: ReactNode }) {
  return (
    <div className="metric">
      {icon}
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  )
}

function SectionHeader({ title, icon }: { title: string; icon: ReactNode }) {
  return (
    <div className="section-header">
      <span>{icon}</span>
      <h2>{title}</h2>
    </div>
  )
}

function PartitionControls({
  cluster,
  onApply,
  onClear,
  busy,
}: {
  cluster?: ClusterSnapshot
  onApply: (groups: string[][]) => void
  onClear: () => void
  busy: boolean
}) {
  const names = cluster?.nodes.map((node) => node.name) ?? ['node1', 'node2', 'node3']
  const [selected, setSelected] = useState(names[0] ?? 'node1')

  return (
    <section className="tool-panel">
      <SectionHeader title="Faults" icon={<Scissors size={18} />} />
      <div className="control-row">
        <select value={selected} onChange={(event) => setSelected(event.target.value)}>
          {names.map((name) => (
            <option key={name}>{name}</option>
          ))}
        </select>
        <button className="button" disabled={busy} onClick={() => onApply([[selected], names.filter((name) => name !== selected)])}>
          <Ban size={16} />
          Isolate
        </button>
      </div>
      <div className="button-grid">
        <button className="button" disabled={busy} onClick={() => onApply([[names[0]], names.slice(1)])}>
          1 | 2,3
        </button>
        <button className="button" disabled={busy} onClick={() => onApply([names.slice(0, 2), names.slice(2)])}>
          1,2 | 3
        </button>
        <button className="button" disabled={busy} onClick={onClear}>
          <RotateCcw size={16} />
          Clear
        </button>
      </div>
      <p className="partition-state">{partitionLabel(cluster)}</p>
    </section>
  )
}

function KVConsole({
  onSet,
  onDelete,
  busy,
}: {
  onSet: (key: string, value: string) => void
  onDelete: (key: string) => void
  busy: boolean
}) {
  const [keyName, setKeyName] = useState('')
  const [value, setValue] = useState('')

  return (
    <section className="tool-panel">
      <SectionHeader title="KV" icon={<Database size={18} />} />
      <div className="kv-grid">
        <input placeholder="key" value={keyName} onChange={(event) => setKeyName(event.target.value)} />
        <input placeholder="value" value={value} onChange={(event) => setValue(event.target.value)} />
      </div>
      <div className="button-grid">
        <button className="button primary" disabled={busy || keyName === ''} onClick={() => onSet(keyName, value)}>
          <Play size={16} />
          Set
        </button>
        <button className="button" disabled={busy || keyName === ''} onClick={() => onDelete(keyName)}>
          <Trash2 size={16} />
          Delete
        </button>
      </div>
    </section>
  )
}

function NodeCard({
  node,
  onStart,
  onStop,
  busy,
}: {
  node: NodeSnapshot
  onStart: () => void
  onStop: () => void
  busy: boolean
}) {
  const status = node.status
  const role = status?.role ?? (node.running ? 'starting' : 'stopped')

  return (
    <article className={clsx('node-card', role, !node.running && 'offline')}>
      <header>
        <div>
          <h3>{node.name}</h3>
          <p>{node.raftAddr}</p>
        </div>
        <span className={clsx('role-pill', role)}>{role}</span>
      </header>

      <div className="stat-grid">
        <Stat label="Term" value={status?.currentTerm ?? '-'} />
        <Stat label="Commit" value={status?.commitIndex ?? '-'} />
        <Stat label="Applied" value={status?.lastApplied ?? '-'} />
        <Stat label="Log" value={status?.lastLogIndex ?? '-'} />
      </div>

      {status ? (
        <div className="replication-map">
          {status.peers.map((peer) => (
            <div key={peer}>
              <span>{peer}</span>
              <strong>
                m{status.matchIndex[peer] ?? 0} / n{status.nextIndex[peer] ?? 0}
              </strong>
            </div>
          ))}
        </div>
      ) : (
        <p className="node-error">{node.error ?? 'process offline'}</p>
      )}

      <footer>
        <span>control {node.controlAddr}</span>
        {node.running ? (
          <button className="icon-button" onClick={onStop} disabled={busy} title="Stop node" aria-label={`Stop ${node.name}`}>
            <Square size={16} />
          </button>
        ) : (
          <button className="icon-button" onClick={onStart} disabled={busy} title="Start node" aria-label={`Start ${node.name}`}>
            <Play size={16} />
          </button>
        )}
      </footer>
    </article>
  )
}

function Stat({ label, value }: { label: string; value: string | number }) {
  return (
    <div className="stat">
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  )
}

function EventTimeline({ events, loading }: { events: ClusterEvent[]; loading: boolean }) {
  return (
    <section className="event-panel">
      <SectionHeader title="Timeline" icon={<Activity size={18} />} />
      <div className="event-list">
        {loading ? <p className="empty-state">Loading events</p> : null}
        {!loading && events.length === 0 ? <p className="empty-state">No events yet</p> : null}
        {events.slice(0, 80).map((event) => (
          <article className="event-row" key={event.id}>
            <time>{new Date(event.timestamp).toLocaleTimeString()}</time>
            <strong>{event.node || event.type}</strong>
            <span>{event.message}</span>
          </article>
        ))}
      </div>
    </section>
  )
}

function compareNodes(a: NodeSnapshot, b: NodeSnapshot) {
  const aRank = roleRank.get(a.status?.role ?? '') ?? 3
  const bRank = roleRank.get(b.status?.role ?? '') ?? 3
  return aRank - bRank || a.name.localeCompare(b.name)
}

function leaderName(cluster?: ClusterSnapshot) {
  return cluster?.nodes.find((node) => node.status?.role === 'leader')?.name ?? 'none'
}

function runningCount(cluster?: ClusterSnapshot) {
  return cluster?.nodes.filter((node) => node.running).length ?? 0
}

function maxTerm(cluster?: ClusterSnapshot) {
  return Math.max(0, ...(cluster?.nodes.map((node) => node.status?.currentTerm ?? 0) ?? [0]))
}

function partitionLabel(cluster?: ClusterSnapshot) {
  const groups = cluster?.partitions ?? []
  if (groups.length === 0) {
    return 'clear'
  }
  return groups.map((group) => group.join(',')).join(' | ')
}

function mergeEvents(...groups: ClusterEvent[][]) {
  const merged = new Map<number, ClusterEvent>()
  for (const event of groups.flat()) {
    merged.set(event.id, event)
  }
  return [...merged.values()].sort((a, b) => b.id - a.id)
}
