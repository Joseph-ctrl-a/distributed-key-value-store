import type { ClusterSnapshot } from '../../api'
import styles from './Header.module.css'

type Props = {
  cluster?: ClusterSnapshot
  streamStatus: 'connecting' | 'connected' | 'offline'
  onStart: () => void
  onStop: () => void
  busy: boolean
}

export function Header({ cluster, streamStatus, onStart, onStop, busy }: Props) {
  const leader = cluster?.nodes.find((n) => n.status?.role === 'leader')?.name ?? 'none'
  const term = Math.max(0, ...(cluster?.nodes.map((n) => n.status?.currentTerm ?? 0) ?? [0]))
  const running = cluster?.nodes.filter((n) => n.running).length ?? 0
  const total = cluster?.nodes.length ?? 3
  const partitions = cluster?.partitions ?? []
  const partitionLabel = partitions.length
    ? partitions.map((g) => g.join(',')).join(' | ')
    : 'clear'

  return (
    <header className={styles.header}>
      <div className={styles.brand}>
        <span className={styles.title}>RAFT SIMULATOR</span>
        <span className={styles.sub}>distributed key-value store</span>
      </div>

      <div className={styles.metrics}>
        <Metric label="LEADER" value={leader} highlight={leader !== 'none'} />
        <Metric label="TERM" value={String(term)} />
        <Metric label="NODES" value={`${running}/${total}`} highlight={running === total} />
        <Metric label="PARTITION" value={partitionLabel} warn={partitions.length > 0} />
      </div>

      <div className={styles.actions}>
        <span className={styles[streamStatus]}>{streamStatus}</span>
        <button className="primary" onClick={onStart} disabled={busy}>▶ START</button>
        <button onClick={onStop} disabled={busy}>■ STOP</button>
      </div>
    </header>
  )
}

function Metric({ label, value, highlight, warn }: { label: string; value: string; highlight?: boolean; warn?: boolean }) {
  return (
    <div className={styles.metric}>
      <span className={styles.metricLabel}>{label}</span>
      <span className={warn ? styles.metricWarn : highlight ? styles.metricHi : styles.metricVal}>
        {value}
      </span>
    </div>
  )
}
