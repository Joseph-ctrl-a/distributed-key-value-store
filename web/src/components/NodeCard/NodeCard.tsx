import type { NodeSnapshot } from '../../api'
import styles from './NodeCard.module.css'

type Props = {
  node: NodeSnapshot
  onStart: () => void
  onStop: () => void
  busy: boolean
  onSelect: () => void
  selected: boolean
}

export function NodeCard({ node, onStart, onStop, busy, onSelect, selected }: Props) {
  const status = node.status
  const role = status?.role ?? (node.running ? 'starting' : 'stopped')

  return (
    <article
      className={`${styles.card} ${styles[role] ?? ''} ${selected ? styles.selected : ''}`}
      onClick={onSelect}
      style={{ cursor: 'pointer' }}
    >
      <div className={styles.top}>
        <div className={styles.roleRow}>
          <span className={`${styles.dot} ${styles[`dot_${role}`] ?? ''}`}>●</span>
          <span className={styles.role}>{role.toUpperCase()}</span>
        </div>
        <button
          className={styles.toggleBtn}
          onClick={(e) => { e.stopPropagation(); node.running ? onStop() : onStart() }}
          disabled={busy}
          title={node.running ? 'Stop node' : 'Start node'}
        >
          {node.running ? '■' : '▶'}
        </button>
      </div>

      <div className={styles.name}>{node.name}</div>
      <div className={styles.addr}>{node.raftAddr}</div>

      <div className={styles.divider} />

      <div className={styles.stats}>
        <Stat label="TERM"    value={status?.currentTerm ?? '–'} />
        <Stat label="COMMIT"  value={status?.commitIndex ?? '–'} />
        <Stat label="APPLIED" value={status?.lastApplied ?? '–'} />
        <Stat label="LOG"     value={status?.lastLogIndex ?? '–'} />
      </div>

      {status && status.peers.length > 0 ? (
        <div className={styles.peers}>
          {status.peers.map((peer) => (
            <div key={peer} className={styles.peerRow}>
              <span className={styles.peerAddr}>{peer}</span>
              <span className={styles.peerIdx}>
                m:{status.matchIndex[peer] ?? 0} n:{status.nextIndex[peer] ?? 0}
              </span>
            </div>
          ))}
        </div>
      ) : (
        <div className={styles.error}>{node.error || (node.running ? 'fetching status…' : 'offline')}</div>
      )}
    </article>
  )
}

function Stat({ label, value }: { label: string; value: string | number }) {
  return (
    <div className={styles.stat}>
      <span className={styles.statLabel}>{label}</span>
      <span className={styles.statVal}>{value}</span>
    </div>
  )
}
