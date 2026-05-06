import { useEffect } from 'react'
import { useQuery } from '@tanstack/react-query'
import { api, type NodeSnapshot } from '../../api'
import styles from './NodePanel.module.css'

type Props = {
  node: NodeSnapshot
  onClose: () => void
}

export function NodePanel({ node, onClose }: Props) {
  const kvQuery  = useQuery({
    queryKey: ['node-kv',  node.name],
    queryFn:  () => api.nodeKV(node.name),
    enabled:  node.running,
    refetchInterval: 1_500,
  })

  const logQuery = useQuery({
    queryKey: ['node-log', node.name],
    queryFn:  () => api.nodeLog(node.name),
    enabled:  node.running,
    refetchInterval: 1_500,
  })

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') onClose() }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [onClose])

  const status = node.status
  const role   = status?.role ?? (node.running ? 'starting' : 'stopped')
  const kvEntries = Object.entries(kvQuery.data ?? {}).sort(([a], [b]) => a.localeCompare(b))
  const logEntries = [...(logQuery.data ?? [])].reverse()

  return (
    <>
      <div className={styles.backdrop} onClick={onClose} />
      <aside className={styles.panel}>
        <div className={styles.panelHeader}>
          <div className={styles.nodeInfo}>
            <span className={`${styles.dot} ${styles[`dot_${role}`] ?? ''}`}>●</span>
            <span className={styles.nodeName}>{node.name}</span>
            <span className={`${styles.roleBadge} ${styles[role] ?? ''}`}>{role.toUpperCase()}</span>
          </div>
          <button className={styles.closeBtn} onClick={onClose} title="Close (Esc)">✕</button>
        </div>

        {status && (
          <div className={styles.statBar}>
            <StatPill label="TERM"    value={status.currentTerm} />
            <StatPill label="COMMIT"  value={status.commitIndex} />
            <StatPill label="APPLIED" value={status.lastApplied} />
            <StatPill label="LOG"     value={status.lastLogIndex} />
          </div>
        )}

        <div className={styles.sections}>
          <section className={styles.section}>
            <div className={styles.sectionLabel}>{'>'} KV STATE
              <span className={styles.count}>{kvEntries.length} keys</span>
            </div>
            <div className={styles.kvList}>
              {!node.running && <div className={styles.empty}>node offline</div>}
              {node.running && kvQuery.isLoading && <div className={styles.empty}>loading…</div>}
              {node.running && kvEntries.length === 0 && !kvQuery.isLoading && (
                <div className={styles.empty}>empty store</div>
              )}
              {kvEntries.map(([key, value]) => (
                <div key={key} className={styles.kvRow}>
                  <span className={styles.kvKey}>{key}</span>
                  <span className={styles.kvArrow}>→</span>
                  <span className={styles.kvValue}>{value}</span>
                </div>
              ))}
            </div>
          </section>

          <section className={styles.section}>
            <div className={styles.sectionLabel}>{'>'} WAL LOG
              <span className={styles.count}>{logQuery.data?.length ?? 0} entries</span>
            </div>
            <div className={styles.logList}>
              {!node.running && <div className={styles.empty}>node offline</div>}
              {node.running && logQuery.isLoading && <div className={styles.empty}>loading…</div>}
              {logEntries.map((entry) => (
                <div key={entry.index} className={styles.logRow}>
                  <span className={styles.logIndex}>#{entry.index}</span>
                  <span className={`${styles.logMethod} ${entry.method === 'SET' ? styles.methodSet : styles.methodDel}`}>
                    {entry.method}
                  </span>
                  <span className={styles.logParams}>{entry.params.join(' → ')}</span>
                  <span className={styles.logTerm}>t{entry.term}</span>
                </div>
              ))}
            </div>
          </section>
        </div>
      </aside>
    </>
  )
}

function StatPill({ label, value }: { label: string; value: number }) {
  return (
    <div className={styles.statPill}>
      <span className={styles.statLabel}>{label}</span>
      <span className={styles.statValue}>{value}</span>
    </div>
  )
}
