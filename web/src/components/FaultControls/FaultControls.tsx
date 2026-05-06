import { useState } from 'react'
import type { ClusterSnapshot } from '../../api'
import styles from './FaultControls.module.css'

type Props = {
  cluster?: ClusterSnapshot
  onApply: (groups: string[][]) => void
  onClear: () => void
  busy: boolean
}

export function FaultControls({ cluster, onApply, onClear, busy }: Props) {
  const names = cluster?.nodes.map((n) => n.name) ?? ['node1', 'node2', 'node3']
  const [selected, setSelected] = useState(names[0] ?? 'node1')
  const partitions = cluster?.partitions ?? []
  const partitionLabel = partitions.length
    ? partitions.map((g) => g.join(',')).join(' | ')
    : 'none'

  return (
    <div className={styles.panel}>
      <div className={styles.sectionLabel}>{'>'} FAULTS</div>

      <div className={styles.presets}>
        <button
          disabled={busy}
          onClick={() => onApply([[names[0]], names.slice(1)])}
        >
          1 | 2,3
        </button>
        <button
          disabled={busy}
          onClick={() => onApply([names.slice(0, 2), [names[2]]])}
        >
          1,2 | 3
        </button>
        <button disabled={busy} onClick={onClear}>CLEAR</button>
      </div>

      <div className={styles.isolateRow}>
        <select value={selected} onChange={(e) => setSelected(e.target.value)}>
          {names.map((n) => <option key={n}>{n}</option>)}
        </select>
        <button
          disabled={busy}
          onClick={() => onApply([[selected], names.filter((n) => n !== selected)])}
        >
          ISOLATE
        </button>
      </div>

      {partitions.length > 0 && (
        <div className={styles.state}>
          <span className={styles.stateLabel}>ACTIVE: </span>
          <span className={styles.stateVal}>{partitionLabel}</span>
        </div>
      )}
    </div>
  )
}
