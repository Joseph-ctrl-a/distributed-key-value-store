import { useState } from 'react'
import styles from './KVConsole.module.css'

type Props = {
  onSet: (key: string, value: string) => void
  onDelete: (key: string) => void
  busy: boolean
}

export function KVConsole({ onSet, onDelete, busy }: Props) {
  const [key, setKey] = useState('')
  const [value, setValue] = useState('')

  const handleSet = () => { if (key) { onSet(key, value); setKey(''); setValue('') } }
  const handleDelete = () => { if (key) { onDelete(key); setKey('') } }

  return (
    <div className={styles.panel}>
      <div className={styles.sectionLabel}>{'>'} KV STORE</div>

      <div className={styles.row}>
        <span className={styles.prompt}>SET</span>
        <input
          placeholder="key"
          value={key}
          onChange={(e) => setKey(e.target.value)}
          onKeyDown={(e) => e.key === 'Enter' && handleSet()}
        />
        <input
          placeholder="value"
          value={value}
          onChange={(e) => setValue(e.target.value)}
          onKeyDown={(e) => e.key === 'Enter' && handleSet()}
        />
        <button className={`primary ${styles.submitBtn}`} onClick={handleSet} disabled={busy || !key}>▶</button>
      </div>

      <div className={styles.row}>
        <span className={styles.prompt}>DEL</span>
        <input
          placeholder="key"
          value={key}
          onChange={(e) => setKey(e.target.value)}
          onKeyDown={(e) => e.key === 'Enter' && handleDelete()}
          className={styles.wideInput}
        />
        <button className={`danger ${styles.submitBtn}`} onClick={handleDelete} disabled={busy || !key}>✕</button>
      </div>
    </div>
  )
}
