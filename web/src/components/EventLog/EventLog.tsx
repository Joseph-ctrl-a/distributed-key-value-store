import { useEffect, useRef } from 'react'
import type { ClusterEvent } from '../../api'
import styles from './EventLog.module.css'

type Props = {
  events: ClusterEvent[]
  loading: boolean
}

const EVENT_COLORS: Record<string, string> = {
  'node.started':        'green',
  'node.stopped':        'red',
  'node.exited':         'red',
  'partition.applied':   'amber',
  'partition.cleared':   'green',
  'sim.build':           'blue',
}

export function EventLog({ events, loading }: Props) {
  const bottomRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [events.length])

  return (
    <div className={styles.panel}>
      <div className={styles.sectionLabel}>{'>'} TIMELINE</div>
      <div className={styles.list}>
        {loading && <div className={styles.empty}>waiting for events…</div>}
        {!loading && events.length === 0 && <div className={styles.empty}>no events yet</div>}
        {[...events].reverse().slice(-120).map((event) => {
          const colorKey = EVENT_COLORS[event.type]
          return (
            <div key={event.id} className={styles.row}>
              <span className={styles.time}>{formatTime(event.timestamp)}</span>
              <span className={`${styles.node} ${colorKey ? styles[colorKey] : ''}`}>
                {event.node || event.type}
              </span>
              <span className={styles.msg}>{event.message}</span>
            </div>
          )
        })}
        <div ref={bottomRef} />
      </div>
    </div>
  )
}

function formatTime(ts: string) {
  return new Date(ts).toLocaleTimeString('en-US', {
    hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false,
  })
}
