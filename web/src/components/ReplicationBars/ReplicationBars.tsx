import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts'
import type { ClusterSnapshot } from '../../api'
import styles from './ReplicationBars.module.css'

type Props = { cluster?: ClusterSnapshot }

export function ReplicationBars({ cluster }: Props) {
  const data = (cluster?.nodes ?? []).map((node) => ({
    name: node.name,
    log:     node.status?.lastLogIndex  ?? 0,
    commit:  node.status?.commitIndex   ?? 0,
    applied: node.status?.lastApplied   ?? 0,
  }))

  return (
    <div className={styles.wrapper}>
      <div className={styles.sectionLabel}>{'>'} REPLICATION</div>
      <ResponsiveContainer width="100%" height={120}>
        <BarChart data={data} margin={{ top: 4, right: 16, left: -20, bottom: 0 }} barCategoryGap="30%">
          <CartesianGrid vertical={false} stroke="var(--border)" />
          <XAxis
            dataKey="name"
            tick={{ fill: 'var(--text-dim)', fontSize: 10, fontFamily: 'var(--font)' }}
            axisLine={{ stroke: 'var(--border)' }}
            tickLine={false}
          />
          <YAxis
            tick={{ fill: 'var(--text-dim)', fontSize: 9, fontFamily: 'var(--font)' }}
            axisLine={false}
            tickLine={false}
            allowDecimals={false}
          />
          <Tooltip
            contentStyle={{
              background: 'var(--surface-2)',
              border: '1px solid var(--border-hi)',
              borderRadius: 2,
              fontFamily: 'var(--font)',
              fontSize: 11,
              color: 'var(--text)',
            }}
            cursor={{ fill: 'rgba(255,255,255,0.03)' }}
          />
          <Legend
            wrapperStyle={{ fontFamily: 'var(--font)', fontSize: 10, color: 'var(--text-dim)', paddingTop: 4 }}
          />
          <Bar dataKey="log"     fill="#2a2a2a"         radius={[2, 2, 0, 0]} name="log" />
          <Bar dataKey="commit"  fill="rgba(0,230,118,0.45)" radius={[2, 2, 0, 0]} name="commit" />
          <Bar dataKey="applied" fill="var(--green)"    radius={[2, 2, 0, 0]} name="applied" />
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}
