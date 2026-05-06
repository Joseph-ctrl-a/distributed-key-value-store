import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'

import type { ClusterSnapshot } from '../api'

export function ReplicationChart({ cluster }: { cluster?: ClusterSnapshot }) {
  const data =
    cluster?.nodes.map((node) => ({
      name: node.name,
      commit: node.status?.commitIndex ?? 0,
      applied: node.status?.lastApplied ?? 0,
      log: node.status?.lastLogIndex ?? 0,
    })) ?? []

  return (
    <div className="chart-shell">
      <ResponsiveContainer>
        <BarChart data={data} margin={{ left: -24, right: 8, top: 8, bottom: 0 }}>
          <CartesianGrid strokeDasharray="4 4" vertical={false} />
          <XAxis dataKey="name" tickLine={false} axisLine={false} />
          <YAxis allowDecimals={false} tickLine={false} axisLine={false} />
          <Tooltip cursor={{ fill: 'var(--chart-hover)' }} contentStyle={{ background: 'var(--panel)', border: '1px solid var(--border)' }} />
          <Bar dataKey="log" fill="var(--ink-weak)" radius={[4, 4, 0, 0]} />
          <Bar dataKey="commit" fill="var(--accent)" radius={[4, 4, 0, 0]} />
          <Bar dataKey="applied" fill="var(--ok)" radius={[4, 4, 0, 0]} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}
