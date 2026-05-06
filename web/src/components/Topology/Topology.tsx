import type { ClusterSnapshot } from '../../api'
import styles from './Topology.module.css'

type Props = { cluster?: ClusterSnapshot }

const W = 320
const H = 180
const R = 18

// Triangle positions: top-center, bottom-left, bottom-right
const POSITIONS: [number, number][] = [
  [W / 2,       36],
  [W * 0.18,    H - 36],
  [W * 0.82,    H - 36],
]

export function Topology({ cluster }: Props) {
  const nodes = cluster?.nodes ?? []
  const partitions = cluster?.partitions ?? []

  const nodeByName = new Map(nodes.map((n, i) => [n.name, { ...n, pos: POSITIONS[i] ?? POSITIONS[0] }]))

  const pairs: [string, string][] = []
  const names = nodes.map((n) => n.name)
  for (let i = 0; i < names.length; i++) {
    for (let j = i + 1; j < names.length; j++) {
      pairs.push([names[i], names[j]])
    }
  }

  const isBlocked = (a: string, b: string) => {
    if (partitions.length === 0) return false
    const groupOf = (name: string) => partitions.findIndex((g) => g.includes(name))
    return groupOf(a) !== groupOf(b)
  }

  return (
    <div className={styles.wrapper}>
      <div className={styles.sectionLabel}>{'>'} TOPOLOGY</div>
      <svg viewBox={`0 0 ${W} ${H}`} className={styles.svg} aria-label="Cluster topology">
        {pairs.map(([a, b]) => {
          const nodeA = nodeByName.get(a)
          const nodeB = nodeByName.get(b)
          if (!nodeA || !nodeB) return null
          const blocked = isBlocked(a, b)
          const bothUp = nodeA.running && nodeB.running
          return (
            <line
              key={`${a}-${b}`}
              x1={nodeA.pos[0]} y1={nodeA.pos[1]}
              x2={nodeB.pos[0]} y2={nodeB.pos[1]}
              className={blocked ? styles.edgeBlocked : bothUp ? styles.edgeActive : styles.edgeDown}
            />
          )
        })}

        {nodes.map((node, i) => {
          const pos = POSITIONS[i] ?? POSITIONS[0]
          const role = node.status?.role ?? (node.running ? 'starting' : 'stopped')
          return (
            <g key={node.name}>
              <circle
                cx={pos[0]} cy={pos[1]} r={R}
                className={`${styles.nodeCircle} ${styles[`circle_${role}`] ?? ''}`}
              />
              <text x={pos[0]} y={pos[1] + 1} className={styles.nodeLabel} textAnchor="middle" dominantBaseline="middle">
                {role === 'leader' ? '★' : '○'}
              </text>
              <text x={pos[0]} y={pos[1] + R + 12} className={styles.nodeName} textAnchor="middle">
                {node.name}
              </text>
              <text x={pos[0]} y={pos[1] + R + 22} className={styles.nodeRole} textAnchor="middle">
                {role}
              </text>
            </g>
          )
        })}
      </svg>
    </div>
  )
}
