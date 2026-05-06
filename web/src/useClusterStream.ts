import { useQueryClient } from '@tanstack/react-query'
import { useEffect, useMemo, useState } from 'react'

import { clusterSnapshotSchema, type ClusterEvent, envelopeSchema, eventSchema, websocketUrl } from './api'

type StreamState = 'connecting' | 'connected' | 'offline'

export function useClusterStream() {
  const queryClient = useQueryClient()
  const [status, setStatus] = useState<StreamState>('connecting')
  const [events, setEvents] = useState<ClusterEvent[]>([])

  useEffect(() => {
    let closed = false
    let socket: WebSocket | null = null
    let retry: number | undefined
    let attempt = 0

    const connect = () => {
      setStatus('connecting')
      socket = new WebSocket(websocketUrl())

      socket.onopen = () => {
        attempt = 0
        setStatus('connected')
      }

      socket.onmessage = (message) => {
        const envelope = envelopeSchema.safeParse(JSON.parse(message.data))
        if (!envelope.success) {
          return
        }

        if (envelope.data.type === 'cluster.snapshot') {
          const snapshot = clusterSnapshotSchema.safeParse(envelope.data.data)
          if (snapshot.success) {
            queryClient.setQueryData(['cluster'], snapshot.data)
          }
          return
        }

        if (envelope.data.type === 'event') {
          const event = eventSchema.safeParse(envelope.data.data)
          if (event.success) {
            setEvents((current) => [event.data, ...current.filter((item) => item.id !== event.data.id)].slice(0, 250))
            queryClient.invalidateQueries({ queryKey: ['cluster'] })
          }
        }
      }

      socket.onclose = () => {
        if (closed) {
          return
        }
        setStatus('offline')
        const delay = Math.min(8_000, 500 * 2 ** attempt)
        attempt += 1
        retry = window.setTimeout(connect, delay)
      }

      socket.onerror = () => socket?.close()
    }

    connect()
    return () => {
      closed = true
      if (retry) {
        window.clearTimeout(retry)
      }
      socket?.close()
    }
  }, [queryClient])

  return useMemo(() => ({ status, events }), [status, events])
}
