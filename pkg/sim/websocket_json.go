package sim

import (
	"context"

	"nhooyr.io/websocket"
	"nhooyr.io/websocket/wsjson"
)

func wsjsonWrite(ctx context.Context, conn *websocket.Conn, value any) error {
	return wsjson.Write(ctx, conn, value)
}
