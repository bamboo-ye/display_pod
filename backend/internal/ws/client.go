package ws

import (
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

type Client struct {
	hub  *Hub
	conn *websocket.Conn
	send chan Event
}

func Serve(hub *Hub, allowOrigin string, writer http.ResponseWriter, request *http.Request) {
	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return allowOrigin == "*" || r.Header.Get("Origin") == allowOrigin || r.Header.Get("Origin") == ""
		},
	}
	conn, err := upgrader.Upgrade(writer, request, nil)
	if err != nil {
		return
	}
	client := &Client{hub: hub, conn: conn, send: make(chan Event, 16)}
	hub.Register(client)
	go client.writePump()
	go client.readPump()
}

func (c *Client) readPump() {
	defer func() {
		c.hub.Unregister(c)
		c.conn.Close()
	}()
	c.conn.SetReadLimit(512)
	c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})
	for {
		if _, _, err := c.conn.ReadMessage(); err != nil {
			return
		}
	}
}

func (c *Client) writePump() {
	ticker := time.NewTicker(30 * time.Second)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()
	for {
		select {
		case event, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			if err := c.conn.WriteJSON(event); err != nil {
				return
			}
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}
