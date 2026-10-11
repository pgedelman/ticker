package main

import (
	"encoding/json"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"strconv"
	"ticker/internal/raft"
	"time"

	"github.com/gorilla/websocket"
)

func main() {
	registry := raft.NewNetworkRegistry()

	nodes := make([]*raft.Node, 0, 3)
	for id := raft.NodeID(1); id <= 3; id++ {
		node := raft.NewNode(id, registry)
		registry.Register(node)
		nodes = append(nodes, node)
	}

	for _, node := range nodes {
		go node.Start()
	}

	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		seq := 0
		for range ticker.C {
			seq++
			tick, _ := json.Marshal(map[string]any{"symbol": "AAPL", "price": 190 + rand.Float64()*10, "seq": seq})
			for _, n := range nodes {
				n.Propose(tick) // no-op on non-leaders, same pattern as cmd/node
			}
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", wsHandler(nodes))
	mux.HandleFunc("POST /nodes/{id}/kill", killHandler(nodes))
	mux.HandleFunc("POST /nodes/{id}/revive", reviveHandler(nodes))
	mux.Handle("/", http.FileServer(http.Dir("web/dist")))

	port := os.Getenv("PORT")
	if port == "" {
		port = "8000"
	}
	log.Printf("listening on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}

func nodeFromRequest(nodes []*raft.Node, r *http.Request) (*raft.Node, bool) {
	id, err := strconv.Atoi(r.PathValue("id"))
	if err != nil {
		return nil, false
	}
	for _, n := range nodes {
		if int(n.ID()) == id {
			return n, true
		}
	}
	return nil, false
}

func killHandler(nodes []*raft.Node) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		node, ok := nodeFromRequest(nodes, r)
		if !ok {
			http.Error(w, "unknown node id", http.StatusNotFound)
			return
		}
		node.Kill()
		w.WriteHeader(http.StatusNoContent)
	}
}

func reviveHandler(nodes []*raft.Node) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		node, ok := nodeFromRequest(nodes, r)
		if !ok {
			http.Error(w, "unknown node id", http.StatusNotFound)
			return
		}
		node.Revive()
		w.WriteHeader(http.StatusNoContent)
	}
}

func wsHandler(nodes []*raft.Node) http.HandlerFunc {
	var upgrader websocket.Upgrader
	return func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		ticker := time.NewTicker(150 * time.Millisecond)
		defer ticker.Stop()
		for range ticker.C {
			views := make([]raft.NodeView, len(nodes))
			for i, n := range nodes {
				views[i] = n.View()
			}
			if err := conn.WriteJSON(views); err != nil {
				return // client disconnected
			}
		}
	}
}
