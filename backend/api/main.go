// backend/api/main.go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
)

type KlineRecord struct {
	Time   time.Time `json:"time"`
	Symbol string    `json:"symbol"`
	Open   float64   `json:"open"`
	High   float64   `json:"high"`
	Low    float64   `json:"low"`
	Close  float64   `json:"close"`
	SMA    *float64  `json:"sma,omitempty"`
}

func calculateSMA(data []KlineRecord, period int) {
	if len(data) < period {
		return
	}
	for i := range data {
		if i >= period-1 {
			sum := 0.0
			for j := i - (period - 1); j <= i; j++ {
				sum += data[j].Close
			}
			smaValue := sum / float64(period)
			data[i].SMA = &smaValue
		}
	}
}

var upgrader = websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
type Subscription struct { Symbol string `json:"symbol"`; Interval string `json:"interval"`}
var clients = make(map[*websocket.Conn]Subscription)
var clientsMutex = &sync.Mutex{}

func wsHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("WS upgrade error:", err)
		return
	}
	defer conn.Close()

	clientsMutex.Lock()
	clients[conn] = Subscription{}
	clientsMutex.Unlock()
	log.Printf("✅ New client connected. Total: %d", len(clients))

	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			break
		}
		var sub Subscription
		if err := json.Unmarshal(message, &sub); err == nil {
			clientsMutex.Lock()
			if _, ok := clients[conn]; ok {
				clients[conn] = sub
				log.Printf("✅ Client subscribed to %s/%s", sub.Symbol, sub.Interval)
			}
			clientsMutex.Unlock()
		}
	}

	clientsMutex.Lock()
	delete(clients, conn)
	clientsMutex.Unlock()
	log.Printf("❌ Client disconnected. Total: %d", len(clients))
}

func getKlinesHandler(dbpool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		symbol := strings.ToUpper(r.URL.Query().Get("symbol"))
		interval := r.URL.Query().Get("interval")
		if symbol == "" || interval == "" {
			http.Error(w, "Missing params", http.StatusBadRequest)
			return
		}
		tableName := fmt.Sprintf("klines_%s", interval)
		querySQL := fmt.Sprintf(`SELECT time, symbol, open, high, low, close FROM %s WHERE symbol = $1 ORDER BY time ASC LIMIT 1020;`, tableName)
		rows, err := dbpool.Query(context.Background(), querySQL, symbol)
		if err != nil {
			http.Error(w, "DB query failed", http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		var klines []KlineRecord
		for rows.Next() {
			var k KlineRecord
			if err := rows.Scan(&k.Time, &k.Symbol, &k.Open, &k.High, &k.Low, &k.Close); err != nil {
				log.Printf("DB Scan Error: %v", err)
				continue
			}
			klines = append(klines, k)
		}
		calculateSMA(klines, 20)
		responseKlines := klines
		if len(klines) > 1000 {
			responseKlines = klines[len(klines)-1000:]
		}

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		json.NewEncoder(w).Encode(responseKlines)
	}
}


type WsMessage struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

func main() {
	databaseUrl := "postgresql://tsdbadmin:iel6cdjrl4i2eekq@i45clmnge1.hinxe82a9b.tsdb.cloud.timescale.com:34526/tsdb?sslmode=require"
	dbpool, err := pgxpool.New(context.Background(), databaseUrl)
	if err != nil {
		log.Fatalf("DB connect error: %v\n", err)
	}
	defer dbpool.Close()
	log.Println("✅ API service connected to TimescaleDB.")
	
	natsURL := "nats://nats:4222"
	nc, err := nats.Connect(natsURL)
	if err != nil {
		log.Fatalf("NATS connect error: %v\n", err)
	}
	defer nc.Close()
	log.Println("✅ API service connected to NATS server.")

	// Broadcaster function to send messages to relevant clients
	broadcaster := func(msg *nats.Msg, msgType string) {
		wsMsg, _ := json.Marshal(WsMessage{Type: msgType, Data: msg.Data})
		
		var targetSymbol, targetInterval string
		parts := strings.Split(msg.Subject, ".")

		switch msgType {
		case "kline":
			if len(parts) != 3 { return }
			targetSymbol, targetInterval = strings.ToUpper(parts[1]), parts[2]
		case "trade":
			if len(parts) != 2 { return }
			targetSymbol = strings.ToUpper(parts[1])
		case "orderbook":
			if len(parts) != 3 { return }
			targetSymbol = strings.ToUpper(parts[2])
		default:
			return
		}

		clientsMutex.Lock()
		defer clientsMutex.Unlock()
		for client, sub := range clients {
			if sub.Symbol == targetSymbol {
				// Send trades and orderbooks to clients viewing the correct symbol
				// Send klines only if interval also matches
				if msgType == "trade" || msgType == "orderbook" || (msgType == "kline" && sub.Interval == targetInterval) {
					if err := client.WriteMessage(websocket.TextMessage, wsMsg); err != nil {
						log.Printf("WS write error to client: %v", err)
						delete(clients, client)
					}
				}
			}
		}
	}

	// Subscribe to all relevant NATS subjects
	nc.Subscribe("klines.*.*", func(msg *nats.Msg) { broadcaster(msg, "kline") })
	nc.Subscribe("trades.*", func(msg *nats.Msg) { broadcaster(msg, "trade") })
	nc.Subscribe("orderbook.snapshot.*", func(msg *nats.Msg) { broadcaster(msg, "orderbook") })


	http.HandleFunc("/klines", getKlinesHandler(dbpool))
	http.HandleFunc("/ws", wsHandler)

	log.Println("✅ API service starting on http://localhost:8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}