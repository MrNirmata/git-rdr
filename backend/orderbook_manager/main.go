package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings" // <-- ეს ხაზი დაბრუნებულია
	"sync"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
)

// --- კონფიგურაცია ---
var symbols = []string{"BTCUSDT", "ETHUSDT", "SOLUSDT"}

// --- მონაცემთა სტრუქტურები ---
type OrderLevel [2]string

type OrderBookSnapshot struct {
	LastUpdateID int64        `json:"lastUpdateId"`
	Bids         []OrderLevel `json:"bids"`
	Asks         []OrderLevel `json:"asks"`
}

type DepthUpdate struct {
	EventType     string       `json:"e"`
	EventTime     int64        `json:"E"`
	Symbol        string       `json:"s"`
	FirstUpdateID int64        `json:"U"`
	FinalUpdateID int64        `json:"u"`
	Bids          []OrderLevel `json:"b"`
	Asks          []OrderLevel `json:"a"`
}

// --- Order Book-ის მენეჯერი ---
type OrderBook struct {
	Bids         map[string]float64
	Asks         map[string]float64
	LastUpdateID int64
	mu           sync.RWMutex
}
type OrderBookManager struct {
	books map[string]*OrderBook
	mu    sync.RWMutex
}

type SortedLevel struct {
	Price  float64 `json:"price"`
	Amount float64 `json:"amount"`
}

func (obm *OrderBookManager) getBook(symbol string) *OrderBook {
	obm.mu.RLock()
	defer obm.mu.RUnlock()
	return obm.books[symbol]
}

func (obm *OrderBookManager) fetchSnapshot(symbol string) {
	log.Printf("Fetching snapshot for %s...", symbol)
	url := fmt.Sprintf("https://api.binance.com/api/v3/depth?symbol=%s&limit=1000", symbol)
	resp, err := http.Get(url)
	if err != nil {
		log.Printf("Error fetching snapshot for %s: %v", symbol, err)
		return
	}
	defer resp.Body.Close()

	var snapshot OrderBookSnapshot
	if err := json.NewDecoder(resp.Body).Decode(&snapshot); err != nil {
		log.Printf("Error decoding snapshot for %s: %v", symbol, err)
		return
	}

	bids := make(map[string]float64)
	asks := make(map[string]float64)
	for _, bid := range snapshot.Bids {
		qty, _ := strconv.ParseFloat(bid[1], 64)
		bids[bid[0]] = qty
	}
	for _, ask := range snapshot.Asks {
		qty, _ := strconv.ParseFloat(ask[1], 64)
		asks[ask[0]] = qty
	}

	obm.mu.Lock()
	obm.books[symbol] = &OrderBook{Bids: bids, Asks: asks, LastUpdateID: snapshot.LastUpdateID}
	obm.mu.Unlock()
	log.Printf("✅ Initial snapshot for %s loaded. LastUpdateID: %d", symbol, snapshot.LastUpdateID)
}

func (obm *OrderBookManager) applyUpdate(update DepthUpdate) {
	book := obm.getBook(update.Symbol)
	if book == nil {
		return
	}

	book.mu.Lock()
	defer book.mu.Unlock()

	if update.FinalUpdateID <= book.LastUpdateID {
		return
	}
	if update.FirstUpdateID > book.LastUpdateID+1 {
		log.Printf("Gap detected in %s order book. Refetching snapshot...", update.Symbol)
		go obm.fetchSnapshot(update.Symbol)
		return
	}

	updateLevels := func(levels map[string]float64, updates []OrderLevel) {
		for _, u := range updates {
			qty, _ := strconv.ParseFloat(u[1], 64)
			if qty == 0 {
				delete(levels, u[0])
			} else {
				levels[u[0]] = qty
			}
		}
	}
	updateLevels(book.Bids, update.Bids)
	updateLevels(book.Asks, update.Asks)
	book.LastUpdateID = update.FinalUpdateID
}

func (obm *OrderBookManager) getOrderBookHandler(w http.ResponseWriter, r *http.Request) {
	symbol := strings.ToUpper(r.URL.Query().Get("symbol"))
	if symbol == "" {
		http.Error(w, "Missing symbol parameter", http.StatusBadRequest)
		return
	}

	book := obm.getBook(symbol)
	if book == nil {
		http.NotFound(w, r)
		return
	}

	book.mu.RLock()
	defer book.mu.RUnlock()

	bids := make([]SortedLevel, 0, len(book.Bids))
	asks := make([]SortedLevel, 0, len(book.Asks))
	for p, a := range book.Bids {
		price, _ := strconv.ParseFloat(p, 64)
		bids = append(bids, SortedLevel{Price: price, Amount: a})
	}
	for p, a := range book.Asks {
		price, _ := strconv.ParseFloat(p, 64)
		asks = append(asks, SortedLevel{Price: price, Amount: a})
	}

	sort.Slice(bids, func(i, j int) bool { return bids[i].Price > bids[j].Price })
	sort.Slice(asks, func(i, j int) bool { return asks[i].Price < asks[j].Price })

	if len(bids) > 20 {
		bids = bids[:20]
	}
	if len(asks) > 20 {
		asks = asks[:20]
	}

	response := map[string]interface{}{"bids": bids, "asks": asks, "lastUpdateId": book.LastUpdateID}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	json.NewEncoder(w).Encode(response)
}


func main() {
	obm := &OrderBookManager{books: make(map[string]*OrderBook)}
	for _, s := range symbols {
		go obm.fetchSnapshot(s)
	}

	natsURL := "nats://nats:4222"
	nc, err := nats.Connect(natsURL)
	if err != nil {
		log.Fatalf("NATS connect error: %v\n", err)
	}
	defer nc.Close()
	log.Println("✅ Orderbook Manager service connected to NATS server.")

	nc.Subscribe("depth.*", func(msg *nats.Msg) {
		var update DepthUpdate
		if err := json.Unmarshal(msg.Data, &update); err == nil {
			obm.applyUpdate(update)
		}
	})

	http.HandleFunc("/orderbook", obm.getOrderBookHandler)
	go func() {
		log.Println("✅ Orderbook Manager's API is starting on http://localhost:8081")
		if err := http.ListenAndServe(":8081", nil); err != nil {
			log.Fatalf("Orderbook Manager failed to start server: %v", err)
		}
	}()

	// დემონსტრაციისთვის: ყოველ 5 წამში ვბეჭდავთ BTCUSDT-ს Order Book-ის ზომას
	go func() {
		for {
			time.Sleep(5 * time.Second)
			book := obm.getBook("BTCUSDT")
			if book != nil {
				book.mu.RLock()
				log.Printf("[STATUS] BTCUSDT Order Book state: Bids=%d levels, Asks=%d levels, LastUpdateID=%d", len(book.Bids), len(book.Asks), book.LastUpdateID)
				book.mu.RUnlock()
			}
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("👋 Orderbook Manager service shutting down...")
}