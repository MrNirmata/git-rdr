package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/gorilla/websocket"
	"github.com/nats-io/nats.go"
)

// --- კონფიგურაცია ---
// აქ ვწერთ, რომელი წყვილები და ინტერვალები გვაინტერესებს
var symbols = []string{"btcusdt", "ethusdt", "solusdt"}
var klineIntervals = []string{"1m", "5m"}

// ფუნქცია ტრეიდების ნაკადთან დასაკავშირებლად
func connectToTradeStream(nc *nats.Conn, symbol string) {
	streamURL := fmt.Sprintf("wss://stream.binance.com:9443/ws/%s@trade", symbol)
	log.Printf("Connecting to Trade Stream: %s", streamURL)

	c, _, err := websocket.DefaultDialer.Dial(streamURL, nil)
	if err != nil {
		log.Printf("Trade stream WebSocket dial error for %s: %v", streamURL, err)
		return
	}
	defer c.Close()
	log.Printf("✅ Successfully connected to Binance Trade stream for %s.", symbol)

	natsSubject := fmt.Sprintf("trades.%s", symbol)

	for {
		_, message, err := c.ReadMessage()
		if err != nil {
			log.Printf("Trade stream read error for %s: %v", streamURL, err)
			return
		}
		err = nc.Publish(natsSubject, message)
		if err != nil {
			log.Printf("Error publishing trade to NATS on subject %s: %v", natsSubject, err)
		}
	}
}

// ფუნქცია სანთლების (K-Line) ნაკადთან დასაკავშირებლად
func connectToKlineStream(nc *nats.Conn, symbol string, interval string) {
	streamURL := fmt.Sprintf("wss://stream.binance.com:9443/ws/%s@kline_%s", symbol, interval)
	log.Printf("Connecting to Kline Stream: %s", streamURL)

	c, _, err := websocket.DefaultDialer.Dial(streamURL, nil)
	if err != nil {
		log.Printf("Kline stream WebSocket dial error for %s: %v", streamURL, err)
		return
	}
	defer c.Close()
	log.Printf("✅ Successfully connected to Binance Kline stream for %s/%s.", symbol, interval)

	natsSubject := fmt.Sprintf("klines.%s.%s", symbol, interval)

	for {
		_, message, err := c.ReadMessage()
		if err != nil {
			log.Printf("Kline stream read error for %s: %v", streamURL, err)
			return
		}
		err = nc.Publish(natsSubject, message)
		if err != nil {
			log.Printf("Error publishing kline to NATS on subject %s: %v", natsSubject, err)
		}
	}
}

// ახალი ფუნქცია Order Book-ის (Depth) ნაკადთან დასაკავშირებლად
func connectToDepthStream(nc *nats.Conn, symbol string) {
	streamURL := fmt.Sprintf("wss://stream.binance.com:9443/ws/%s@depth", symbol)
	log.Printf("Connecting to Depth Stream: %s", streamURL)

	c, _, err := websocket.DefaultDialer.Dial(streamURL, nil)
	if err != nil {
		log.Printf("Depth stream WebSocket dial error for %s: %v", streamURL, err)
		return
	}
	defer c.Close()
	log.Printf("✅ Successfully connected to Binance Depth stream for %s.", symbol)

	natsSubject := fmt.Sprintf("depth.%s", symbol)

	for {
		_, message, err := c.ReadMessage()
		if err != nil {
			log.Printf("Depth stream read error for %s: %v", streamURL, err)
			return
		}
		// ვაქვეყნებთ Order Book-ის განახლებას NATS-ის შესაბამის თემაზე
		err = nc.Publish(natsSubject, message)
		if err != nil {
			log.Printf("Error publishing depth to NATS on subject %s: %v", natsSubject, err)
		}
	}
}


func main() {
	natsURL := "nats://nats:4222"
	nc, err := nats.Connect(natsURL)
	if err != nil {
		log.Fatalf("ERROR: could not connect to NATS: %v", err)
	}
	defer nc.Close()
	log.Println("✅ Ingestor service successfully connected to NATS server at", natsURL)

	// ვიწყებთ ყველა ნაკადის გაშვებას კონფიგურაციის მიხედვით
	for _, symbol := range symbols {
		// ვიწყებთ ტრეიდების ნაკადს
		go connectToTradeStream(nc, symbol)

		// ვიწყებთ სანთლების ნაკადს
		for _, interval := range klineIntervals {
			go connectToKlineStream(nc, symbol, interval)
		}

		// ვიწყებთ Order Book-ის ნაკადს
		go connectToDepthStream(nc, symbol)
	}

	// ველით შეწყვეტის სიგნალს
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("👋 Ingestor service shutting down...")
}