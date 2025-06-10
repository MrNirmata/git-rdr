package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings" // We keep this one because it's used in the kline subscriber
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
)

// --- Configuration ---
var klineIntervals = []string{"1m", "5m"}

// --- Data Structures (unchanged) ---
type BinanceTrade struct {
	EventType    string `json:"e"`
	EventTime    int64  `json:"E"`
	Symbol       string `json:"s"`
	TradeID      int64  `json:"t"`
	Price        string `json:"p"`
	Quantity     string `json:"q"`
	TradeTime    int64  `json:"T"`
	IsBuyerMaker bool   `json:"m"`
}
type BinanceKlinePayload struct {
	EventType string       `json:"e"`
	EventTime int64        `json:"E"`
	Symbol    string       `json:"s"`
	KlineData BinanceKline `json:"k"`
}
type BinanceKline struct {
	StartTime      int64       `json:"t"`
	CloseTime      int64       `json:"T"`
	Symbol         string      `json:"s"`
	Interval       string      `json:"i"`
	OpenPrice      json.Number `json:"o"`
	ClosePrice     json.Number `json:"c"`
	HighPrice      json.Number `json:"h"`
	LowPrice       json.Number `json:"l"`
	Volume         json.Number `json:"v"`
	NumberOfTrades int64       `json:"n"`
	IsClosed       bool        `json:"x"`
}

// --- Dynamic Database Setup (unchanged) ---
func setupDatabase(dbpool *pgxpool.Pool) {
	// Setup trades table
	_, err := dbpool.Exec(context.Background(), `
	CREATE TABLE IF NOT EXISTS trades (
		time TIMESTAMPTZ NOT NULL,
		trade_id BIGINT,
		symbol TEXT,
		price DOUBLE PRECISION,
		quantity DOUBLE PRECISION,
		is_buyer_maker BOOLEAN,
		UNIQUE (time, trade_id)
	);`)
	if err != nil { log.Fatalf("Unable to create trades table: %v\n", err) }
	_, err = dbpool.Exec(context.Background(), `SELECT create_hypertable('trades', 'time', if_not_exists => TRUE);`)
	if err != nil { log.Fatalf("Unable to create trades hypertable: %v\n", err) }
	log.Println("✅ Database table 'trades' is ready.")

	// Loop through configured intervals and create a table for each
	for _, interval := range klineIntervals {
		tableName := fmt.Sprintf("klines_%s", interval)
		createTableSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			time TIMESTAMPTZ NOT NULL,
			symbol TEXT,
			open DOUBLE PRECISION,
			high DOUBLE PRECISION,
			low DOUBLE PRECISION,
			close DOUBLE PRECISION,
			volume DOUBLE PRECISION,
			is_closed BOOLEAN,
			UNIQUE (time, symbol)
		);`, tableName)
		createHypertableSQL := fmt.Sprintf(`SELECT create_hypertable('%s', 'time', if_not_exists => TRUE);`, tableName)
		_, err := dbpool.Exec(context.Background(), createTableSQL)
		if err != nil { log.Fatalf("Unable to create table %s: %v\n", tableName, err) }
		_, err = dbpool.Exec(context.Background(), createHypertableSQL)
		if err != nil { log.Fatalf("Unable to create hypertable for %s: %v\n", tableName, err) }
		log.Printf("✅ Database table '%s' is ready.", tableName)
	}
}


func main() {
	databaseUrl := "postgresql://tsdbadmin:iel6cdjrl4i2eekq@i45clmnge1.hinxe82a9b.tsdb.cloud.timescale.com:34526/tsdb?sslmode=require"
	dbpool, err := pgxpool.New(context.Background(), databaseUrl)
	if err != nil { log.Fatalf("DB connect error: %v\n", err) }
	defer dbpool.Close()
	log.Println("✅ Archiver service connected to TimescaleDB.")
	setupDatabase(dbpool)

	natsURL := "nats://nats:4222"
	nc, err := nats.Connect(natsURL)
	if err != nil { log.Fatalf("NATS connect error: %v\n", err) }
	defer nc.Close()
	log.Println("✅ Archiver service connected to NATS server.")

	// Wildcard subscription for all trades
	nc.Subscribe("trades.*", func(msg *nats.Msg) {
		var trade BinanceTrade
		json.Unmarshal(msg.Data, &trade)
		insertSQL := `INSERT INTO trades (time, trade_id, symbol, price, quantity, is_buyer_maker) 
					  VALUES ($1, $2, $3, $4, $5, $6) 
					  ON CONFLICT(time, trade_id) DO NOTHING`
		tradeTimestamp := time.Unix(0, trade.TradeTime*int64(time.Millisecond))
		_, err = dbpool.Exec(context.Background(), insertSQL, tradeTimestamp, trade.TradeID, trade.Symbol, trade.Price, trade.Quantity, trade.IsBuyerMaker)
		if err != nil { log.Printf("Failed to insert trade for %s: %v", trade.Symbol, err) }
	})

	// Wildcard subscription for all klines
	nc.Subscribe("klines.*.*", func(msg *nats.Msg) {
		parts := strings.Split(msg.Subject, ".")
		if len(parts) != 3 { return }
		interval := parts[2]
		tableName := fmt.Sprintf("klines_%s", interval)

		var payload BinanceKlinePayload
		if err := json.Unmarshal(msg.Data, &payload); err != nil { log.Printf("Error unmarshaling kline: %v", err); return }
		kline := payload.KlineData

		if kline.IsClosed { log.Printf("Archiving closed kline for %s on interval %s", kline.Symbol, kline.Interval) }

		open, _ := kline.OpenPrice.Float64()
		high, _ := kline.HighPrice.Float64()
		low, _ := kline.LowPrice.Float64()
		close, _ := kline.ClosePrice.Float64()
		volume, _ := kline.Volume.Float64()
		klineTimestamp := time.Unix(0, kline.StartTime*int64(time.Millisecond))

		insertSQL := fmt.Sprintf(`
			INSERT INTO %s (time, symbol, open, high, low, close, volume, is_closed) 
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
			ON CONFLICT (time, symbol) DO UPDATE SET
				high = GREATEST(%s.high, EXCLUDED.high),
				low = LEAST(%s.low, EXCLUDED.low),
				close = EXCLUDED.close,
				volume = EXCLUDED.volume,
				is_closed = EXCLUDED.is_closed;
		`, tableName, tableName, tableName)
		_, err = dbpool.Exec(context.Background(), insertSQL, klineTimestamp, kline.Symbol, open, high, low, close, volume, kline.IsClosed)
		if err != nil { log.Printf("Failed to insert/update kline into %s: %v", tableName, err) }
	})

	log.Println("Archiver is now listening to all configured streams...")
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("👋 Archiver service shutting down...")
}