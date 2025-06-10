'use client';

import { ChartComponent } from './ChartComponent';
import { OrderBook } from './OrderBook';
import React, { useState, useEffect, useCallback } from 'react';
import useWebSocket from 'react-use-websocket';

const SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"];
const INTERVALS = ["1m", "5m"];

export default function HomePage() {
  const [symbol, setSymbol] = useState(SYMBOLS[0]);
  const [interval, setInterval] = useState(INTERVALS[0]);
  
  const [klineData, setKlineData] = useState<any[]>([]);
  const [orderBookData, setOrderBookData] = useState<any>({ bids: [], asks: [] });
  const [isLoading, setIsLoading] = useState(true);

  // --- ისტორიული მონაცემების წამოღება ---
  const fetchInitialData = useCallback(async (currentSymbol: string, currentInterval: string) => {
    setIsLoading(true);
    try {
      const klineRes = await fetch(`/api/klines?symbol=${currentSymbol}&interval=${currentInterval}`);
      if (!klineRes.ok) throw new Error(`Kline fetch failed`);
      const klineJson = await klineRes.json();
      setKlineData(klineJson || []); // ვაწვდით პირდაპირ, დაუმუშავებელ მონაცემს

      const orderBookRes = await fetch(`http://localhost:8081/orderbook?symbol=${currentSymbol}`);
      if (!orderBookRes.ok) throw new Error(`OrderBook fetch failed`);
      const orderBookJson = await orderBookRes.json();
      setOrderBookData(orderBookJson || { bids: [], asks: [] });

    } catch (error) {
      console.error("Error fetching initial data:", error);
      setKlineData([]);
      setOrderBookData({ bids: [], asks: [] });
    } finally {
      setIsLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchInitialData(symbol, interval);
  }, [symbol, interval, fetchInitialData]);

  // --- WebSocket კავშირის და მონაცემების მართვა ---
  const { lastJsonMessage, sendJsonMessage } = useWebSocket('ws://localhost:8080/ws', {
    onOpen: () => console.log(`✅ Central WebSocket connected.`),
    shouldReconnect: (closeEvent) => true,
  });

  useEffect(() => {
    console.log(`Subscribing to ${symbol}/${interval}`);
    sendJsonMessage({ symbol, interval });
  }, [symbol, interval, sendJsonMessage]);
  
  useEffect(() => {
    if (!lastJsonMessage) return;
    const message = lastJsonMessage as any;
    
    if (message.type === 'orderbook') {
        const book = message.data;
        if (book.symbol === symbol) {
            setOrderBookData(book);
        }
    }
  }, [lastJsonMessage, symbol]);


  return (
    <main className="flex min-h-screen flex-col items-center p-6 sm:p-12 bg-gray-900 text-white">
      <h1 className="text-3xl sm:text-4xl font-bold mb-4">Crypto Platform v3.0</h1>
      
      <div className="flex gap-4 mb-8">
         <div>
          <label htmlFor="symbol-select" className="block text-sm font-medium text-gray-400">Symbol</label>
          <select id="symbol-select" value={symbol} onChange={(e) => setSymbol(e.target.value)} className="bg-gray-700 border border-gray-600 text-white text-sm rounded-lg focus:ring-blue-500 focus:border-blue-500 block w-full p-2.5" >
            {SYMBOLS.map(s => <option key={s} value={s}>{s}</option>)}
          </select>
        </div>
        <div>
          <label htmlFor="interval-select" className="block text-sm font-medium text-gray-400">Interval</label>
          <select id="interval-select" value={interval} onChange={(e) => setInterval(e.target.value)} className="bg-gray-700 border border-gray-600 text-white text-sm rounded-lg focus:ring-blue-500 focus:border-blue-500 block w-full p-2.5">
            {INTERVALS.map(i => <option key={i} value={i}>{i}</option>)}
          </select>
        </div>
      </div>

      <div className="w-full max-w-7xl mx-auto grid grid-cols-1 lg:grid-cols-3 gap-6">
        <div className="lg:col-span-2 bg-[#1e222d] p-4 rounded-lg shadow-2xl">
          {isLoading ? (
              <div className="w-full h-[500px] flex items-center justify-center"><p className="text-gray-400">Loading data...</p></div>
          ) : (
              <ChartComponent 
                initialData={klineData} 
                symbol={symbol} 
                interval={interval}
                lastJsonMessage={lastJsonMessage}
              />
          )}
        </div>
        <div className="lg:col-span-1">
            <OrderBook bids={orderBookData.bids} asks={orderBookData.asks} />
        </div>
      </div>
    </main>
  );
}