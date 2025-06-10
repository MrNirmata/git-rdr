'use client';

import React, { useEffect, useRef } from 'react';
import { createChart, ColorType } from 'lightweight-charts';
import useWebSocket from 'react-use-websocket';

// Props და Stream-ის მონაცემების ტიპები
interface ChartComponentProps {
  initialData: { time: string; open: number; high: number; low: number; close: number; }[];
  symbol: string;
  interval: string;
}
interface KlineStreamData { s: string; k: { t: number; i: string; o: string; h: string; l: string; c: string; };}
interface TradeStreamData { s: string; p: string; }

export const ChartComponent: React.FC<ChartComponentProps> = ({ initialData, symbol, interval }) => {
  const chartContainerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<any>(null);
  const seriesRef = useRef<any>(null);
  const lastCandleRef = useRef<any>(null);

  const { lastJsonMessage, sendJsonMessage } = useWebSocket('ws://localhost:8080/ws', {
    onOpen: () => {
      console.log(`✅ WebSocket connected, subscribing to ${symbol}/${interval}`);
      sendJsonMessage({ symbol, interval });
    },
    shouldReconnect: (closeEvent) => true,
  });

  // ეფექტი #1: გრაფიკის შექმნა
  useEffect(() => {
    if (!chartContainerRef.current) return;
    const chart = createChart(chartContainerRef.current, {
        layout: { background: { type: ColorType.Solid, color: '#131722' }, textColor: '#D9D9D9' },
        grid: { vertLines: { color: '#2A2E39' }, horzLines: { color: '#2A2E39' } },
        width: chartContainerRef.current.clientWidth, height: 500,
    });
    const candlestickSeries = chart.addCandlestickSeries({
        upColor: '#26a69a', downColor: '#ef5350', borderDownColor: '#ef5350',
        borderUpColor: '#26a69a', wickDownColor: '#ef5350', wickUpColor: '#26a69a',
    });
    chartRef.current = chart;
    seriesRef.current = candlestickSeries;
    const handleResize = () => chart.applyOptions({ width: chartContainerRef.current!.clientWidth });
    window.addEventListener('resize', handleResize);
    return () => { window.removeEventListener('resize', handleResize); chart.remove(); };
  }, []);

  // ეფექტი #2: ისტორიული მონაცემების დაყენება
  useEffect(() => {
    if (seriesRef.current && initialData?.length > 0) {
      // --- მთავარი შესწორება: ფორმატირება ხდება აქ ---
      const formattedData = initialData.map(d => ({
        time: new Date(d.time).getTime() / 1000,
        open: d.open,
        high: d.high,
        low: d.low,
        close: d.close,
      }));
      seriesRef.current.setData(formattedData);
      lastCandleRef.current = formattedData[formattedData.length - 1];
      chartRef.current?.timeScale().fitContent();
    }
  }, [initialData]);

  // ეფექტი #3: რეალური დროის მონაცემების დამუშავება
  useEffect(() => {
    if (!lastJsonMessage || !seriesRef.current) return;
    const message = lastJsonMessage as any;

    if (message.type === 'kline') {
      const kline = message.data.k;
      if (kline.s === symbol && kline.i === interval) {
        const newCandle = {
            time: kline.t / 1000,
            open: parseFloat(kline.o), high: parseFloat(kline.h),
            low: parseFloat(kline.l), close: parseFloat(kline.c),
        };
        seriesRef.current.update(newCandle);
        lastCandleRef.current = newCandle;
      }
    } else if (message.type === 'trade') {
      const trade = message.data;
      if (trade.s === symbol && lastCandleRef.current) {
        const tradePrice = parseFloat(trade.p);
        const updatedCandle = { ...lastCandleRef.current };
        updatedCandle.close = tradePrice;
        if (tradePrice > updatedCandle.high) updatedCandle.high = tradePrice;
        if (tradePrice < updatedCandle.low) updatedCandle.low = tradePrice;
        seriesRef.current.update(updatedCandle);
        lastCandleRef.current = updatedCandle;
      }
    }
  }, [lastJsonMessage, symbol, interval]);

  return <div ref={chartContainerRef} />;
};