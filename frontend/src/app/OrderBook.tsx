// frontend/src/app/OrderBook.tsx
'use client';

import React from 'react';

// ინტერფეისი გასწორებულია backend-ის შესაბამისად (lowercase)
interface OrderLevel {
  price: number;
  amount: number;
}

interface OrderBookProps {
  bids: OrderLevel[];
  asks: OrderLevel[];
}

export const OrderBook: React.FC<OrderBookProps> = ({ bids, asks }) => {
  if (!bids || !asks) {
    return <div className="text-center text-gray-400">Loading Order Book...</div>;
  }

  return (
    <div className="bg-[#1e222d] p-4 rounded-lg shadow-lg text-xs w-full">
      <h3 className="text-lg font-bold text-center mb-4 text-white">Order Book</h3>
      <div className="grid grid-cols-2 gap-4">
        {/* Asks (გამყიდველები) */}
        <div>
          <table className="w-full">
            <thead>
              <tr className="text-gray-400">
                <th className="text-left font-normal px-2">Price (USDT)</th>
                <th className="text-right font-normal px-2">Amount</th>
              </tr>
            </thead>
            <tbody>
              {/* ვასწორებთ .Price-ს .price-ზე და .Amount-ს .amount-ზე */}
              {[...asks].reverse().map((ask, index) => (
                <tr key={index} className="relative h-6">
                  <td className="text-red-400 px-2">{ask.price.toFixed(2)}</td>
                  <td className="text-right px-2">{ask.amount.toFixed(4)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        {/* Bids (შემსყიდველები) */}
        <div>
          <table className="w-full">
            <thead>
              <tr className="text-gray-400">
                <th className="text-left font-normal px-2">Price (USDT)</th>
                <th className="text-right font-normal px-2">Amount</th>
              </tr>
            </thead>
            <tbody>
              {/* ვასწორებთ .Price-ს .price-ზე და .Amount-ს .amount-ზე */}
              {bids.map((bid, index) => (
                <tr key={index} className="relative h-6">
                  <td className="text-green-400 px-2">{bid.price.toFixed(2)}</td>
                  <td className="text-right px-2">{bid.amount.toFixed(4)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
};