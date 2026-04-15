/**
 * Pure indicator computation utilities for lightweight-charts.
 * All functions take OHLCV bar arrays and return chart-ready data.
 */

import type { OHLCVBar } from "../api/stockApi";
import type { Time } from "lightweight-charts";

// ── Helpers ──────────────────────────────────────────────────────────────────

function toTime(ts: number): Time {
  return (ts / 1000) as Time;
}

// ── SMA (Simple Moving Average) ─────────────────────────────────────────────

export function computeSMA(bars: OHLCVBar[], period: number): Array<{ time: Time; value: number }> {
  if (bars.length < period) return [];
  const result: Array<{ time: Time; value: number }> = [];
  for (let i = period - 1; i < bars.length; i++) {
    let sum = 0;
    for (let j = 0; j < period; j++) {
      sum += bars[i - j].close;
    }
    result.push({ time: toTime(bars[i].timestamp), value: sum / period });
  }
  return result;
}

// ── EMA (Exponential Moving Average) ─────────────────────────────────────────

export function computeEMA(
  bars: OHLCVBar[],
  period: number
): Array<{ time: Time; value: number }> {
  if (bars.length < period) return [];
  const multiplier = 2 / (period + 1);
  const result: Array<{ time: Time; value: number }> = [];

  // Seed with SMA
  let sum = 0;
  for (let i = 0; i < period; i++) sum += bars[i].close;
  let ema = sum / period;
  result.push({ time: toTime(bars[period - 1].timestamp), value: ema });

  for (let i = period; i < bars.length; i++) {
    ema = (bars[i].close - ema) * multiplier + ema;
    result.push({ time: toTime(bars[i].timestamp), value: ema });
  }
  return result;
}

// ── Bollinger Bands ──────────────────────────────────────────────────────────

export interface BollingerResult {
  upper: Array<{ time: Time; value: number }>;
  middle: Array<{ time: Time; value: number }>;
  lower: Array<{ time: Time; value: number }>;
  bandwidth: Array<{ time: Time; value: number }>; // for pane
}

export function computeBollinger(
  bars: OHLCVBar[],
  period = 20,
  stdMultiplier = 2
): BollingerResult {
  if (bars.length < period) return { upper: [], middle: [], lower: [], bandwidth: [] };

  const middle = computeSMA(bars, period);
  const upper: Array<{ time: Time; value: number }> = [];
  const lower: Array<{ time: Time; value: number }> = [];
  const bandwidth: Array<{ time: Time; value: number }> = [];

  for (let i = period - 1; i < bars.length; i++) {
    let sum = 0;
    for (let j = 0; j < period; j++) {
      sum += bars[i - j].close;
    }
    const sma = sum / period;
    let sqSum = 0;
    for (let j = 0; j < period; j++) {
      const d = bars[i - j].close - sma;
      sqSum += d * d;
    }
    const std = Math.sqrt(sqSum / period);
    const t = middle[i - (period - 1)].time;
    const u = sma + stdMultiplier * std;
    const l = sma - stdMultiplier * std;
    upper.push({ time: t, value: u });
    lower.push({ time: t, value: l });
    // Bandwidth % = (upper - lower) / middle * 100
    bandwidth.push({ time: t, value: ((u - l) / sma) * 100 });
  }

  return { upper, middle, lower, bandwidth };
}

// ── MACD ─────────────────────────────────────────────────────────────────────

export interface MACDResult {
  macdLine: Array<{ time: Time; value: number }>;
  signalLine: Array<{ time: Time; value: number }>;
  histogram: Array<{ time: Time; value: number; color: string }>;
}

export function computeMACD(
  bars: OHLCVBar[],
  fastPeriod = 12,
  slowPeriod = 26,
  signalPeriod = 9
): MACDResult {
  const fastEMA = computeEMA(bars, fastPeriod);
  const slowEMA = computeEMA(bars, slowPeriod);

  // Align fast & slow — both start from index slowPeriod-1
  const fastStart = fastPeriod - 1;
  const diff: Array<{ time: Time; value: number }> = [];

  for (let i = 0; i < slowEMA.length; i++) {
    const fastBarIdx = fastStart + i;
    if (fastBarIdx < fastEMA.length) {
      diff.push({
        time: slowEMA[i].time,
        value: fastEMA[fastBarIdx].value - slowEMA[i].value,
      });
    }
  }

  // EMA of diff = signal line
  if (diff.length < signalPeriod) return { macdLine: [], signalLine: [], histogram: [] };

  const multiplier = 2 / (signalPeriod + 1);
  const signalLine: Array<{ time: Time; value: number }> = [];

  let sum = 0;
  for (let i = 0; i < signalPeriod; i++) sum += diff[i].value;
  let ema = sum / signalPeriod;
  signalLine.push({ time: diff[signalPeriod - 1].time, value: ema });

  for (let i = signalPeriod; i < diff.length; i++) {
    ema = (diff[i].value - ema) * multiplier + ema;
    signalLine.push({ time: diff[i].time, value: ema });
  }

  // Histogram = MACD - Signal
  const macdLine: Array<{ time: Time; value: number }> = [];
  const histogram: Array<{ time: Time; value: number; color: string }> = [];
  const sigStart = signalPeriod - 1;

  for (let i = 0; i < signalLine.length; i++) {
    const diffIdx = sigStart + i;
    if (diffIdx < diff.length) {
      const macdVal = diff[diffIdx].value;
      const sigVal = signalLine[i].value;
      const histVal = macdVal - sigVal;
      const t = signalLine[i].time;
      macdLine.push({ time: t, value: macdVal });
      histogram.push({
        time: t,
        value: histVal,
        color: histVal >= 0 ? "rgba(5,150,105,0.7)" : "rgba(220,38,38,0.7)",
      });
    }
  }

  return { macdLine, signalLine, histogram };
}

// ── RSI (Relative Strength Index) ───────────────────────────────────────────

export interface RSIResult {
  rsiLine: Array<{ time: Time; value: number }>;
  overbought: Array<{ time: Time; value: number }>;
  oversold: Array<{ time: Time; value: number }>;
}

export function computeRSI(
  bars: OHLCVBar[],
  period = 14
): RSIResult {
  if (bars.length < period + 1) return { rsiLine: [], overbought: [], oversold: [] };

  const gains: number[] = [];
  const losses: number[] = [];

  for (let i = 1; i < bars.length; i++) {
    const change = bars[i].close - bars[i - 1].close;
    gains.push(change > 0 ? change : 0);
    losses.push(change < 0 ? -change : 0);
  }

  // Seed averages
  let avgGain = gains.slice(0, period).reduce((a, b) => a + b, 0) / period;
  let avgLoss = losses.slice(0, period).reduce((a, b) => a + b, 0) / period;

  const rsiLine: Array<{ time: Time; value: number }> = [];
  const overbought: Array<{ time: Time; value: number }> = [];
  const oversold: Array<{ time: Time; value: number }> = [];

  // RSI at bar = period (0-indexed gains)
  const t0 = toTime(bars[period].timestamp);
  const rs0 = avgLoss === 0 ? 100 : avgGain / avgLoss;
  const rsi0 = 100 - 100 / (1 + rs0);
  rsiLine.push({ time: t0, value: rsi0 });
  overbought.push({ time: t0, value: 70 });
  oversold.push({ time: t0, value: 30 });

  for (let i = period; i < gains.length; i++) {
    avgGain = (avgGain * (period - 1) + gains[i]) / period;
    avgLoss = (avgLoss * (period - 1) + losses[i]) / period;
    const rs = avgLoss === 0 ? 100 : avgGain / avgLoss;
    const rsi = 100 - 100 / (1 + rs);
    const t = toTime(bars[i + 1].timestamp);
    rsiLine.push({ time: t, value: rsi });
    overbought.push({ time: t, value: 70 });
    oversold.push({ time: t, value: 30 });
  }

  return { rsiLine, overbought, oversold };
}
