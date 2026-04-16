import { clsx, type ClassValue } from "clsx";
import { twMerge } from "tailwind-merge";

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

/**
 * Price comparison result used for color decisions.
 * Prices are integers (e.g. 7110 = 7.110 VND) — pass raw API values.
 */
export type PriceCompare = "above" | "at_ceiling" | "at_ref" | "at_floor" | "below";

/**
 * Compare a price to reference / ceiling / floor — all raw integer API values.
 * Priority: ceiling → ref → floor → below/above.
 */
export function comparePrice(
  price: number | undefined | null,
  ref: number | undefined | null,
  ceiling: number | undefined | null,
  floor: number | undefined | null
): PriceCompare {
  if (price == null) return "below";
  if (ref == null) return "below";
  if (ceiling != null && price >= ceiling) return "at_ceiling";
  if (floor != null && price <= floor) return "at_floor";
  if (price > ref) return "above";
  if (price === ref) return "at_ref";
  return "below";
}

/**
 * Color class for price text, based on comparePrice result.
 * Returns a CSS class name string for use with className.
 */
export function priceColorClass(result: PriceCompare): string {
  switch (result) {
    case "at_ceiling": return "text-purple-400"; // Trần — purple
    case "above":      return "text-green-400";  // above ref — green
    case "at_ref":     return "text-yellow-400"; // = ref price — yellow
    case "at_floor":   return "text-blue-400";  // Sàn — blue
    case "below":      return "text-red-400";   // below ref — red
  }
}

/**
 * Maps a PriceCompare result to a CSS color value (var(--accent-*))
 * for use with inline style={{ color: ... }}.
 */
export function priceColorByCompare(result: PriceCompare): string {
  switch (result) {
    case "at_ceiling": return "var(--accent-purple)";
    case "above":      return "var(--accent-green)";
    case "at_ref":     return "var(--accent-yellow)";
    case "at_floor":   return "var(--accent-blue)";
    case "below":      return "var(--accent-red)";
  }
}

/**
 * Format a stock price in VND.
 * Prices are stored as integers * 1000 in the DB (e.g. 17950 = 17.950 VND).
 * Divide by 1000, format with period decimal and comma thousands.
 * e.g. 17950 → "17.95" | 16800 → "16.80"
 */
export function formatPrice(value: number | undefined | null): string {
  if (value == null || isNaN(value)) return "—";
  if (value === 0) return "—";
  const divided = value / 1000;
  // Period (.) for decimal, comma (,) for thousands — custom formatting
  const parts = divided.toFixed(2).split(".");
  const intPart = parts[0].replace(/\B(?=(\d{3})+(?!\d))/g, ",");
  return `${intPart}.${parts[1]}`;
}

/**
 * Format volume / big numbers in full with comma thousands.
 * e.g. 1961000 → "1,961,000"
 */
export function formatVolume(value: number | undefined | null): string {
  if (value == null || isNaN(value)) return "—";
  if (value === 0) return "—";
  return value.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

export function formatChange(value: number | undefined | null): string {
  if (value == null || isNaN(value)) return "—";
  const sign = value >= 0 ? "+" : "";
  return `${sign}${value.toFixed(2)}`;
}

export function priceColor(value: number | undefined | null): string {
  if (value == null || isNaN(value)) return "text-gray-400";
  if (value > 0) return "text-green-400";
  if (value < 0) return "text-red-400";
  return "text-gray-400";
}

export function pctColor(pct: number | undefined | null): string {
  if (pct == null || isNaN(pct)) return "text-gray-400";
  if (pct > 0) return "text-green-400";
  if (pct < 0) return "text-red-400";
  return "text-gray-400";
}

export function formatIndexValue(value: number | undefined | null): string {
  if (value == null || isNaN(value)) return "—";
  if (value === 0) return "—";
  
  // Không chia cho 1000, chỉ format định dạng số
  return value.toLocaleString("vi-VN", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });
}
