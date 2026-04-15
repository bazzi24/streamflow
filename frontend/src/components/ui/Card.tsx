import { cn } from "../../lib/utils";
import type { ReactNode, ButtonHTMLAttributes } from "react";

export function Card({ children, className }: { children: ReactNode; className?: string }) {
  return (
    <div className={cn("rounded-lg border border-gray-700 bg-gray-800 p-4", className)}>
      {children}
    </div>
  );
}

export function Button({
  children,
  variant = "default",
  className,
  ...props
}: {
  children: ReactNode;
  variant?: "default" | "ghost" | "outline";
  className?: string;
} & ButtonHTMLAttributes<HTMLButtonElement>) {
  const base =
    "inline-flex items-center justify-center rounded px-3 py-1.5 text-sm font-medium transition-colors focus:outline-none focus:ring-2 focus:ring-blue-500 disabled:opacity-50";
  const variants = {
    default: "bg-blue-600 text-white hover:bg-blue-700",
    ghost: "text-gray-300 hover:bg-gray-700",
    outline: "border border-gray-600 text-gray-300 hover:bg-gray-700",
  };
  return (
    <button className={cn(base, variants[variant], className)} {...props}>
      {children}
    </button>
  );
}

export function Badge({
  children,
  variant = "default",
  className,
}: {
  children: ReactNode;
  variant?: "default" | "green" | "red";
  className?: string;
}) {
  const variants = {
    default: "bg-gray-700 text-gray-300",
    green: "bg-green-900 text-green-300",
    red: "bg-red-900 text-red-300",
  };
  return (
    <span className={cn("inline-flex items-center rounded px-2 py-0.5 text-xs font-medium", variants[variant], className)}>
      {children}
    </span>
  );
}
