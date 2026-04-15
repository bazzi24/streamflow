/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{js,ts,jsx,tsx}"],
  darkMode: "class",
  theme: {
    extend: {
      colors: {
        // Vietnamese stock market palette
        green: {
          500: "#16a34a",
          600: "#15803d",
        },
        red: {
          500: "#dc2626",
          600: "#b91c1c",
        },
        gold: {
          400: "#f59e0b",
          500: "#d97706",
        },
        // Terminal / Cyber-Researcher theme
        terminal: {
          bg: "#0a0e17",
          panel: "#0f1623",
          hover: "#141c2b",
          elevated: "#1a2333",
          cyan: "#00d4ff",
          green: "#00e676",
          red: "#ff3d57",
          purple: "#b388ff",
          gold: "#ffd54f",
          text: "#e2e8f0",
          muted: "#4b5563",
          border: "rgba(0,212,255,0.08)",
          "border-active": "rgba(0,212,255,0.35)",
          "border-hover": "rgba(0,212,255,0.18)",
        },
      },
      fontFamily: {
        mono: ["JetBrains Mono", "Fira Code", "Cascadia Code", "monospace"],
        data: ["IBM Plex Mono", "JetBrains Mono", "monospace"],
      },
      boxShadow: {
        glow: "0 0 12px rgba(0,212,255,0.4)",
        "glow-sm": "0 0 6px rgba(0,212,255,0.25)",
        "glow-green": "0 0 8px rgba(0,230,118,0.35)",
        "glow-red": "0 0 8px rgba(255,61,87,0.35)",
      },
    },
  },
  plugins: [],
};
