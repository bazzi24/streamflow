import { IChartApi } from "lightweight-charts";
import { useAppStore, type TimeInterval, type ChartType, type DrawingTool } from "../stores/appStore";
import { useTranslation } from "react-i18next";
import { cn } from "../lib/utils";

// ── Toolbar Button ──────────────────────────────────────────────────────────

interface ToolbarBtnProps {
  active?: boolean;
  onClick?: () => void;
  disabled?: boolean;
  children: React.ReactNode;
  title?: string;
}

function ToolbarBtn({ active, onClick, disabled, children, title }: ToolbarBtnProps) {
  return (
    <button
      onClick={onClick}
      disabled={disabled}
      title={title}
      className={cn(
        "flex items-center gap-1 rounded px-2 py-1 text-xs font-medium transition-colors",
        active
          ? "bg-blue-600 text-white"
          : "bg-gray-700 text-gray-300 hover:bg-gray-600 hover:text-white",
        disabled && "cursor-not-allowed opacity-40"
      )}
    >
      {children}
    </button>
  );
}

// ── Drawing Tool icons (simple SVG) ─────────────────────────────────────────

function TrendIcon() {
  return (
    <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
      <path d="M2 11L7 5L10 8L12 4" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
    </svg>
  );
}
function HLineIcon() {
  return (
    <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
      <line x1="2" y1="7" x2="12" y2="7" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
    </svg>
  );
}
function FibIcon() {
  return (
    <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
      <line x1="2" y1="3" x2="12" y2="3" stroke="currentColor" strokeWidth="1.2" />
      <line x1="2" y1="7" x2="12" y2="7" stroke="currentColor" strokeWidth="1.2" />
      <line x1="2" y1="11" x2="12" y2="11" stroke="currentColor" strokeWidth="1.2" />
    </svg>
  );
}
function VertIcon() {
  return (
    <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
      <line x1="7" y1="2" x2="7" y2="12" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
    </svg>
  );
}
function ChannelIcon() {
  return (
    <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
      <path d="M2 10L6 4L10 7L12 3" stroke="currentColor" strokeWidth="1.2" />
      <path d="M2 7L6 1L10 4L12 0" stroke="currentColor" strokeWidth="1.2" strokeDasharray="2 1" />
    </svg>
  );
}
function GannIcon() {
  return (
    <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
      <line x1="2" y1="12" x2="12" y2="2" stroke="currentColor" strokeWidth="1.2" />
      <line x1="2" y1="12" x2="8.5" y2="12" stroke="currentColor" strokeWidth="1.2" />
      <line x1="2" y1="12" x2="2" y2="5.5" stroke="currentColor" strokeWidth="1.2" />
    </svg>
  );
}
function SRLIcon() {
  return (
    <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
      <line x1="2" y1="7" x2="12" y2="7" stroke="currentColor" strokeWidth="1.2" />
      <line x1="2" y1="3.5" x2="12" y2="3.5" stroke="currentColor" strokeWidth="1" strokeDasharray="2 1" />
      <line x1="2" y1="10.5" x2="12" y2="10.5" stroke="currentColor" strokeWidth="1" strokeDasharray="2 1" />
    </svg>
  );
}

// ── MA icon ─────────────────────────────────────────────────────────────────

function MAIcon() {
  return (
    <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
      <polyline points="2,10 4,8 6,9 8,5 10,6 12,3" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" fill="none" />
    </svg>
  );
}

// ── Main Toolbar Component ───────────────────────────────────────────────────

interface ChartToolbarProps {
  /** Ref to the chart API (for programmatic control) */
  chartRef: React.MutableRefObject<IChartApi | null>;
  /** Callback fired when user picks a new interval */
  onIntervalChange?: (interval: TimeInterval) => void;
  /** Callback fired when chart type changes */
  onChartTypeChange?: (type: ChartType) => void;
  /** Callback fired when a drawing tool is activated */
  onDrawingToolChange?: (tool: DrawingTool | null) => void;
  /** Callback fired when an indicator is toggled */
  onIndicatorToggle?: (indicator: string) => void;
  activeIndicators?: string[];
}

export function ChartToolbar({
  chartRef,
  onIntervalChange,
  onChartTypeChange,
  onDrawingToolChange,
  onIndicatorToggle,
  activeIndicators = [],
}: ChartToolbarProps) {
  const {
    activeInterval,
    setActiveInterval,
    activeChartType,
    setActiveChartType,
    activeDrawingTool,
    setActiveDrawingTool,
    toggleIndicator,
    setActiveDrawingTool: _sa,
  } = useAppStore();
  const { t } = useTranslation();

  const intervals: { key: TimeInterval; label: string }[] = [
    { key: "1m",  label: "1m"  },
    { key: "5m",  label: "5m"  },
    { key: "15m", label: "15m" },
    { key: "30m", label: "30m" },
    { key: "1h",  label: "1h"  },
    { key: "2h",  label: "2h"  },
    { key: "4h",  label: "4h"  },
    { key: "1D",  label: "1D"  },
    { key: "1W",  label: "1W"  },
    { key: "1M",  label: "1M"  },
  ];

  const chartTypes: { key: ChartType; label: string }[] = [
    { key: "candlestick", label: "Nến" },
    { key: "line", label: "Đường" },
    { key: "area", label: "Vùng" },
    { key: "bar", label: "Thanh" },
  ];

  const drawingTools: { key: DrawingTool; label: string; Icon: React.FC }[] = [
    { key: "trend", label: "Xu hướng", Icon: TrendIcon },
    { key: "horizontal", label: "Ngang", Icon: HLineIcon },
    { key: "fib", label: "Fibonacci", Icon: FibIcon },
    { key: "vertical", label: "Dọc", Icon: VertIcon },
    { key: "channel", label: "Kênh", Icon: ChannelIcon },
    { key: "gann", label: "Gann Fan", Icon: GannIcon },
    { key: "srl", label: "SRL", Icon: SRLIcon },
  ];

  const indicators: { key: string; label: string; color: string }[] = [
    { key: "MA5", label: "MA5", color: "#f59e0b" },
    { key: "MA10", label: "MA10", color: "#3b82f6" },
    { key: "MA20", label: "MA20", color: "#8b5cf6" },
    { key: "MA50", label: "MA50", color: "#ec4899" },
    { key: "EMA", label: "EMA", color: "#06b6d4" },
    { key: "BB", label: "Bollinger", color: "#f97316" },
    { key: "MACD", label: "MACD", color: "#10b981" },
    { key: "RSI", label: "RSI", color: "#a855f7" },
  ];

  function handleInterval(iv: TimeInterval) {
    setActiveInterval(iv);
    onIntervalChange?.(iv);
  }

  function handleChartType(ct: ChartType) {
    setActiveChartType(ct);
    onChartTypeChange?.(ct);
  }

  function handleDrawingTool(tool: DrawingTool) {
    const next = activeDrawingTool === tool ? null : tool;
    setActiveDrawingTool(next);
    onDrawingToolChange?.(next);
  }

  function handleIndicator(ind: string) {
    toggleIndicator(ind);
    onIndicatorToggle?.(ind);
  }

  function handleScreenshot() {
    if (chartRef.current) {
      const canvas = chartRef.current.takeScreenshot();
      const url = canvas.toDataURL("image/png");
      const a = document.createElement("a");
      a.href = url;
      a.download = `chart-${Date.now()}.png`;
      a.click();
    }
  }

  function handleZoomIn() {
    if (chartRef.current) {
      const ts = chartRef.current.timeScale();
      ts.applyOptions({ rightOffset: ts.scrollPosition() + 5 });
    }
  }

  function handleZoomOut() {
    if (chartRef.current) {
      const ts = chartRef.current.timeScale();
      ts.applyOptions({ rightOffset: ts.scrollPosition() - 5 });
    }
  }

  return (
    <div className="flex flex-wrap items-center gap-1 border-b border-gray-700 bg-gray-800 px-3 py-2 text-xs">
      {/* ── Logo / Title ── */}
      <span className="mr-2 text-sm font-bold text-blue-400">StreamFlow Chart</span>

      {/* ── Time Intervals ── */}
      <div className="flex items-center gap-0.5 rounded bg-gray-700 p-0.5">
        {intervals.map((iv) => (
          <ToolbarBtn
            key={iv.key}
            active={activeInterval === iv.key}
            onClick={() => handleInterval(iv.key)}
            title={`Khung thời gian ${iv.key}`}
          >
            {iv.label}
          </ToolbarBtn>
        ))}
      </div>

      {/* ── Separator ── */}
      <div className="mx-1 h-5 w-px bg-gray-600" />

      {/* ── Chart Types ── */}
      <div className="flex items-center gap-0.5 rounded bg-gray-700 p-0.5">
        {chartTypes.map((ct) => (
          <ToolbarBtn
            key={ct.key}
            active={activeChartType === ct.key}
            onClick={() => handleChartType(ct.key)}
            title={`Biểu đồ ${ct.label}`}
          >
            {ct.label}
          </ToolbarBtn>
        ))}
      </div>

      {/* ── Separator ── */}
      <div className="mx-1 h-5 w-px bg-gray-600" />

      {/* ── Drawing Tools ── */}
      <div className="relative group/draw">
        <ToolbarBtn title="Công cụ vẽ">✏️ Vẽ</ToolbarBtn>
        <div className="absolute left-0 top-full z-50 mt-1 hidden min-w-max rounded border border-gray-600 bg-gray-800 p-2 shadow-lg group-hover/draw:block">
          <div className="grid grid-cols-2 gap-1">
            {drawingTools.map((dt) => {
              const Icon = dt.Icon;
              return (
                <button
                  key={dt.key}
                  onClick={() => handleDrawingTool(dt.key)}
                  className={cn(
                    "flex items-center gap-2 rounded px-3 py-1.5 text-xs transition-colors",
                    activeDrawingTool === dt.key
                      ? "bg-blue-600 text-white"
                      : "bg-gray-700 text-gray-300 hover:bg-gray-600 hover:text-white"
                  )}
                  title={dt.label}
                >
                  <Icon />
                  {dt.label}
                </button>
              );
            })}
          </div>
          {activeDrawingTool && (
            <div className="mt-2 border-t border-gray-600 pt-2">
              <p className="mb-1 text-xs text-gray-400">
                {t("chartToolbar.selected")}: <span className="text-white">{drawingTools.find(d => d.key === activeDrawingTool)?.label}</span>
              </p>
              <p className="text-xs text-gray-500">Click trên biểu đồ để vẽ. Click phải để xóa.</p>
            </div>
          )}
        </div>
      </div>

      {/* ── Separator ── */}
      <div className="mx-1 h-5 w-px bg-gray-600" />

      {/* ── Indicators ── */}
      <div className="relative group/ind">
        <ToolbarBtn title="Chỉ báo kỹ thuật">
          <MAIcon />
          Chỉ báo
        </ToolbarBtn>
        <div className="absolute left-0 top-full z-50 mt-1 hidden min-w-max rounded border border-gray-600 bg-gray-800 p-2 shadow-lg group-hover/ind:block">
          <div className="grid grid-cols-2 gap-1">
            {indicators.map((ind) => (
              <button
                key={ind.key}
                onClick={() => handleIndicator(ind.key)}
                className={cn(
                  "flex items-center gap-2 rounded px-3 py-1.5 text-xs transition-colors",
                  activeIndicators.includes(ind.key)
                    ? "text-white"
                    : "text-gray-300 hover:bg-gray-700 hover:text-white"
                )}
                style={activeIndicators.includes(ind.key) ? { backgroundColor: ind.color + "33", border: `1px solid ${ind.color}` } : {}}
                title={ind.label}
              >
                <span
                  className="h-3 w-3 rounded-full"
                  style={{ backgroundColor: ind.color }}
                />
                {ind.label}
              </button>
            ))}
          </div>
        </div>
      </div>

      {/* ── Separator ── */}
      <div className="mx-1 h-5 w-px bg-gray-600" />

      {/* ── Zoom ── */}
      <ToolbarBtn onClick={handleZoomIn} title="Phóng to">🔍+</ToolbarBtn>
      <ToolbarBtn onClick={handleZoomOut} title="Thu nhỏ">🔍−</ToolbarBtn>

      {/* ── Screenshot ── */}
      <ToolbarBtn onClick={handleScreenshot} title="Chụp ảnh biểu đồ">📷</ToolbarBtn>

      {/* ── Clear drawings ── */}
      <ToolbarBtn
        onClick={() => setActiveDrawingTool(null)}
        disabled={!activeDrawingTool}
        title="Xóa công cụ vẽ"
      >
        🗑️ Xóa vẽ
      </ToolbarBtn>
    </div>
  );
}
