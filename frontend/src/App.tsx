import { BrowserRouter, Routes, Route, Navigate, useParams, useNavigate } from "react-router-dom";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { useEffect } from "react";
import { DashboardPage } from "./pages/Dashboard/DashboardPage";
import { PriceBoardPage } from "./pages/PriceBoard/PriceBoardPage";
import { ChartModal } from "./pages/ChartPageV2";
import { FavoritesPage } from "./pages/FavoritesPage";
import { MarketsPage } from "./pages/MarketsPage";
import { LoginPage } from "./pages/LoginPage";
import { useAppStore } from "./stores/appStore";

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 1,
      refetchOnWindowFocus: false,
    },
  },
});

/** Routes that require authentication */
function ProtectedRoute({ children }: { children: JSX.Element }) {
  const token = useAppStore((s) => s.token);
  if (!token) return <Navigate to="/login" replace />;
  return children;
}

/** Public route — redirects to / if already logged in */
function PublicRoute({ children }: { children: JSX.Element }) {
  const token = useAppStore((s) => s.token);
  if (token) return <Navigate to="/" replace />;
  return children;
}

/** Chart page as standalone route (URL /chart/:symbol) */
function ChartPageRoute() {
  const { symbol } = useParams<{ symbol: string }>();
  const navigate = useNavigate();
  if (!symbol) return <Navigate to="/" replace />;
  return <ChartModal symbol={symbol} onClose={() => navigate("/")} />;
}

export default function App() {
  const theme = useAppStore((s) => s.theme);

  useEffect(() => {
    document.documentElement.setAttribute("data-theme", theme);
  }, [theme]);

  return (
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <Routes>
          {/* ── SSI iBoard Price Board (default landing) ─────────────── */}
          <Route path="/" element={<PriceBoardPage />} />

          {/* ── Secondary pages ──────────────────────────────────────────── */}
          <Route path="/login" element={<PublicRoute><LoginPage /></PublicRoute>} />
          <Route path="/favorites" element={<ProtectedRoute><FavoritesPage /></ProtectedRoute>} />
          <Route path="/markets" element={<MarketsPage />} />
          <Route path="/markets/:segment" element={<MarketsPage />} />

          {/* ── Chart page (standalone route) ────────────────────────── */}
          <Route path="/chart/:symbol" element={<ChartPageRoute />} />

          {/* ── Legacy chart dashboard ─────────────────────────────── */}
          <Route path="/dashboard" element={<DashboardPage />} />

          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
      </BrowserRouter>
    </QueryClientProvider>
  );
}
