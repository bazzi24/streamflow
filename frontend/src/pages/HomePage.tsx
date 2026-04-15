import { useNavigate } from "react-router-dom";
import { useTranslation } from "react-i18next";
import { useAppStore } from "../stores/appStore";
import { marketApi } from "../api/stockApi";
import { IndexTickerTape } from "../components/IndexTickerTape";
import { TopMovers } from "../components/TopMovers";
import { NewsSection } from "../components/NewsCard";
import { MarketDepthChart } from "../components/MarketDepthChart";
import { formatPrice } from "../lib/utils";
import styles from "./HomePage.module.css";
import { useQuery } from "@tanstack/react-query";

function MarketIndices() {
  const { data } = useQuery({
    queryKey: ["market-overview"],
    queryFn: () => marketApi.getOverview().then((r) => r.data),
    refetchInterval: 30_000,
  });
  return (
    <div className={styles.indicesRow}>
      {(data?.indices ?? []).slice(0, 5).map((idx) => {
        const up = idx.ratio_change >= 0;
        return (
          <div key={idx.index_id} className={styles.indexCard}>
            <div className={styles.indexName}>{idx.index_name ?? idx.index_id}</div>
            <div className={styles.indexValue}>
              {formatPrice(idx.index_value)}
            </div>
            <div
              className={`${styles.indexChange} ${up ? styles.up : styles.down}`}
            >
              {up ? "▲" : "▼"}{" "}
              {idx.ratio_change?.toFixed(2)}%
            </div>
          </div>
        );
      })}
    </div>
  );
}

function Header() {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const { user, clearAuth, toggleTheme } = useAppStore();
  return (
    <header className={styles.header}>
      <div className={styles.headerLeft}>
        <div className={styles.logoWrap}>
          <div className={styles.logo}>SF</div>
          <span className={styles.logoText}>StreamFlow</span>
        </div>
        <nav className={styles.nav}>
          <a className={styles.navLink} href="/">{t("nav.home")}</a>
          <a className={styles.navLink} href="/markets">{t("nav.markets")}</a>
        </nav>
      </div>
      <div className={styles.headerRight}>
        <button className={styles.iconBtn} title="Toggle theme" onClick={toggleTheme}>
          ☀️
        </button>
        {user ? (
          <>
            <button className={styles.watchlistBtn} onClick={() => navigate("/favorites")}>
              {t("home.watchlist")}
            </button>
            <div className={styles.userChip}>
              <div className={styles.userAvatar}>
                {user.username.charAt(0).toUpperCase()}
              </div>
              <span>{user.username}</span>
              <button className={styles.logoutBtn} onClick={clearAuth}>
                {t("auth.logout")}
              </button>
            </div>
          </>
        ) : (
          <button
            className={styles.loginBtn}
            onClick={() => navigate("/login")}
          >
            {t("auth.login")}
          </button>
        )}
      </div>
    </header>
  );
}

export function HomePage() {
  const { t } = useTranslation();
  return (
    <div className={styles.page}>
      <Header />
      <IndexTickerTape />

      <main className={styles.main}>
        {/* Market Indices */}
        <section className={styles.section}>
          <h2 className={styles.sectionTitle}>
            <span className={styles.sectionDot} />
            {t("home.indices")}
          </h2>
          <MarketIndices />
        </section>

        {/* Two-col layout */}
        <div className={styles.twoCol}>
          <section className={styles.colWide}>
            <h2 className={styles.sectionTitle}>
              <span className={styles.sectionDot} />
              {t("home.marketDepth")}
            </h2>
            <MarketDepthChart symbol="VND" />
          </section>

          <section className={styles.colNarrow}>
            <h2 className={styles.sectionTitle}>
              <span className={styles.sectionDot} />
              {t("home.topMovers")}
            </h2>
            <TopMovers />
          </section>
        </div>

        {/* News */}
        <section className={styles.section}>
          <h2 className={styles.sectionTitle}>
            <span className={styles.sectionDot} />
            {t("home.news")}
          </h2>
          <NewsSection />
        </section>
      </main>

      <footer className={styles.footer}>
        <span>{t("home.footer")}</span>
        <span>·</span>
        <span>Real-time data from SSI</span>
      </footer>
    </div>
  );
}
