import { Link, useLocation } from "react-router-dom";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { useAppStore } from "../stores/appStore";
import styles from "./Header.module.css";

const NAV_ITEMS = (t: (key: string) => string) => [
  { to: "/", label: t("nav.home"), icon: "🏠" },
  { to: "/markets", label: t("nav.markets"), icon: "📊", hasDropdown: true },
  { to: "/favorites", label: t("nav.favorites"), icon: "⭐" },
];

const MARKET_SEGMENTS = (t: (key: string) => string) => [
  { to: "/markets", label: t("tab.all") },
  { to: "/markets/HOSE", label: "HOSE" },
  { to: "/markets/HNX", label: "HNX" },
  { to: "/markets/VN30", label: "VN30" },
  { to: "/markets/HNX30", label: "HNX30" },
  { to: "/markets/UPCOM", label: "UPCOM" },
];

export function Header() {
  const { user, clearAuth, toggleTheme, language, setLanguage } = useAppStore();
  const { t } = useTranslation();
  const location = useLocation();
  const [marketsOpen, setMarketsOpen] = useState(false);

  const navItems = NAV_ITEMS(t);
  const marketSegments = MARKET_SEGMENTS(t);

  function toggleLanguage() {
    setLanguage(language === "vi" ? "en" : "vi");
  }

  return (
    <header className={styles.header}>
      <div className={styles.left}>
        <Link to="/" className={styles.logo}>StreamFlow</Link>
        <nav className={styles.nav}>
          {navItems.map((item) => {
            if (item.to === "/markets") {
              return (
                <div
                  key={item.to}
                  className={styles.marketsWrapper}
                  onMouseEnter={() => setMarketsOpen(true)}
                  onMouseLeave={() => setMarketsOpen(false)}
                >
                  <Link
                    to={item.to}
                    className={`${styles.navLink} ${
                      location.pathname.startsWith("/markets") ? styles.active : ""
                    }`}
                  >
                    <span>{item.icon}</span>
                    {item.label}
                    <span style={{ fontSize: 9 }}>▾</span>
                  </Link>
                  {marketsOpen && (
                    <div className={styles.marketsDropdown}>
                      {marketSegments.map((m) => (
                        <Link
                          key={m.to}
                          to={m.to}
                          className={`${styles.dropItem} ${
                            location.pathname === m.to ? styles.active : ""
                          }`}
                        >
                          {m.label}
                        </Link>
                      ))}
                    </div>
                  )}
                </div>
              );
            }
            return (
              <Link
                key={item.to}
                to={item.to}
                className={`${styles.navLink} ${
                  location.pathname === item.to ? styles.active : ""
                }`}
              >
                <span>{item.icon}</span>
                {item.label}
              </Link>
            );
          })}
        </nav>
      </div>

      <div className={styles.right}>
        {/* Language toggle — left of theme toggle */}
        <button
          className={styles.navLink}
          title={t("header.support")}
          onClick={toggleLanguage}
          style={{ padding: "4px 8px", fontSize: 11, fontWeight: 700, minWidth: 36 }}
        >
          {language === "vi" ? "EN" : "VI"}
        </button>
        <button className={styles.navLink} title="Toggle theme" onClick={toggleTheme} style={{ padding: "6px" }}>
          ☀️
        </button>
        {user ? (
          <>
            <div className={styles.userBadge}>
              <span>{user.username}</span>
            </div>
            <button className={styles.logoutBtn} onClick={clearAuth}>
              {t("auth.logout")}
            </button>
          </>
        ) : (
          <Link to="/login" className={styles.loginBtn}>
            {t("auth.login")}
          </Link>
        )}
      </div>
    </header>
  );
}
