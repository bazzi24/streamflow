import { Link, useLocation } from "react-router-dom";
import { useState } from "react";
import { useAppStore } from "../stores/appStore";
import styles from "./Header.module.css";

const NAV_ITEMS = [
  { to: "/", label: "Trang chủ", icon: "🏠" },
  { to: "/markets", label: "Thị trường", icon: "📊", hasDropdown: true },
  { to: "/favorites", label: "Yêu thích", icon: "⭐" },
];

const MARKET_SEGMENTS = [
  { to: "/markets", label: "Tất cả" },
  { to: "/markets/HOSE", label: "HOSE" },
  { to: "/markets/HNX", label: "HNX" },
  { to: "/markets/VN30", label: "VN30" },
  { to: "/markets/HNX30", label: "HNX30" },
  { to: "/markets/UPCOM", label: "UPCOM" },
];

export function Header() {
  const { user, clearAuth, toggleTheme } = useAppStore();
  const location = useLocation();
  const [marketsOpen, setMarketsOpen] = useState(false);

  return (
    <header className={styles.header}>
      <div className={styles.left}>
        <Link to="/" className={styles.logo}>StreamFlow</Link>
        <nav className={styles.nav}>
          {NAV_ITEMS.map((item) => {
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
                      {MARKET_SEGMENTS.map((m) => (
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
        <button className={styles.navLink} title="Toggle theme" onClick={toggleTheme} style={{ padding: "6px" }}>
          ☀️
        </button>
        {user ? (
          <>
            <div className={styles.userBadge}>
              <span>{user.username}</span>
            </div>
            <button className={styles.logoutBtn} onClick={clearAuth}>
              Đăng xuất
            </button>
          </>
        ) : (
          <Link to="/login" className={styles.loginBtn}>
            Đăng nhập
          </Link>
        )}
      </div>
    </header>
  );
}
