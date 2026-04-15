import styles from "./NewsCard.module.css";

const NEWS = [
  {
    category: "Market",
    time: "Hôm nay, 09:30",
    title: "VN-Index bứt phá ngưỡng 1.350 điểm, thanh khoản cải thiện rõ rệt",
  },
  {
    category: "Stock",
    time: "Hôm nay, 08:15",
    title: "VCB công bố lợi nhuận quý 1 tăng trưởng 22% YoY",
  },
  {
    category: "Foreign",
    time: "Hôm qua, 16:45",
    title: "Khối ngoại mua ròng 1.850 tỷ đồng, tập trung VCB, CTG, FPT",
  },
  {
    category: "Sector",
    time: "Hôm qua, 14:20",
    title: "Nhóm ngân hàng dẫn sóng phiên tăng điểm cuối ngày",
  },
  {
    category: "IPO",
    time: "19/03/2026",
    title: "Hai công ty công bố IPO thành công trên HOSE với mức giá cao hơn dự kiến",
  },
  {
    category: "Tech",
    time: "19/03/2026",
    title: "FPT ký hợp đồng chuyển giao công nghệ trị giá 85 triệu USD tại Nhật Bản",
  },
];

export function NewsSection() {
  return (
    <div className={styles.grid}>
      {NEWS.map((n, i) => (
        <div key={i} className={styles.card} style={{ animationDelay: `${i * 0.05}s` }}>
          <div className={styles.meta}>
            <span className={styles.category}>{n.category}</span>
            <span className={styles.time}>{n.time}</span>
          </div>
          <p className={styles.title}>{n.title}</p>
        </div>
      ))}
    </div>
  );
}
