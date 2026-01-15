import hashlib
import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd
import akshare as ak

DB_PATH = Path("data") / "sina_news.db"


def ensure_table(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS sina_news (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            time TEXT,
            content TEXT,
            content_hash TEXT UNIQUE,
            source TEXT,
            fetched_at TEXT
        )
        """
    )
    # 常用索引（可选，但建议）
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sina_news_time ON sina_news(time)")
    conn.commit()


def make_hash(time_str: str, content: str) -> str:
    raw = f"{time_str}||{content}".encode("utf-8")
    return hashlib.sha1(raw).hexdigest()


def main():
    # 1) 抓取数据（最近 20 条）
    df = ak.stock_info_global_sina()

    # 2) 选字段并标准化
    # akshare 返回列名：时间、内容
    df = df[["时间", "内容"]].copy()

    # 3) 处理时间：转成统一格式字符串（SQLite友好）
    df["时间"] = pd.to_datetime(df["时间"], errors="coerce")
    df["时间"] = df["时间"].dt.strftime("%Y-%m-%d %H:%M:%S")

    # 4) 生成去重哈希（time + content）
    df["content_hash"] = df.apply(lambda r: make_hash(str(r["时间"]), str(r["内容"])), axis=1)

    # 5) 增加元信息
    fetched_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    df["source"] = "sina_finance_7x24"
    df["fetched_at"] = fetched_at

    # 6) 确保 data/ 存在
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    # 7) 写入 SQLite（content_hash UNIQUE 去重）
    conn = sqlite3.connect(str(DB_PATH))
    ensure_table(conn)
    cur = conn.cursor()

    insert_sql = """
    INSERT OR IGNORE INTO sina_news
    (time, content, content_hash, source, fetched_at)
    VALUES (?, ?, ?, ?, ?)
    """

    rows = df[["时间", "内容", "content_hash", "source", "fetched_at"]].values.tolist()
    cur.executemany(insert_sql, rows)
    conn.commit()

    total = pd.read_sql("SELECT COUNT(*) AS c FROM sina_news", conn).iloc[0]["c"]
    conn.close()

    print(f"Fetched {len(rows)} rows; DB total rows now: {total}; saved at: {DB_PATH}")


if __name__ == "__main__":
    main()
