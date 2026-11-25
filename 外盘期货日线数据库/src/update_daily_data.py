"""daily_data 增量日更：双源自动降级（Yahoo 主 / 东财 备）-> Supabase public.daily_data

设计背景：
  - 云端 GitHub Actions 是海外 IP：Yahoo Finance 可访问，东财常 Connection closed
  - 本地国内 IP：东财可访问，Yahoo 被 403/限流
  → 脚本按序尝试两个源，任一成功即写库，本地云端都能跑

源映射（统一写成同一 contract_code，两源数据幂等互补）：
  大豆 103.ZS00Y = Yahoo ZS=F = 东财 103.ZS00Y
  豆油 103.ZL00Y = Yahoo ZL=F = 东财 103.ZL00Y
  豆粕 103.ZM00Y = Yahoo ZM=F = 东财 103.ZM00Y
  玉米 103.ZC00Y = Yahoo ZC=F = 东财 103.ZC00Y
  小麦 103.ZW00Y = Yahoo ZW=F = 东财 103.ZW00Y

其他：
  - 增量：默认拉最近 LOOKBACK_DAYS 天（默认 10）
  - 幂等：唯一键 (contract_code, trade_date) 冲突时 UPDATE 行情字段
  - updated_at 由库内触发器自动更新
  - DATABASE_URL 从环境变量读（云端走 Secrets），不落盘
"""

import os
import sys
import time
import logging
from datetime import date, timedelta

import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("daily_data_daily")

PRODUCTS = {
    "大豆": ("103.ZS00Y", "ZS=F", "CBOT", "ZS"),
    "豆油": ("103.ZL00Y", "ZL=F", "CBOT", "ZL"),
    "豆粕": ("103.ZM00Y", "ZM=F", "CBOT", "ZM"),
    "玉米": ("103.ZC00Y", "ZC=F", "CBOT", "ZC"),
    "小麦": ("103.ZW00Y", "ZW=F", "CBOT", "ZW"),
}
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "10"))
ALL_COLS = ["contract_code", "contract_type", "exchange", "exchange_code",
            "product", "trade_date", "pre_close", "open", "high", "low",
            "close", "change1", "vol"]
FIELDS2 = "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61"
EM_HOSTS = [
    "https://push2his.eastmoney.com/api/qt/stock/kline/get",
    "https://push2his-ali.eastmoney.com/api/qt/stock/kline/get",
]


def r2(x):
    return None if x in (None, "") else f"{float(x):.2f}"


def make_row(contract_code, product, ex, ec, d, o, h, l, c, prev, vol):
    chg = f"{float(c) - float(prev):.2f}" if prev is not None else None
    return (contract_code, "continuous", ex, ec, product, str(d), prev,
            r2(o), r2(h), r2(l), r2(c), chg,
            str(int(float(vol))) if vol not in (None, "") else None)


def fetch_yahoo(start_d, end_d):
    """主源：Yahoo Finance（yfinance）。云端海外 IP 可用。"""
    try:
        import yfinance as yf
    except ImportError:
        log.warning("yfinance 未安装，跳过 Yahoo 源")
        return []
    syms = [v[1] for v in PRODUCTS.values()]
    try:
        df = yf.download(syms, start=start_d.isoformat(),
                         end=(end_d + timedelta(days=1)).isoformat(),
                         auto_adjust=False, progress=False, threads=False)
    except Exception as exc:
        log.warning("Yahoo 下载失败: %s", exc)
        return []
    if df is None or df.empty:
        log.warning("Yahoo 返回空")
        return []
    import pandas as pd
    ticker_level = None
    if isinstance(df.columns, pd.MultiIndex):
        # yfinance 多标的结构是 (Price, Ticker)：ticker 在 level=1
        for lvl in (1, 0):
            if ysym in df.columns.get_level_values(lvl):
                ticker_level = lvl
                break
    rows = []
    for product, (code, ysym, ex, ec) in PRODUCTS.items():
        try:
            if ticker_level is None:
                sub = df.copy()
            else:
                sub = df.xs(ysym, axis=1, level=ticker_level).copy()
                # xs 后若剩单列(Ticker 在 level=0 的情形)，升维成单行字段表
                if not isinstance(sub.columns, pd.MultiIndex) and len(sub.columns) == 1:
                    sub = sub.iloc[:, 0].to_frame()
            for col in ("Open", "High", "Low", "Close", "Volume"):
                if col not in sub.columns:
                    cand = [c for c in sub.columns if str(c).lower() == col.lower()]
                    if cand:
                        sub = sub.rename(columns={cand[0]: col})
            sub = sub.dropna(subset=["Close"])
        except Exception as exc:
            log.warning("%s Yahoo 解析失败: %s", product, exc)
            continue
        prev = None
        for d, r in sub.iterrows():
            rows.append(make_row(code, product, ex, ec,
                                 d.date() if hasattr(d, "date") else d,
                                 r["Open"], r["High"], r["Low"], r["Close"],
                                 prev, r.get("Volume")))
            prev = r2(r["Close"])
        log.info("Yahoo %s rows=%d", product, len(sub))
    return rows


def fetch_eastmoney(start_d, end_d):
    """备源：东财（curl_cffi）。国内 IP 可用。"""
    from curl_cffi import requests as cr
    beg, end = start_d.strftime("%Y%m%d"), end_d.strftime("%Y%m%d")
    rows = []
    for product, (code, ysym, ex, ec) in PRODUCTS.items():
        secid = code
        params = {"secid": secid, "fields1": "f1,f2,f3,f4,f5", "fields2": FIELDS2,
                  "klt": "101", "fqt": "0", "beg": beg, "end": end}
        data, err = None, None
        for host in EM_HOSTS:
            for attempt in range(2):
                try:
                    resp = cr.get(host, params=params, impersonate="chrome", timeout=20)
                    resp.raise_for_status()
                    j = resp.json()
                    if j.get("data") and j["data"].get("klines"):
                        data = j["data"]
                        break
                except Exception as exc:
                    err = exc
                    time.sleep(1)
            if data is not None:
                break
        if data is None:
            log.warning("东财 %s 失败: %s", product, err)
            continue
        prev = None
        for line in data["klines"]:
            f = dict(zip(FIELDS2.split(","), line.split(",")))
            rows.append(make_row(code, product, ex, ec, f["f51"],
                                 f["f52"], f["f54"], f["f55"], f["f53"],
                                 prev, f["f56"]))
            prev = r2(f["f53"])
        log.info("东财 %s rows=%d", product, len(data["klines"]))
        time.sleep(0.5)  # 降频，避免触发限流
    return rows


def backfill_pre_close(cur, rows):
    out = []
    for r in rows:
        r = list(r)
        if r[6] is None:
            cur.execute("select close from public.daily_data where contract_code=%s "
                        "and trade_date < %s order by trade_date desc limit 1", (r[0], r[5]))
            hit = cur.fetchone()
            if hit:
                r[6] = f"{float(hit[0]):.2f}"
                r[11] = f"{float(r[10]) - float(r[6]):.2f}"
        out.append(tuple(r))
    return out


def upsert(url, rows):
    if not rows:
        log.warning("无数据可写")
        return 0
    rows = [tuple(None if v == "" else v for v in r) for r in rows]
    with psycopg2.connect(url, connect_timeout=15) as conn:
        with conn.cursor() as cur:
            rows = backfill_pre_close(cur, rows)
            cur.execute("select column_name from information_schema.columns "
                        "where table_schema='public' and table_name='daily_data'")
            db_cols = {x[0] for x in cur.fetchall()}
            cols = [c for c in ALL_COLS if c in db_cols]
            idx = [ALL_COLS.index(c) for c in cols]
            rows = [tuple(r[i] for i in idx) for r in rows]
            update_cols = [c for c in cols if c not in ("contract_code", "trade_date")]
            sql = ("insert into public.daily_data (" + ",".join(cols) + ") values %s "
                   "on conflict (contract_code, trade_date) do update set "
                   + ", ".join(f"{c}=excluded.{c}" for c in update_cols))
            execute_values(cur, sql, rows, page_size=500)
            cur.execute("select count(*) from public.daily_data")
            total = cur.fetchone()[0]
        conn.commit()
    log.info("写入完成：本批 %d 行，库内累计 %d 行", len(rows), total)
    return len(rows)


def main():
    url = os.environ.get("DATABASE_URL", "").strip()
    if not url:
        log.error("缺少 DATABASE_URL")
        sys.exit(2)
    today = date.today()
    start = today - timedelta(days=LOOKBACK_DAYS)
    log.info("拉取区间 %s ~ %s", start, today)

    rows = fetch_yahoo(start, today)
    source = "Yahoo"
    if not rows:
        log.info("Yahoo 无数据，降级东财")
        rows = fetch_eastmoney(start, today)
        source = "EastMoney"
    if not rows:
        log.error("两个数据源均未抓到数据，终止（不写库）")
        sys.exit(3)
    log.info("使用数据源：%s，合计 %d 行", source, len(rows))
    upsert(url, rows)


if __name__ == "__main__":
    main()
