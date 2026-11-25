"""daily_data 增量日更：东财 COBOT 5 品种 -> Supabase public.daily_data

特点：
1. 增量：默认只拉最近 LOOKBACK_DAYS 天（默认 7 天），覆盖周末/假期无数据的情况
2. 幂等：唯一键 (contract_code, trade_date) 冲突时 UPDATE 行情字段（值可能修正）
3. 通用框架：写 exchange/exchange_code/contract_type/product；created_at 仅插入时生成，updated_at 由触发器自动更新
4. 零依赖数据库密码落盘：DATABASE_URL 从环境变量读（GitHub Actions 走 Secrets）

本地跑：
    set DATABASE_URL=...  (Windows) / export DATABASE_URL=... (bash)
    python 238_20261125_daily_data云端日更.py
"""

import os
import sys
import logging
from datetime import date, timedelta

import psycopg2
from psycopg2.extras import execute_values
from curl_cffi import requests as curl_requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("daily_data_daily")

# 与 237 号脚本一致：COBOT 5 品种（JYS 已核对）
PRODUCTS = {
    "大豆": ("103.ZS00Y", "CBOT", "ZS"),
    "豆油": ("103.ZL00Y", "CBOT", "ZL"),
    "豆粕": ("103.ZM00Y", "CBOT", "ZM"),
    "玉米": ("103.ZC00Y", "CBOT", "ZC"),
    "小麦": ("103.ZW00Y", "CBOT", "ZW"),
}
KLINE_URL = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
FIELDS2 = "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61"
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "7"))

# 注意：库内 daily_data 可能被手动调整过列（如 contract_type 被移除）。
# 这里按「库内实际存在的列」动态裁剪，避免写死导致 UndefinedColumn。
ALL_COLS = ["contract_code", "contract_type", "exchange", "exchange_code",
            "product", "trade_date", "pre_close", "open", "high", "low",
            "close", "change1", "vol"]


def r2(x):
    return None if x in (None, "") else f"{float(x):.2f}"


def fetch_one(product, secid, exchange, excode, start, end):
    params = {"secid": secid, "fields1": "f1,f2,f3,f4,f5", "fields2": FIELDS2,
              "klt": "101", "fqt": "0", "beg": start, "end": end}
    resp = curl_requests.get(KLINE_URL, params=params, impersonate="chrome", timeout=25)
    resp.raise_for_status()
    data = resp.json().get("data")
    if not data or not data.get("klines"):
        log.warning("%s 无数据（%s~%s）", product, start, end)
        return []
    rows, prev = [], None
    for line in data["klines"]:
        f = dict(zip(FIELDS2.split(","), line.split(",")))
        close = r2(f["f53"])
        chg = f"{float(close) - float(prev):.2f}" if prev is not None else None
        rows.append((secid, "continuous", exchange, excode, product, f["f51"],
                     prev, r2(f["f52"]), r2(f["f54"]), r2(f["f55"]), close, chg,
                     str(int(float(f["f56"]))) if f["f56"] else None))
        prev = close
    # 注意：LOOKBACK 窗口内首行的 pre_close 只能用窗口内前值；若窗口首行恰好不是该品种第一日，
    # pre_close 可能为 None。下面入库前会用库内上一交易日 close 回填，保证连续性。
    return rows


def fetch_all():
    today = date.today()
    start = (today - timedelta(days=LOOKBACK_DAYS)).strftime("%Y%m%d")
    end = today.strftime("%Y%m%d")
    log.info("拉取区间 %s~%s（COBOT 5 品种）", start, end)
    all_rows = []
    for p, (secid, ex, ec) in PRODUCTS.items():
        rows = fetch_one(p, secid, ex, ec, start, end)
        log.info("  %s %s rows=%d", p, secid, len(rows))
        all_rows.extend(rows)
    return all_rows


def backfill_pre_close(cur, rows):
    """用库内上一交易日 close 回填窗口首行的 pre_close/change1（保证连续性）。"""
    out = []
    for r in rows:
        r = list(r)
        if r[6] is None:  # pre_close 缺
            cur.execute(
                "select close from public.daily_data where contract_code=%s and trade_date < %s order by trade_date desc limit 1",
                (r[0], r[5]),
            )
            hit = cur.fetchone()
            if hit:
                r[6] = f"{float(hit[0]):.2f}"
                r[11] = f"{float(r[10]) - float(r[6]):.2f}"
        out.append(tuple(r))
    return out


def upsert(url, rows):
    if not rows:
        log.warning("无数据可写，跳过")
        return 0
    # 清洗空串
    rows = [tuple(None if v == "" else v for v in r) for r in rows]
    with psycopg2.connect(url, connect_timeout=15) as conn:
        with conn.cursor() as cur:
            rows = backfill_pre_close(cur, rows)
            # 按库内实际列裁剪：丢弃库里不存在的列（如 contract_type 被删）
            cur.execute("select column_name from information_schema.columns "
                        "where table_schema='public' and table_name='daily_data'")
            db_cols = {r[0] for r in cur.fetchall()}
            cols = [c for c in ALL_COLS if c in db_cols]
            idx = [ALL_COLS.index(c) for c in cols]
            rows = [tuple(r[i] for i in idx) for r in rows]
            log.info("库内列 %d 个，写入列：%s", len(db_cols), ",".join(cols))
            update_cols = [c for c in cols if c not in ("contract_code", "trade_date")]
            upd = ", ".join(f"{c}=excluded.{c}" for c in update_cols)
            sql = ("insert into public.daily_data (" + ",".join(cols) + ") values %s "
                   "on conflict (contract_code, trade_date) do update set " + upd)
            execute_values(cur, sql, rows, page_size=500)
            cur.execute("select count(*) from public.daily_data")
            total = cur.fetchone()[0]
        conn.commit()
    log.info("写入完成：本批 %d 行，库内累计 %d 行", len(rows), total)
    return len(rows)


def main():
    url = os.environ.get("DATABASE_URL", "").strip()
    if not url:
        log.error("缺少 DATABASE_URL 环境变量")
        sys.exit(2)
    rows = fetch_all()
    upsert(url, rows)


if __name__ == "__main__":
    main()
