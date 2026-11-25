"""daily_data 增量日更 v2 -> Supabase public.daily_data

v1 -> v2 修正的三个缺陷
------------------------------------------------------------
[缺陷1] 数据源主备颠倒
    v1 以 Yahoo 为主源。Yahoo 连续合约(=F)有两个硬伤：
      a) Volume 字段是滚动/累计口径，不是当日成交量 —— 实测 11-23 与 11-24
         五天量值完全相同(如大豆 101248/101248)，而东财为 90183/132460；
      b) 连续合约换月日与东财不同步 —— 玉米东财 11-19 换月(跳 +14 美分)，
         Yahoo 到 11-25 才换，两个序列不可混用。
    v2：东财为唯一真值源(价格+成交量)；Yahoo 降为"只补缺口"的备源。

[缺陷2] 写入未收盘的交易日
    CBOT 谷物"交易日 D"= D-1 日 19:00 CT 夜盘 + D 日 08:30~13:20 CT 日盘。
    北京时间白天跑批时，D 日的夜盘才刚开始几小时，此刻抓到的 D 日 bar 是
    残缺的(实测大豆 11-25 量 7048，正常 9~13 万)。
    v2：用 America/Chicago 计算"最后一个已收盘交易日" last_final，
        trade_date > last_final 的行一律丢弃，并 DELETE 库内同类残行。

[缺陷3] Yahoo 备源会覆写东财已有行
    v2：走 Yahoo 时改为 INSERT ... ON CONFLICT DO NOTHING（只补缺口，
        绝不覆盖东财已写入的任何字段）。

其他
    - 幂等：唯一键 (contract_code, trade_date)
    - updated_at 由库内触发器维护
    - DATABASE_URL 从环境变量读（云端走 Secrets），不落盘
    - 环境变量：LOOKBACK_DAYS（默认 20，含修复余量）
"""

import os
import sys
import time
import logging
from datetime import date, datetime, timedelta, timezone

import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("daily_data_daily")

# 品种 -> (contract_code, yahoo_symbol, exchange, exchange_code)
PRODUCTS = {
    "大豆": ("103.ZS00Y", "ZS=F", "CBOT", "ZS"),
    "豆油": ("103.ZL00Y", "ZL=F", "CBOT", "ZL"),
    "豆粕": ("103.ZM00Y", "ZM=F", "CBOT", "ZM"),
    "玉米": ("103.ZC00Y", "ZC=F", "CBOT", "ZC"),
    "小麦": ("103.ZW00Y", "ZW=F", "CBOT", "ZW"),
}

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "20"))

ALL_COLS = ["contract_code", "contract_type", "exchange", "exchange_code",
            "product", "trade_date", "pre_close", "open", "high", "low",
            "close", "change1", "vol"]
FIELDS2 = "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61"
EM_HOSTS = [
    "https://push2his.eastmoney.com/api/qt/stock/kline/get",
    "https://push2his-ali.eastmoney.com/api/qt/stock/kline/get",
]

# CBOT 谷物与油籽：日盘 08:30~13:20 CT，夜盘 19:00~07:45 CT（夜盘归属下一交易日）
SESSION_CLOSE_H, SESSION_CLOSE_M = 13, 20


# ---------------------------------------------------------------- 交易日历
def cbot_now():
    """返回美国中部时间。本地无 tzdata 时退化为固定 UTC-6。"""
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("America/Chicago"))
    except Exception:
        return datetime.now(timezone.utc) - timedelta(hours=6)


def last_final_trade_date():
    """最后一个已收盘的 CBOT 交易日。

    交易日 D 在 D 日 13:20 CT 收盘后才定型，因此：
        last_final = (now_CT - 13h20m) 的日期
    例：now_CT = 11-24 23:19（11-25 交易日夜盘中）
        -> -13h20m = 11-24 09:59 -> last_final = 11-24，11-25 被拦下。
    """
    now = cbot_now()
    return (now - timedelta(hours=SESSION_CLOSE_H, minutes=SESSION_CLOSE_M)).date()


def drop_unclosed(rows):
    """剔除未收盘 / 未来交易日的 bar。"""
    lim = last_final_trade_date()
    kept, dropped = [], []
    for r in rows:
        d = r[5]
        d = d if isinstance(d, date) else date.fromisoformat(str(d)[:10])
        (kept if d <= lim else dropped).append(r)
    if dropped:
        log.info("拦截未收盘交易日 %d 行（last_final=%s，CBOT 现在 %s）：%s",
                 len(dropped), lim, cbot_now().strftime("%Y-%m-%d %H:%M %Z"),
                 sorted({str(x[5]) for x in dropped}))
    return kept


# ---------------------------------------------------------------- 工具
def r2(x):
    return None if x in (None, "") else f"{float(x):.2f}"


def make_row(contract_code, product, ex, ec, d, o, h, l, c, prev, vol):
    chg = f"{float(c) - float(prev):.2f}" if prev is not None else None
    return (contract_code, "continuous", ex, ec, product, str(d)[:10], prev,
            r2(o), r2(h), r2(l), r2(c), chg,
            str(int(float(vol))) if vol not in (None, "") else None)


# ---------------------------------------------------------------- 东财（主源）
def fetch_eastmoney(start_d, end_d):
    """东财 push2his K 线。secid 前缀：103=CBOT, 110=MDEX, 115=CZCE, 159=EFI。"""
    from curl_cffi import requests as cr
    beg, end = start_d.strftime("%Y%m%d"), end_d.strftime("%Y%m%d")
    rows = []
    for product, (code, _ysym, ex, ec) in PRODUCTS.items():
        params = {"secid": code, "fields1": "f1,f2,f3,f4,f5", "fields2": FIELDS2,
                  "klt": "101", "fqt": "0", "beg": beg, "end": end}
        data, err = None, None
        for host in EM_HOSTS:
            for attempt in range(2):
                try:
                    resp = cr.get(host, params=params, impersonate="chrome", timeout=25)
                    resp.raise_for_status()
                    j = resp.json()
                    if j.get("data") and j["data"].get("klines"):
                        data = j["data"]
                        break
                except Exception as exc:      # 限流/跨境断链都会走到这里
                    err = exc
                    time.sleep(2 + attempt * 2)
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
        time.sleep(1.5)          # 降频，避免触发限流
    return rows


# ---------------------------------------------------------------- Yahoo（备源，只补缺口）
def fetch_yahoo(start_d, end_d):
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
        probe = next(iter(PRODUCTS.values()))[1]
        for lvl in (1, 0):
            if probe in df.columns.get_level_values(lvl):
                ticker_level = lvl
                break
    rows = []
    for product, (code, ysym, ex, ec) in PRODUCTS.items():
        try:
            if ticker_level is None:
                sub = df.copy()
            else:
                sub = df.xs(ysym, axis=1, level=ticker_level).copy()
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


# ---------------------------------------------------------------- 写库
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


def write(url, rows, source):
    """source='EastMoney' -> 全字段 UPSERT；source='Yahoo' -> 只补缺口 DO NOTHING。"""
    if not rows:
        log.warning("无数据可写")
        return 0
    rows = [tuple(None if v == "" else v for v in r) for r in rows]
    lim = last_final_trade_date()
    with psycopg2.connect(url, connect_timeout=20) as conn:
        with conn.cursor() as cur:
            rows = backfill_pre_close(cur, rows)
            cur.execute("select column_name from information_schema.columns "
                        "where table_schema='public' and table_name='daily_data'")
            db_cols = {x[0] for x in cur.fetchall()}
            cols = [c for c in ALL_COLS if c in db_cols]
            idx = [ALL_COLS.index(c) for c in cols]
            rows = [tuple(r[i] for i in idx) for r in rows]

            if source == "EastMoney":
                upd = [c for c in cols if c not in ("contract_code", "trade_date")]
                sql = ("insert into public.daily_data (" + ",".join(cols) + ") values %s "
                       "on conflict (contract_code, trade_date) do update set "
                       + ", ".join(f"{c}=excluded.{c}" for c in upd))
            else:
                sql = ("insert into public.daily_data (" + ",".join(cols) + ") values %s "
                       "on conflict (contract_code, trade_date) do nothing")
            execute_values(cur, sql, rows, page_size=500)

            # 永久护栏：清掉任何晚于"最后一个已收盘交易日"的残 bar
            cur.execute("delete from public.daily_data where trade_date > %s", (lim,))
            purged = cur.rowcount

            cur.execute("select count(*), max(trade_date) from public.daily_data")
            total, dmax = cur.fetchone()
        conn.commit()
    log.info("写入完成：源=%s 本批 %d 行，清理未收盘残行 %d，库内累计 %d 行，最新 %s",
             source, len(rows), purged, total, dmax)
    return len(rows)


def sanity(cur_hint=""):
    """写后体检：相邻交易日成交量完全重复 = 源串味或残 bar。"""
    url = os.environ.get("DATABASE_URL", "").strip()
    with psycopg2.connect(url, connect_timeout=20) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                select contract_code, trade_date::text, vol
                from (
                  select contract_code, trade_date, vol,
                         lag(vol) over (partition by contract_code order by trade_date) pv
                  from public.daily_data) t
                where pv is not null and vol = pv
                order by contract_code, trade_date""")
            bad = cur.fetchall()
    if bad:
        log.warning("体检异常：%d 处相邻交易日成交量完全重复 %s", len(bad), bad[:10])
    else:
        log.info("体检通过：无相邻交易日成交量重复")
    return bad


def main():
    url = os.environ.get("DATABASE_URL", "").strip()
    if not url:
        log.error("缺少 DATABASE_URL")
        sys.exit(2)
    lim = last_final_trade_date()
    start = lim - timedelta(days=LOOKBACK_DAYS)
    log.info("CBOT 现在 %s；最后一个已收盘交易日 %s；拉取 %s ~ %s",
             cbot_now().strftime("%Y-%m-%d %H:%M %Z"), lim, start, lim)

    rows = fetch_eastmoney(start, lim)
    source = "EastMoney"
    if not rows:
        log.warning("东财失败，降级 Yahoo（仅补缺口，不覆盖东财已有行）")
        rows = fetch_yahoo(start, lim)
        source = "Yahoo"
    if not rows:
        log.error("两个数据源均未抓到数据，终止（不写库）")
        sys.exit(3)

    rows = drop_unclosed(rows)
    log.info("使用数据源：%s，合计 %d 行", source, len(rows))
    write(url, rows, source)
    sanity()


if __name__ == "__main__":
    main()
