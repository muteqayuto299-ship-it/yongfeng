"""daily_data 一次性数据修正执行器（2026-11-25）

背景
----
2026-11-25 的云端日更（v1 脚本，Yahoo 主源）把两处脏数据写进了 public.daily_data：

  1) 2026-11-25 的 5 行：CBOT 交易日 11-25 的夜盘当时才跑了约 4 小时，
     抓到的是未收盘残 bar（大豆量 7048，正常 9~13 万）。
  2) 2026-11-18 ~ 2026-11-24 的 25 行：Yahoo 连续合约口径覆盖了东财原始值
     - Volume 是滚动口径：11-23 与 11-24 五天量值完全相同
       （玉米 201551/201551、豆油 60954/60954、豆粕 69831/69831、
         大豆 101248/101248、小麦 51529/51529）
     - 换月日错位：玉米东财 11-19 换月（跳 +14 美分），Yahoo 11-25 才换，
       导致 11-19~11-24 两天序列相差 14~16 美分

本脚本用东财原始抓取结果（234_20261124_改造前_daily_data全量备份.csv）覆盖回滚，
并在同一事务内做 5 项断言，任一失败整体回滚。

幂等：重复执行结果一致。
用法：DATABASE_URL=... python repair_20261125.py
"""

import os
import sys
import logging
from datetime import date, datetime, timedelta, timezone

import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("repair_20261125")

SESSION_CLOSE_H, SESSION_CLOSE_M = 13, 20

# 东财真值：(contract_code, trade_date, pre_close, open, high, low, close, change1, vol)
TRUTH = [
    ("103.ZC00Y", "2026-11-18", 446.75, 446.75, 447.75, 434.25, 435.00, -11.75, 224423),
    ("103.ZC00Y", "2026-11-19", 435.00, 449.00, 456.00, 446.25, 452.75, 17.75, 239327),
    ("103.ZC00Y", "2026-11-20", 452.75, 452.50, 453.00, 447.25, 448.75, -4.00, 175258),
    ("103.ZC00Y", "2026-11-23", 448.75, 449.00, 454.50, 446.50, 449.00, 0.25, 232488),
    ("103.ZC00Y", "2026-11-24", 449.00, 449.00, 452.00, 446.25, 450.25, 1.25, 243349),
    ("103.ZL00Y", "2026-11-18", 62.12, 62.14, 62.25, 59.80, 59.85, -2.27, 81970),
    ("103.ZL00Y", "2026-11-19", 59.85, 60.10, 61.24, 59.83, 60.32, 0.47, 79065),
    ("103.ZL00Y", "2026-11-20", 60.32, 60.33, 60.34, 59.41, 60.00, -0.32, 75219),
    ("103.ZL00Y", "2026-11-23", 60.00, 59.99, 60.42, 59.03, 59.44, -0.56, 89397),
    ("103.ZL00Y", "2026-11-24", 59.44, 59.55, 59.92, 58.06, 58.11, -1.33, 127294),
    ("103.ZM00Y", "2026-11-18", 333.30, 333.10, 336.20, 331.80, 333.90, 0.60, 77985),
    ("103.ZM00Y", "2026-11-19", 333.90, 334.00, 334.90, 329.30, 331.20, -2.70, 76539),
    ("103.ZM00Y", "2026-11-20", 331.20, 330.90, 341.90, 330.70, 340.10, 8.90, 104843),
    ("103.ZM00Y", "2026-11-23", 340.10, 340.10, 344.70, 338.80, 343.90, 3.80, 102395),
    ("103.ZM00Y", "2026-11-24", 343.90, 344.10, 351.50, 342.80, 350.60, 6.70, 111682),
    ("103.ZS00Y", "2026-11-18", 1225.50, 1225.50, 1229.75, 1211.75, 1216.00, -9.50, 118863),
    ("103.ZS00Y", "2026-11-19", 1216.00, 1216.25, 1226.00, 1204.50, 1207.25, -8.75, 117259),
    ("103.ZS00Y", "2026-11-20", 1207.25, 1207.25, 1223.50, 1203.25, 1220.50, 13.25, 112060),
    ("103.ZS00Y", "2026-11-23", 1220.50, 1222.00, 1235.00, 1218.75, 1227.50, 7.00, 90183),
    ("103.ZS00Y", "2026-11-24", 1227.50, 1227.25, 1244.25, 1226.75, 1232.25, 4.75, 132460),
    ("103.ZW00Y", "2026-11-18", 610.25, 610.25, 611.75, 585.00, 586.75, -23.50, 103442),
    ("103.ZW00Y", "2026-11-19", 586.75, 587.25, 599.50, 581.75, 595.25, 8.50, 102175),
    ("103.ZW00Y", "2026-11-20", 595.25, 596.00, 598.00, 587.50, 593.00, -2.25, 90408),
    ("103.ZW00Y", "2026-11-23", 593.00, 596.75, 596.75, 587.75, 594.50, 1.50, 91831),
    ("103.ZW00Y", "2026-11-24", 594.50, 595.75, 597.25, 585.75, 588.75, -5.75, 97783),
]

EXPECT_TOTAL = 1130          # 5 品种 x 226 交易日
EXPECT_ZS_1124_VOL = 132460  # 大豆 2026-11-24 东财真实成交量


def cbot_now():
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("America/Chicago"))
    except Exception:
        return datetime.now(timezone.utc) - timedelta(hours=6)


def last_final_trade_date():
    now = cbot_now()
    return (now - timedelta(hours=SESSION_CLOSE_H, minutes=SESSION_CLOSE_M)).date()


def main():
    url = os.environ.get("DATABASE_URL", "").strip()
    if not url:
        log.error("缺少 DATABASE_URL")
        sys.exit(2)

    lim = last_final_trade_date()
    log.info("CBOT 现在 %s，最后一个已收盘交易日 %s", cbot_now().strftime("%Y-%m-%d %H:%M %Z"), lim)

    with psycopg2.connect(url, connect_timeout=25) as conn:
        conn.autocommit = False
        with conn.cursor() as cur:
            # 0) 修正前快照
            cur.execute("drop table if exists public.daily_data_bak_20261125")
            cur.execute("""create table public.daily_data_bak_20261125 as
                           select * from public.daily_data
                           where trade_date between '2026-11-18' and '2026-11-25'""")
            log.info("快照 public.daily_data_bak_20261125 <- %d 行", cur.rowcount)

            # 1) 删除晚于最后收盘日的残 bar（重点是 2026-11-25）
            cur.execute("delete from public.daily_data where trade_date > %s", (lim,))
            log.info("删除未收盘交易日行：%d 行（trade_date > %s）", cur.rowcount, lim)

            # 2) 东财真值回滚
            execute_values(cur, """
                update public.daily_data d
                   set pre_close = s.pre_close, open = s.open, high = s.high,
                       low = s.low, close = s.close, change1 = s.change1, vol = s.vol
                  from (values %s) as s(contract_code, trade_date, pre_close, open,
                                        high, low, close, change1, vol)
                 where d.contract_code = s.contract_code
                   and d.trade_date    = s.trade_date::date
            """, TRUTH, page_size=100)
            log.info("回滚污染窗口：%d 行", cur.rowcount)

            # 3) 事务内断言
            cur.execute("select count(*), max(trade_date) from public.daily_data")
            total, dmax = cur.fetchone()
            cur.execute("select count(*) from public.daily_data where trade_date = '2026-11-25'")
            n25 = cur.fetchone()[0]
            cur.execute("""select count(*) from (
                             select contract_code, trade_date, vol,
                                    lag(vol) over (partition by contract_code order by trade_date) pv
                               from public.daily_data) t
                            where pv is not null and vol = pv""")
            ndup = cur.fetchone()[0]
            cur.execute("""select count(*) from (
                             select contract_code, trade_date, close,
                                    lag(close) over (partition by contract_code order by trade_date) pc
                               from public.daily_data) t
                            where pc is not null and close = pc""")
            ndup_close = cur.fetchone()[0]
            cur.execute("""select vol from public.daily_data
                            where contract_code='103.ZS00Y' and trade_date='2026-11-24'""")
            zs_vol = cur.fetchone()[0]
            cur.execute("select min(trade_date) from public.daily_data")
            dmin = cur.fetchone()[0]

            checks = [
                ("总行数 == %d" % EXPECT_TOTAL, total == EXPECT_TOTAL, total),
                ("2026-11-25 已清除", n25 == 0, n25),
                ("最新交易日 <= %s" % lim, dmax <= lim, dmax),
                ("无相邻交易日成交量重复", ndup == 0, ndup),
                ("大豆 11-24 量 == %d" % EXPECT_ZS_1124_VOL, zs_vol == EXPECT_ZS_1124_VOL, zs_vol),
                ("历史未被截断（min_date=2026-01-02）", str(dmin) == "2026-01-02", dmin),
            ]
            bad = [(n, v) for n, ok, v in checks if not ok]
            for n, ok, v in checks:
                log.info("  [%s] %s (实际=%s)", "PASS" if ok else "FAIL", n, v)
            # 相邻日收盘价相同虽罕见但非错误（如涨跌停锁死），仅提示不阻断
            log.info("  [INFO] 相邻交易日收盘价相同 %d 处", ndup_close)
            if bad:
                conn.rollback()
                log.error("断言失败，已回滚：%s", bad)
                sys.exit(4)

            conn.commit()
            log.info("已提交：%d 行，%s ~ %s", total, dmin, dmax)

    # 4) 独立只读复查
    with psycopg2.connect(url, connect_timeout=25) as conn, conn.cursor() as cur:
        cur.execute("""select contract_code, count(*), min(trade_date), max(trade_date)
                         from public.daily_data group by 1 order by 1""")
        for r in cur.fetchall():
            log.info("复查 %s rows=%d %s ~ %s", *r)
        cur.execute("""select contract_code, trade_date, close, vol from public.daily_data
                        where trade_date >= '2026-11-24' order by 1,2""")
        for r in cur.fetchall():
            log.info("末行 %s %s close=%s vol=%s", *r)


if __name__ == "__main__":
    main()
