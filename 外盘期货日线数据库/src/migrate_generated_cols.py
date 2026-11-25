"""给 public.daily_data 就地追加三个"自动计算字段"（GENERATED ALWAYS AS ... STORED）

为什么用生成列而不是让脚本算
---------------------------
振幅、涨跌幅、名义成交额都是 close/high/low/pre_close/vol 的确定性函数。
在 Python 里算 = 每写一行算一次、每个查询再算一次，且一旦脚本漏算/算错，
库里就留下与行情不一致的脏值（本次 11-25 事故就是这种）。
改成 STORED 生成列后：
  - 写入方只提供原始 OHLCV，三个派生值由 PG 自己算，不可能不一致；
  - 结果落盘，查询零计算成本，可以直接建索引/做聚合；
  - 历史行在 ADD COLUMN 时自动回算，无需 UPDATE。

幂等：ADD COLUMN IF NOT EXISTS，可重复执行。
用法：DATABASE_URL=... python migrate_generated_cols.py
"""

import os
import sys
import logging

import psycopg2

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("migrate_gen_cols")

DDL = [
    ("pct_chg",
     """alter table public.daily_data
        add column if not exists pct_chg numeric(10,4) generated always as (
            case when pre_close is null or pre_close = 0 then null
                 else round((close - pre_close) / pre_close * 100, 4) end
        ) stored"""),
    ("amplitude",
     """alter table public.daily_data
        add column if not exists amplitude numeric(10,4) generated always as (
            case when pre_close is null or pre_close = 0 then null
                 else round((high - low) / pre_close * 100, 4) end
        ) stored"""),
    ("notional_usd",
     """alter table public.daily_data
        add column if not exists notional_usd numeric(22,2) generated always as (
            case product
              when '大豆' then round(vol::numeric * close * 50,  2)
              when '玉米' then round(vol::numeric * close * 50,  2)
              when '小麦' then round(vol::numeric * close * 50,  2)
              when '豆油' then round(vol::numeric * close * 600, 2)
              when '豆粕' then round(vol::numeric * close * 100, 2)
            end
        ) stored"""),
]

COMMENTS = [
    ("pct_chg", "涨跌幅 % = (close-pre_close)/pre_close*100，4 位小数（原东财 f59）"),
    ("amplitude", "振幅 % = (high-low)/pre_close*100，4 位小数（原东财 f58）"),
    ("notional_usd", "名义成交额 USD = vol*合约乘数*close/100；乘数 大豆/玉米/小麦 5000 蒲式耳、豆油 60000 磅、豆粕 100 短吨。非交易所官方成交金额（东财 f57 对期货恒为 0）"),
]


def main():
    url = os.environ.get("DATABASE_URL", "").strip()
    if not url:
        log.error("缺少 DATABASE_URL")
        sys.exit(2)

    with psycopg2.connect(url, connect_timeout=25) as conn:
        conn.autocommit = False
        with conn.cursor() as cur:
            for name, sql in DDL:
                log.info("执行 DDL: %s", name)
                cur.execute(sql)
            for name, text in COMMENTS:
                cur.execute(f"comment on column public.daily_data.{name} is %s", (text,))
        conn.commit()
    log.info("DDL 提交完成（历史行已随 ADD COLUMN 自动回算）")

    # ---------------------------------------------------------- 验收
    with psycopg2.connect(url, connect_timeout=25) as conn, conn.cursor() as cur:
        cur.execute("""
            select count(*),
                   count(*) filter (where pct_chg is null),
                   count(*) filter (where amplitude is null),
                   count(*) filter (where notional_usd is null)
            from public.daily_data""")
        total, n_pct, n_amp, n_not = cur.fetchone()
        # 每个品种的首个交易日没有前收盘价，涨跌幅/振幅只能为 NULL —— 这是正确行为
        cur.execute("select count(*) from public.daily_data where pre_close is null")
        n_noprev = cur.fetchone()[0]
        log.info("行数=%d  pct_chg空=%d  amplitude空=%d  notional空=%d  无前收盘价行=%d",
                 total, n_pct, n_amp, n_not, n_noprev)

        # 与原始定义逐行比对（应全 0）
        cur.execute("""
            select count(*) filter (where change1 is distinct from round(close - pre_close, 2)),
                   count(*) filter (where pct_chg is distinct from
                         round((close - pre_close) / pre_close * 100, 4)),
                   count(*) filter (where amplitude is distinct from
                         round((high - low) / pre_close * 100, 4))
            from public.daily_data""")
        bad_ch, bad_pct, bad_amp = cur.fetchone()
        log.info("自检：change1 不符=%d  pct_chg 不符=%d  amplitude 不符=%d",
                 bad_ch, bad_pct, bad_amp)

        cur.execute("""
            select contract_code, trade_date, close, pre_close, vol,
                   pct_chg, amplitude, notional_usd
            from public.daily_data
            where trade_date >= '2026-11-20'
            order by contract_code, trade_date""")
        for r in cur.fetchall():
            log.info("  %s %s close=%s pre=%s vol=%s | pct=%s amp=%s notional=%s", *r)

        # 断言：关键锚点值
        cur.execute("""select pct_chg, amplitude, notional_usd
                         from public.daily_data
                        where contract_code='103.ZS00Y' and trade_date='2026-11-24'""")
        pct, amp, notional = cur.fetchone()

    checks = [
        ("总行数 == 1130", total == 1130, total),
        ("pct_chg 空值数 == 无前收盘价行数", n_pct == n_noprev, f"{n_pct} vs {n_noprev}"),
        ("amplitude 空值数 == 无前收盘价行数", n_amp == n_noprev, f"{n_amp} vs {n_noprev}"),
        ("notional_usd 无空值", n_not == 0, n_not),
        ("change1 与 close-pre_close 一致", bad_ch == 0, bad_ch),
        ("pct_chg 与定义一致", bad_pct == 0, bad_pct),
        ("amplitude 与定义一致", bad_amp == 0, bad_amp),
        ("大豆 11-24 涨跌幅 == 0.3870", float(pct) == 0.3870, pct),
        ("大豆 11-24 振幅 == 1.4257", float(amp) == 1.4257, amp),
        ("大豆 11-24 名义额 == 8161191750.00", float(notional) == 8161191750.00, notional),
    ]
    bad = [(n, v) for n, ok, v in checks if not ok]
    for n, ok, v in checks:
        log.info("  [%s] %s (实际=%s)", "PASS" if ok else "FAIL", n, v)
    if bad:
        log.error("验收失败：%s", bad)
        sys.exit(4)
    log.info("验收全部通过")


if __name__ == "__main__":
    main()
