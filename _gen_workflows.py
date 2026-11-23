# -*- coding: utf-8 -*-
"""生成 GitHub Actions workflow 文件（yongfeng 自动化上云）。
用法：python _gen_workflows.py
生成后本文件可删除。所有敏感值均从 GitHub Secrets 注入，脚本里不写明文。
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parent
WF = ROOT / ".github" / "workflows"
WF.mkdir(parents=True, exist_ok=True)

# 公共：检出 + Python + 依赖缓存 + 安装依赖 + 注入 DATABASE_URL
HEADER = """name: {name}
on:
  schedule:
    - cron: '{cron}'   # 北京时间 {bj}
  workflow_dispatch:   # 允许在 Actions 页面手动触发

permissions:
  contents: read

jobs:
  run:
    runs-on: ubuntu-latest
    timeout-minutes: {timeout}
    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'
          cache: 'pip'

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
{install}

      - name: Run
        env:
          DATABASE_URL: ${{{{ secrets.DATABASE_URL }}}}
          TZ: Asia/Shanghai
{extra_env}
        run: |
{command}
"""

workflows = {
    "option_daily.yml": dict(
        name="01-期权日线(新浪)",
        cron="30 10 * * *",   # 北京 18:30
        bj="18:30 每日",
        timeout=40,
        install='          pip install -r news_articles/requirements.txt',
        extra_env="",
        command='          python news_articles/src/04_crawl_option_daily.py',
    ),
    "futures_tv_daily.yml": dict(
        name="02-外盘期货日线(TradingView)",
        cron="32 10 * * *",   # 北京 18:32
        bj="18:32 每日",
        timeout=90,
        install='          pip install websocket-client psycopg2-binary pandas',
        extra_env="",
        command='          python "外盘期货日线数据库/src/update_daily_tv.py"',
    ),
    "hedging_daily.yml": dict(
        name="03-套保公告(巨潮)",
        cron="34 10 * * *",   # 北京 18:34
        bj="18:34 每日",
        timeout=40,
        install='          pip install -r 套期保值hedging_announcements/requirements.txt',
        extra_env="",
        command='          python "套期保值hedging_announcements/src/04_incremental_update_postgres.py"',
    ),
    "news_daily.yml": dict(
        name="04-新闻编排(Twitter+akshare+RSS)",
        cron="10 15 * * *",   # 北京 23:10
        bj="23:10 每日",
        timeout=45,
        install='          pip install -r news_articles/requirements.txt',
        extra_env='          TWITTER_COOKIES: ${{ secrets.TWITTER_COOKIES }}',
        command='          python news_articles/src/run_daily_news.py',
    ),
    "symbol_weekly.yml": dict(
        name="05-symbol表维护(周日)",
        cron="0 19 * * 6",   # 北京周日 03:00 = UTC 周六 19:00
        bj="周日 03:00（UTC 周六 19:00）",
        timeout=15,
        install='          pip install psycopg2-binary',
        extra_env="",
        command='          python "symbol表/src/02_add_auto_columns.py"',
    ),
    "rollover_weekly.yml": dict(
        name="06-移仓换月宽表维护(周日)",
        cron="30 19 * * 6",  # 北京周日 03:30 = UTC 周六 19:30
        bj="周日 03:30（UTC 周六 19:30）",
        timeout=15,
        install='          pip install psycopg2-binary',
        extra_env='          PG_TABLE: public.移仓换月宽表',
        command='          python "移仓换月宽表/src/02_add_auto_columns.py"',
    ),
    "probe_sources.yml": dict(
        name="00-探测数据源可达性(手动)",
        cron="0 0 1 1 *",   # 仅在 1/1 触发，主要靠 workflow_dispatch 手动跑
        bj="一次性探测，建议手动触发",
        timeout=20,
        install='          pip install requests akshare feedparser websocket-client',
        extra_env="",
        command='''          python - <<'PY'
          import requests, sys
          targets = {
              "新浪财经": "https://finance.sina.com.cn",
              "巨潮资讯": "http://www.cninfo.com.cn",
              "上海金属网": "https://www.metal.com",
              "TradingView WS": "https://data.tradingview.com",
              "X/Twitter": "https://x.com",
          }
          ok = 0
          for name, url in targets.items():
              try:
                  r = requests.get(url, timeout=15)
                  print(f"OK   {name}: {r.status_code}")
                  ok += 1
              except Exception as e:
                  print(f"FAIL {name}: {type(e).__name__}: {e}")
          print(f"reachable {ok}/{len(targets)}")
          PY''',
    ),
}

for fname, cfg in workflows.items():
    content = HEADER.format(**cfg)
    (WF / fname).write_text(content, encoding="utf-8")
    print("written:", WF / fname)

print("total:", len(workflows))
