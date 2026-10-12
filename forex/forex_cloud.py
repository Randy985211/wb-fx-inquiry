# -*- coding: utf-8 -*-
"""银行询价云端抓取→腾讯文档直写（GitHub Actions 版 v2）

v2 变更（修复 15:00 时点从未触发的问题）：
  1. cron 改为工作日 UTC 1:00-8:50 每 10 分钟（北京 9:00-16:50），由脚本自行判定
     应抓时点——GitHub 定时触发偶发延迟/丢失不再导致某个时点整天漏抓
  2. 幂等：时点块已存在且数据完整（≥8行、无"数据源异常"）→ 跳过不重写
  3. 补漏：当日已过时点若仍无数据或存在异常行 → 自动补写/就地重写
  4. 修复 v1 刷新旧块时公司名称列被清空的 bug；读取范围 500→2000 行
  5. 回读校验失败以非零退出码结束（Action 标红，GitHub 自动发邮件提醒）

功能：工作日 09:40/10:00/10:30/11:00/11:30/14:00/15:00/15:30/16:15/16:50/17:30
     （北京时间）共11个时点，抓取 8 家银行现汇买入价，
     按"牌价+优惠点=最终报价"计算后直写腾讯文档「银行询价」子表。
     节假日自动跳过；单家银行失败写"数据源异常"，绝不编造。

数据源（全部官方，中信为聚合站兜底并标注）：
  农业银行(美元)  https://ewealth.abchina.com/app/data/api/DataService/ExchangeRateV2  ÷100
  中国银行        https://www.boc.cn/sourcedb/whpj/  现汇买入价 ÷100（兼作节假日判定）
  中信银行(欧元)  https://www.ip138.com/paijia/citicbank.htm（兜底 https://www.cnyrate.net/eci.html，
                  均为转载中信官方牌价的聚合站；中信官网为加密网关无法直连）
  建设银行(欧元)  http://forex1.ccb.com/cn/home/news/jshckpj_new.xml  官方XML BidRateOfCcy(现汇买入)
  民生银行(欧元)  https://www.cmbc.com.cn/gw/po_web/queryExRateByForex.do  POST cxfg=1&domesticCurrency=RMB
  中国银行(欧元)  同上中行 whpj
  兴业银行(欧元)  personalbank.cib.com.cn 两步请求（GET拿session→POST /list 取 ifxBuyPrice ÷100）
  交通银行(欧元)  https://www.bankcomm.com/BankCommSite/default.shtml  首页汇市信息 ÷100

环境变量：
  TDOC_TOKEN  腾讯文档 MCP Authorization Token（32位hex，与本地 ~/.mcporter/mcporter.json 一致）
  FX_SLOT     可选，强制指定时点（1000/1100/1500/1615）；缺省自动判定+补漏
  FX_DRYRUN   设为 1 则只抓取计算不写表
"""
import csv
import io
import json
import os
import re
import ssl
import sys
import time
import urllib.request
import gzip
from datetime import datetime, timezone, timedelta

FILE_ID = "OPWMZyPohMGZ"
SHEET_ID = "5kzK4f"
SHEET_MCP = "https://docs.qq.com/api/v6/sheet/mcp"
BJ = timezone(timedelta(hours=8))
SLOTS = [(9, 40), (10, 0), (10, 30), (11, 0), (11, 30), (14, 0), (15, 0), (15, 30), (16, 15), (16, 50), (17, 30)]
SLOT_NAMES = {(9, 40): "09:40", (10, 0): "10:00", (10, 30): "10:30", (11, 0): "11:00", (11, 30): "11:30",
              (14, 0): "14:00", (15, 0): "15:00", (15, 30): "15:30", (16, 15): "16:15", (16, 50): "16:50",
              (17, 30): "17:30"}
SLOT_ORDER = [SLOT_NAMES[s] for s in SLOTS]
SLOT_INDEX = {name: i for i, name in enumerate(SLOT_ORDER)}
WINDOW_EARLY = 10   # 时点前多少分钟内允许开写（避免过早抓牌价）
WINDOW_LATE = 25    # 时点后多少分钟内仍可写（GitHub延迟兜底）；超过则该时点留空不补，
                    # 避免用当前牌价冒充历史时点数据（用户明确要求）。人工补录走 FX_SLOT 显式指定。
BANKS = [
    ("农业银行镇江新区支行", "USD", "abc"),
    ("中国银行镇江大港支行", "USD", "boc"),
    ("中信银行赣州滨江东支行", "EUR", "citic"),
    ("建设银行赣州开发区支行", "EUR", "ccb"),
    ("民生银行赣州河东支行", "EUR", "cmbc"),
    ("中国银行赣州新区支行", "EUR", "boc"),
    ("兴业银行赣州分行营业部", "EUR", "cib"),
    ("交通银行广州珠村支行", "EUR", "bocom"),
]
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36"}
SSL_CTX = ssl.create_default_context()
SSL_CTX.options |= 0x4  # 允许旧式 TLS 重协商（农行/交行需要）
OPENER = urllib.request.build_opener(urllib.request.HTTPSHandler(context=SSL_CTX))

_log = []


def log(msg):
    print(msg, flush=True)
    _log.append(str(msg))


def http_get(url, headers=None, timeout=25):
    h = dict(UA)
    h.update(headers or {})
    req = urllib.request.Request(url, headers=h)
    resp = OPENER.open(req, timeout=timeout)
    raw = resp.read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    for enc in ("utf-8", "gbk"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            pass
    return raw.decode("utf-8", "ignore")


def http_post(url, data, headers=None, timeout=25):
    h = dict(UA)
    h.update(headers or {})
    req = urllib.request.Request(url, headers=h, data=data)
    resp = OPENER.open(req, timeout=timeout)
    raw = resp.read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return raw.decode("utf-8", "ignore")


def retry(fn, times=2):
    last = None
    for _ in range(times + 1):
        try:
            return fn()
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(2)
    raise last


# ──────────────────────── 各银行抓取（返回 1外币兑人民币 4位小数 float）────────────────────────

def fetch_boc():
    html = http_get("https://www.boc.cn/sourcedb/whpj/")
    m = re.search(r"(\d{4})/(\d{2})/(\d{2})", html)
    pub_date = f"{m.group(1)}-{m.group(2)}-{m.group(3)}" if m else None
    out = {}
    for cur in ("美元", "欧元"):
        mm = re.search(r"<td>%s</td>\s*<td>([\d.]+)</td>" % cur, html)
        if mm:
            out[cur] = round(float(mm.group(1)) / 100, 4)
    return pub_date, out.get("美元"), out.get("欧元")


def fetch_abc():
    j = json.loads(http_get("https://ewealth.abchina.com/app/data/api/DataService/ExchangeRateV2"))
    for r in j.get("Data", {}).get("Table", []):
        if str(r.get("CurrName", "")).startswith("美元"):
            return round(float(r["BuyingPrice"]) / 100, 4)
    raise ValueError("USD not found")


def fetch_citic():
    # ip138（每1外币口径，含更新时间）
    try:
        t = re.sub(r"<[^>]+>", " ", http_get("https://www.ip138.com/paijia/citicbank.htm"))
        m = re.search(r"欧元\s*EUR\s*([\d.]+)\s*([\d.]+)", t)
        if m:
            return round(float(m.group(1)), 4)
    except Exception:  # noqa: BLE001
        pass
    # 兜底 cnyrate（每100外币）
    t2 = re.sub(r"<[^>]+>", " ", http_get("https://www.cnyrate.net/eci.html"))
    m2 = re.search(r"欧元\s*([\d.]+)\s*--", t2)
    if not m2:
        raise ValueError("citic no match")
    return round(float(m2.group(1)) / 100, 4)


def fetch_ccb():
    x = http_get("http://forex1.ccb.com/cn/home/news/jshckpj_new.xml")
    m = re.search(r"978.*?<Ofr_Ccy_CcyCd>156</Ofr_Ccy_CcyCd>\s*<BidRateOfCash>([\d.]+)</BidRateOfCash>"
                  r"\s*<OfrRateOfCash>([\d.]+)</OfrRateOfCash>\s*<BidRateOfCcy>([\d.]+)</BidRateOfCcy>", x, re.S)
    if not m:
        raise ValueError("EUR block not found")
    return round(float(m.group(3)), 4)  # BidRateOfCcy = 现汇买入价（每1外币）


def fetch_cmbc():
    body = b"cxfg=1&domesticCurrency=RMB"
    j = json.loads(http_post("https://www.cmbc.com.cn/gw/po_web/queryExRateByForex.do", body, headers={
        "Content-Type": "application/x-www-form-urlencoded", "X-Requested-With": "XMLHttpRequest",
        "Referer": "https://www.cmbc.com.cn/sy/xqsj/wh/dgjsh/"}))
    for r in j.get("result", []):
        if r.get("foreignCurrency") == "EUR":
            return round(float(r["buyPrice"]), 4)
    raise ValueError("EUR not found")


def fetch_cib():
    import http.cookiejar
    cj = http.cookiejar.CookieJar()
    op = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj),
                                     urllib.request.HTTPSHandler(context=SSL_CTX))
    req = urllib.request.Request("https://personalbank.cib.com.cn/pers/main/pubinfo/ifxQuotationQuery.do",
                                 headers=UA)
    op.open(req, timeout=25).read()
    nd = str(int(time.time() * 1000))
    body = f"dataSet.page=1&dataSet.rows=80&dataSet.sidx=&dataSet.sord=asc&dataSet.nd={nd}".encode()
    req2 = urllib.request.Request(
        "https://personalbank.cib.com.cn/pers/main/pubinfo/ifxQuotationQuery/list",
        headers={**UA, "X-Requested-With": "XMLHttpRequest", "Content-Type": "application/x-www-form-urlencoded",
                 "Referer": "https://personalbank.cib.com.cn/pers/main/pubinfo/ifxQuotationQuery.do"},
        data=body)
    j = json.loads(op.open(req2, timeout=25).read())
    for row in j.get("rows", []):
        c = row.get("cell", [])
        if len(c) >= 4 and c[1] == "EUR":
            return round(float(c[3]) / 100, 4)
    raise ValueError("EUR not found")


def fetch_bocom():
    t = re.sub(r"<[^>]+>", " ", http_get("https://www.bankcomm.com/BankCommSite/default.shtml"))
    m = re.search(r"欧元EUR\s*([\d]+\.[\d]+)\s*([\d]+\.[\d]+)", t)
    if not m:
        raise ValueError("EUR not found")
    return round(float(m.group(1)) / 100, 4)


FETCHERS = {"abc": fetch_abc, "citic": fetch_citic, "ccb": fetch_ccb,
            "cmbc": fetch_cmbc, "cib": fetch_cib, "bocom": fetch_bocom}


# ──────────────────────── 腾讯文档 MCP HTTP 客户端 ────────────────────────

def mcp_call(tool, arguments):
    token = os.environ["TDOC_TOKEN"]
    body = {"jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": {"name": tool, "arguments": arguments}}
    last = None
    for _ in range(3):
        try:
            req = urllib.request.Request(
                SHEET_MCP, data=json.dumps(body).encode(),
                headers={"Content-Type": "application/json", "Accept": "application/json, text/event-stream",
                         "Authorization": token})
            resp = urllib.request.urlopen(req, timeout=60)
            j = json.loads(resp.read())
            if "error" in j:
                raise RuntimeError(f"MCP error: {j['error']}")
            res = j["result"]
            text = res["content"][0]["text"] if res.get("content") else ""
            if res.get("isError"):
                raise RuntimeError(f"MCP tool error: {str(text)[:200]}")
            try:
                return json.loads(text)
            except (json.JSONDecodeError, TypeError):
                return {"raw": text}
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(3)
    raise last


def sheet_find_tail():
    """只读 A:B 两列定位表最后一行有数据的位置（轻量）。固定 2000 行上限在
    表格每天增长约110行、约18天后会造成新块写错位置并覆盖已有数据。"""
    r = mcp_call("get_cell_data", {"file_id": FILE_ID, "sheet_id": SHEET_ID,
                                   "start_row": 0, "start_col": 0, "end_row": 20000, "end_col": 1,
                                   "return_csv": True})
    rows = list(csv.reader(io.StringIO(r.get("csv_data", ""))))
    last = 0
    for i, rr in enumerate(rows):
        a = (list(rr) + ["", ""])[0].strip()
        b = (list(rr) + ["", ""])[1].strip()
        if a or b:
            last = i
    return last


def sheet_read(span=600):
    """返回 (rows, base)：只读表尾 span 行（覆盖约45天，足够算变动基准），
    base 为 rows[0] 对应的绝对行号。"""
    last = sheet_find_tail()
    base = max(0, last - span)
    r = mcp_call("get_cell_data", {"file_id": FILE_ID, "sheet_id": SHEET_ID,
                                   "start_row": base, "start_col": 0, "end_row": last + 1, "end_col": 9,
                                   "return_csv": True})
    rows = list(csv.reader(io.StringIO(r.get("csv_data", ""))))
    return [list(r[:9]) + [""] * max(0, 9 - len(r)) for r in rows], base


def sheet_write(start_row, start_col, rows_csv_text):
    return mcp_call("set_range_value_by_csv", {"file_id": FILE_ID, "sheet_id": SHEET_ID,
                                               "start_row": start_row, "start_col": start_col,
                                               "csv_data": rows_csv_text})


def fmt(x):
    return f"{x:.4f}" if isinstance(x, float) else str(x)


def parse_blocks(rows, base=0):
    """按「日期+时间」组合键分块（每行A列都有日期，不能按行分块）。
    base 为 rows[0] 的绝对行号，块起始行号按绝对行记录。"""
    blocks = {}
    cur_key, cur_start, cur_rows = None, None, []
    for i, r in enumerate(rows):
        a, b = r[0].strip(), r[1].strip()
        is_date_row = bool(re.match(r"^\d{4}-\d{2}-\d{2}$", a)) and bool(re.match(r"^\d{1,2}:\d{2}$", b))
        all_empty = not any(c.strip() for c in r)
        if all_empty:
            if cur_key and cur_rows:
                blocks.setdefault(cur_key[0], {})[cur_key[1]] = (cur_start, cur_rows)
            cur_key, cur_rows = None, []
        elif is_date_row:
            if cur_key and cur_rows and cur_key != (a, b):
                blocks.setdefault(cur_key[0], {})[cur_key[1]] = (cur_start, cur_rows)
                cur_key, cur_rows = None, []
            if cur_key != (a, b):
                cur_key, cur_start, cur_rows = (a, b), base + i, [r]
            else:
                cur_rows.append(r)
        elif cur_key:
            cur_rows.append(r)
    if cur_key and cur_rows:
        blocks.setdefault(cur_key[0], {})[cur_key[1]] = (cur_start, cur_rows)
    return blocks


def block_clean(blk):
    """块完整 = ≥8行 且 没有"数据源异常"行。"""
    return len(blk) >= 8 and not any(r[5].strip() == "数据源异常" for r in blk)


def main():
    now = datetime.now(BJ)
    today = now.strftime("%Y-%m-%d")
    now_min = now.hour * 60 + now.minute
    log(f"=== 银行询价 v2 云端运行 北京时间 {now:%Y-%m-%d %H:%M} ===")

    # 0. 确定本次应处理的时点
    slot_env = os.environ.get("FX_SLOT")
    if slot_env:
        due_slots = [(int(slot_env[:2]), int(slot_env[2:]))]
        log(f"FX_SLOT={slot_env}，强制处理该时点。")
    else:
        if now.weekday() >= 5:
            log("周末休市，快速退出。")
            return 0
        due_slots = [s for s in SLOTS
                     if -WINDOW_EARLY <= now_min - (s[0] * 60 + s[1]) <= WINDOW_LATE]
        if not due_slots:
            nxt = next((s for s in SLOTS if s[0] * 60 + s[1] - WINDOW_EARLY > now_min), None)
            if nxt is None:
                # 已过全部时点窗口：睡到收工点再退出（chain_next 到点自然停止），防秒级空转刷棒
                log(f"北京 {now:%H:%M} 已过全部时点窗口，睡到 17:57 收工。")
                time.sleep(max(0, 17 * 60 + 57 - now_min) * 60)
                return 0
            wait_min = nxt[0] * 60 + nxt[1] - WINDOW_EARLY - now_min + 1
            log(f"北京 {now:%H:%M} 不在任何时点窗口内，等待 {wait_min} 分钟至 "
                f"{nxt[0]:02d}:{nxt[1]:02d} 窗口（防秒级空转刷棒）。")
            time.sleep(wait_min * 60)
            now = datetime.now(BJ)
            today = now.strftime("%Y-%m-%d")
            now_min = now.hour * 60 + now.minute
            due_slots = [s for s in SLOTS
                         if -WINDOW_EARLY <= now_min - (s[0] * 60 + s[1]) <= WINDOW_LATE] or [nxt]

    # 1. 读表（先做幂等判定，全部跳过时无需抓数，节省运行时长）
    rows, base = retry(sheet_read)
    blocks = parse_blocks(rows, base)
    today_blocks = dict(blocks.get(today, {}))
    dates = sorted(d for d in blocks if d <= today)
    prev_day = next((d for d in reversed(dates) if d < today), None)
    log(f"表内已有: 今日{list(today_blocks.keys()) or '无'} "
        f"上日({prev_day}){list(blocks.get(prev_day, {}).keys()) if prev_day else '无'}")

    todo = []
    for s in due_slots:
        k = SLOT_NAMES[s]
        if k in today_blocks and block_clean(today_blocks[k][1]):
            log(f"{k} 块已存在且数据完整，跳过。")
        else:
            todo.append(s)
    if not todo:
        end_min = max(s[0] * 60 + s[1] for s in due_slots) + WINDOW_LATE
        if end_min > now_min:
            log(f"本次应处理时点均已完整，等待窗口结束 {end_min // 60:02d}:{end_min % 60:02d} "
                f"后退出（防秒级接力刷棒）。")
            time.sleep((end_min + 1 - now_min) * 60)
        else:
            log("本次应处理时点均已完整，无需写入，退出。")
        return 0

    # 2. 抓数（含节假日判定）
    try:
        pub_date, boc_usd, boc_eur = retry(fetch_boc)
    except Exception as e:  # noqa: BLE001
        log(f"[异常] 中行抓取失败: {e!r}")
        pub_date, boc_usd, boc_eur = None, None, None
    if pub_date and pub_date != today:
        end_min = max(s[0] * 60 + s[1] for s in due_slots) + WINDOW_LATE
        log(f"中行牌价发布日期为 {pub_date}（非今日），判定休市，"
            f"睡到窗口结束 {end_min // 60:02d}:{end_min % 60:02d} 后退出（防秒级接力刷棒）。")
        time.sleep(max(0, end_min + 1 - now_min) * 60)
        return 0

    prices = {"boc": {"USD": boc_usd, "EUR": boc_eur}, "note": {}}
    for code, fn in FETCHERS.items():
        try:
            prices[code] = {"EUR": retry(fn)} if code != "abc" else {"USD": retry(fn)}
        except Exception as e:  # noqa: BLE001
            prices[code] = {"EUR": None} if code != "abc" else {"USD": None}
            prices["note"][code] = repr(e)[:80]
            log(f"[异常] {code} 抓取失败: {e!r}")
    log("抓取结果: " + json.dumps({k: v for k, v in prices.items() if k != "note"},
                                  ensure_ascii=False))

    bank_row = lambda blk: {r[3].strip(): r for r in blk}  # noqa: E731

    def prev_block_rows(slot_idx):
        """变动列基准：今日更早时点块 > 上一日最后一块。"""
        for k in reversed(SLOT_ORDER[:slot_idx]):
            if k in today_blocks:
                return bank_row(today_blocks[k][1])
        if prev_day and blocks.get(prev_day):
            lk = list(blocks[prev_day].keys())[-1]
            return bank_row(blocks[prev_day][lk][1])
        return None

    def disc_source():
        """优惠点来源：今日最新一块 > 上一日最后一块。"""
        for k in reversed(SLOT_ORDER):
            if k in today_blocks:
                return bank_row(today_blocks[k][1])
        if prev_day and blocks.get(prev_day):
            lk = list(blocks[prev_day].keys())[-1]
            return bank_row(blocks[prev_day][lk][1])
        return None

    written = []
    for slot in todo:  # SLOTS 顺序即时间顺序
        slot_name = SLOT_NAMES[slot]
        slot_idx = SLOT_INDEX[slot_name]
        delta = now_min - (slot[0] * 60 + slot[1])
        tag = "窗口" if delta <= 20 else "补漏"
        prev_slot_rows = prev_block_rows(slot_idx)
        disc_src = disc_source()

        # 3. 组装 8 行
        new_rows, summary = [], []
        for name, ccy, src in BANKS:
            price = prices.get(src, {}).get(ccy)
            disc = ""
            if disc_src and name in disc_src:
                try:
                    disc = f"{float(disc_src[name][6]):.4f}"
                except (ValueError, IndexError):
                    disc = disc_src[name][6]
            final = change = ""
            if price is not None:
                try:
                    final = f"{price + float(disc):.4f}" if disc else ""
                except ValueError:
                    final = ""
            if prev_slot_rows and name in prev_slot_rows and final:
                try:
                    change = f"{float(final) - float(prev_slot_rows[name][7]):+.4f}"
                except (ValueError, IndexError):
                    change = ""
            f_txt = fmt(price) if price is not None else "数据源异常"
            new_rows.append([today, slot_name, "", name, ccy, f_txt, disc, final, change or "-"])
            summary.append(f"{name}({ccy}): 牌价{f_txt} 优惠{disc or '?'} 最终{final or '?'} 变动{change or '-'}")

        # 公司名称列从来源块复制（append/refresh 统一处理，修复 v1 refresh 清空C列的bug）
        for r in new_rows:
            if r[3] in (disc_src or {}):
                r[2] = disc_src[r[3]][2]

        if os.environ.get("FX_DRYRUN") == "1":
            log(f"【DRYRUN】{slot_name}（{tag}）逐行:")
            for s_, r_ in zip(summary, new_rows):
                log(f"  {s_}  | {'|'.join(r_)}")
            continue

        # 4. 写入（已有块就地刷新；无块追加到表尾，块间空一行）
        if slot_name in today_blocks:
            start = today_blocks[slot_name][0]
            action = "刷新"
        else:
            pos = [(st + len(rs)) for d_ in blocks for st, rs in blocks[d_].values()]
            start = (max(pos) + 2) if pos else 1
            action = "新写入"
        csv_text = "\n".join(",".join("" if x in ("", None) else str(x) for x in r) for r in new_rows)
        retry(lambda st=start, tx=csv_text: sheet_write(st, 0, tx))
        log(f"{slot_name}（{tag}）{action}完成：第 {start + 1} 行起 8 行")
        for s_ in summary:
            log("  " + s_)
        today_blocks[slot_name] = (start, new_rows)
        blocks[today] = today_blocks
        written.append(slot_name)

    if os.environ.get("FX_DRYRUN") == "1" or not written:
        return 0

    # 5. 回读校验（失败→非零退出码，GitHub 会邮件提醒）
    rows2, base2 = retry(sheet_read)
    blocks2 = parse_blocks(rows2, base2)
    fails = []
    for k in written:
        blk = blocks2.get(today, {}).get(k)
        if not blk:
            fails.append(k)
            log(f"[校验失败] {k} 未回读到块")
            continue
        ok = sum(1 for r in blk[1] if r[0] == today and r[1] == k)
        log(f"回读校验 {k}: {ok}/8 行匹配（第 {blk[0] + 1} 行起）")
        if ok < 8:
            fails.append(k)
    if prices["note"]:
        log("异常源: " + json.dumps(prices["note"], ensure_ascii=False))
    log(f"文档: https://docs.qq.com/sheet/{FILE_ID}")
    if fails:
        log(f"校验失败时点: {fails}，请人工检查！")
        return 1
    return 0


REPO_API = "https://api.github.com/repos/Randy985211/wb-fx-inquiry"


def chain_next():
    """链条接力：每次运行结束触发下一棒（public 仓库 Actions 免费不限时）。
    GitHub schedule 对新 workflow 注册失效，链条模式彻底绕开 schedule：
    每天一次种子点火 → 自续到北京 17:56 收工；08:00 前的空转不接力。"""
    now = datetime.now(BJ)
    now_min = now.hour * 60 + now.minute
    if now_min >= 17 * 60 + 56 or now_min < 8 * 60:
        return
    if now.weekday() >= 5:
        return  # 周末休市，不空转
    tok = os.environ.get("GITHUB_TOKEN")
    if not tok:
        return  # 本地手动运行无 GITHUB_TOKEN，不接力
    last = None
    for _ in range(3):
        try:
            req = urllib.request.Request(
                f"{REPO_API}/actions/workflows/fx.yml/dispatches",
                data=json.dumps({"ref": "main"}).encode(),
                headers={"Authorization": f"Bearer {tok}", "Accept": "application/vnd.github+json",
                         "User-Agent": "fx-chain"})
            urllib.request.urlopen(req, timeout=30).read()
            log(f"[接力] 已触发下一棒（{now:%H:%M}）")
            return
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(3)
    log(f"[接力失败] {last!r}")


if __name__ == "__main__":
    try:
        _code = main()
    finally:
        try:
            chain_next()
        except Exception:  # noqa: BLE001
            pass
    sys.exit(_code)
