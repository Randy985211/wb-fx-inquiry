# -*- coding: utf-8 -*-
"""银行询价云端抓取→腾讯文档直写（GitHub Actions 版 v3）

v3 变更（修复 10-12 下午全部时点漏写 + 表格改为倒序）：
  【事故根因】v2 用"链条接力"（每棒结束 dispatch 下一棒）维持全天，
    但 mcp_call 在配额报错(400007)时抛异常 → main 崩溃 → finally 里
    chain_next() 仍无条件接力 → 每 15 秒刷一棒，每棒又重复 sheet_read
    消耗配额 → 配额被打爆 → 下午 14:00/15:00/15:30/16:15/16:50/17:30
    全部写入失败。
  【v3 修复】
  1. 彻底删除链条接力（chain_next 变空操作）。改由 GitHub Actions
     schedule（每 15 分钟，北京 09:00-18:00）+ 本机每 30 分钟保活点火
     驱动。全天约 40 棒，不再刷棒，配额消耗降至 1/60。
  2. 配额错误(400007)不重试、不接力，直接退出，等下一棒自然恢复。
  3. 表格改为【倒序】：最新时点永远在最上面（表头下第一行起）。
     写入用 insert_dimension 在正确位置插入 10 行（1 日期 + 8 银行 + 1 空行）。
  4. 幂等判定只需读表头前 400 行（倒序后今日/昨日数据都在顶部），
     省掉 v2 的"读 20000 行定位表尾"，MCP 调用数从 2 次降到 1 次。

功能：工作日 09:40/10:00/10:30/11:00/11:30/14:00/15:00/15:30/16:15/16:50/17:30
     （北京时间）共11个时点，抓取 8 家银行现汇买入价，
     按"牌价+优惠点=最终报价"计算后直写腾讯文档「银行询价」子表。
     节假日自动跳过；单家银行失败写"数据源异常"，绝不编造。

表格布局（0-based 行号）：
     row 0        表头
     row 1 起     数据块，每块 10 行 = 1 日期行 + 8 银行行 + 1 空行
     块按【时间倒序】排列，最新在最上面

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
  FX_SLOT     可选，强制指定时点（0940/1000/.../1730），用于人工补录
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
NROW = 9            # A..I 共 9 列
BLOCK_ROWS = 9      # 每块 8 行银行数据 + 1 空行（日期/时间写在每行 A/B 列）
DATA_START = 1      # row 0 是表头，数据从 row 1 起
READ_SPAN = 400     # 幂等判定只读表头下 400 行（倒序后足够覆盖今日+昨日）
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36"}
SSL_CTX = ssl.create_default_context()
SSL_CTX.options |= 0x4  # 允许旧式 TLS 重协商（农行/交行需要）
OPENER = urllib.request.build_opener(urllib.request.HTTPSHandler(context=SSL_CTX))


class QuotaExceeded(RuntimeError):
    """腾讯文档 MCP 配额打爆（400007）。不可重试，必须立即退出等下一棒。"""


def log(msg):
    print(msg, flush=True)


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
        except QuotaExceeded:
            raise                      # 配额错误不重试
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
    """单次调用，不做内部重试（重试由外层 retry 控制）。
    配额错误 400007 立即抛 QuotaExceeded，不重试。"""
    token = os.environ["TDOC_TOKEN"]
    body = {"jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": {"name": tool, "arguments": arguments}}
    req = urllib.request.Request(
        SHEET_MCP, data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "Accept": "application/json, text/event-stream",
                 "Authorization": token})
    resp = urllib.request.urlopen(req, timeout=60)
    j = json.loads(resp.read())
    if "error" in j:
        if j["error"].get("code") == 400007:
            raise QuotaExceeded("腾讯文档配额打爆(400007)，本棒放弃，等下一棒")
        raise RuntimeError(f"MCP error: {j['error']}")
    res = j["result"]
    text = res["content"][0]["text"] if res.get("content") else ""
    if res.get("isError"):
        if "400007" in str(text):
            raise QuotaExceeded("腾讯文档配额打爆(400007)，本棒放弃，等下一棒")
        raise RuntimeError(f"MCP tool error: {str(text)[:200]}")
    try:
        return json.loads(text)
    except (json.JSONDecodeError, TypeError):
        return {"raw": text}


def sheet_read():
    """只读表头下 READ_SPAN 行（倒序布局：今日/昨日块都在顶部）。"""
    r = mcp_call("get_cell_data", {"file_id": FILE_ID, "sheet_id": SHEET_ID,
                                   "start_row": 0, "start_col": 0,
                                   "end_row": READ_SPAN - 1, "end_col": NROW - 1,
                                   "return_csv": True})
    rows = list(csv.reader(io.StringIO(r.get("csv_data", ""))))
    return [list(x)[:NROW] + [""] * max(0, NROW - len(x)) for x in rows]


def sheet_insert_rows(index, count):
    return mcp_call("insert_dimension", {"file_id": FILE_ID, "sheet_id": SHEET_ID,
                                        "dimension_type": "row", "direction": "before",
                                        "index": index, "count": count})


def sheet_write(start_row, start_col, rows_csv_text):
    return mcp_call("set_range_value_by_csv", {"file_id": FILE_ID, "sheet_id": SHEET_ID,
                                              "start_row": start_row, "start_col": start_col,
                                              "csv_data": rows_csv_text})


def fmt(x):
    return f"{x:.4f}" if isinstance(x, float) else str(x)


def to_min(hhmm):
    h, m = hhmm.split(":")
    return int(h) * 60 + int(m)


def parse_blocks(rows):
    """按「日期+时间」组合键分块，返回按表中顺序（时间倒序）的块列表。
    每项 = (key, start_row, rows)，key=(日期, 时点分钟) 用于排序定位。"""
    out = []
    cur_key, cur_start, cur_rows = None, None, []
    for i, r in enumerate(rows):
        a, b = r[0].strip(), r[1].strip()
        is_date_row = bool(re.match(r"^\d{4}-\d{2}-\d{2}$", a)) and bool(re.match(r"^\d{1,2}:\d{2}$", b))
        all_empty = not any(c.strip() for c in r)
        if all_empty:
            if cur_key and cur_rows:
                out.append((cur_key, cur_start, cur_rows))
            cur_key, cur_rows = None, []
        elif is_date_row:
            if cur_key and cur_rows and cur_key != (a, to_min(b)):
                out.append((cur_key, cur_start, cur_rows))
                cur_key, cur_rows = None, []
            if cur_key != (a, to_min(b)):
                cur_key, cur_start, cur_rows = (a, to_min(b)), i, [r]
            else:
                cur_rows.append(r)
        elif cur_key:
            cur_rows.append(r)
    if cur_key and cur_rows:
        out.append((cur_key, cur_start, cur_rows))
    return out


def block_clean(blk):
    """块完整 = ≥8行 且 没有"数据源异常"行。"""
    return len(blk) >= 8 and not any(r[5].strip() == "数据源异常" for r in blk)


def insert_pos(new_key, ordered_blocks, tail_pos):
    """倒序布局下的插入位置。

    ordered_blocks 必须按表中顺序（= 行号升序 = 时间倒序）传入。
    返回第一个【比 new_key 更旧】的块的 start_row —— 新块插到它上方，
    即整个时间序仍保持倒序。若没有块比 new_key 更旧，说明 new_key 是全表
    最旧，应追加到表尾（tail_pos）。
    """
    for key, st, _ in ordered_blocks:
        if key < new_key:
            return st
    return tail_pos


def sheet_tail_pos(blocks, rows_read):
    """估算表尾下一空行位置（0-based 行号，可直接 insert）。

    倒序布局下新块通常插在顶部（DATA_START），只有补录"比全表都旧"的时点才会
    走到表尾。若读取窗口已被占满（说明表比 READ_SPAN 还长），再单独定位一次真表尾。
    """
    tail = (blocks[-1][1] + BLOCK_ROWS) if blocks else DATA_START
    if rows_read >= READ_SPAN and rows_read and any(c.strip() for c in rows_read[-1]):
        r = mcp_call("get_sheet_info", {"file_id": FILE_ID})
        txt = r.get("text") or r.get("raw") or ""
        try:
            info = json.loads(r["content"][0]["text"]) if "content" in r else json.loads(txt)
        except Exception:  # noqa: BLE001
            info = {}
        for sh in (info.get("sheets") or info.get("sheet_list") or []):
            if str(sh.get("sheet_id")) == SHEET_ID:
                last = sh.get("row_count") or sh.get("used_range", {}).get("last_row")
                if last is not None:
                    tail = max(tail, int(last))
                break
    return tail


def main():
    now = datetime.now(BJ)
    today = now.strftime("%Y-%m-%d")
    now_min = now.hour * 60 + now.minute
    log(f"=== 银行询价 v3 云端运行 北京时间 {now:%Y-%m-%d %H:%M} ===")

    # 0. 确定本次应处理的时点
    slot_env = (os.environ.get("FX_SLOT") or "").strip()
    if slot_env:
        hh, mm = slot_env[:2], slot_env[2:]
        due_slots = [(int(hh), int(mm))]
        log(f"FX_SLOT={slot_env}，强制处理该时点（人工补录）。")
    else:
        if now.weekday() >= 5:
            log("周末休市，退出。")
            return 0
        due_slots = [s for s in SLOTS
                     if -WINDOW_EARLY <= now_min - (s[0] * 60 + s[1]) <= WINDOW_LATE]
        if not due_slots:
            log(f"北京 {now:%H:%M} 不在任何时点窗口内，正常退出（等待下一次 schedule 触发）。")
            return 0

    # 1. 读表（幂等判定）
    rows = retry(sheet_read)
    blocks = parse_blocks(rows)
    idx = {k: (st, rs) for k, st, rs in blocks}
    today_blocks = {k: v for k, v in idx.items() if k[0] == today}
    dates = sorted(d for d, _ in idx if d <= today)
    prev_day = next((d for d in reversed(dates) if d < today), None)
    log(f"表内已有 {len(blocks)} 块；今日 {sorted(t[1] for t in today_blocks) or '无'}；"
        f"上日({prev_day}) {sorted(t[1] for t in idx if t[0] == prev_day) if prev_day else '无'}")

    todo = []
    for s in due_slots:
        k = (today, s[0] * 60 + s[1])
        if k in idx and block_clean(idx[k][1]):
            log(f"{SLOT_NAMES[s]} 块已存在且数据完整，跳过。")
        else:
            todo.append(s)
    if not todo:
        log("本次应处理时点均已完整，无需写入，退出。")
        return 0

    # 2. 抓数（含节假日判定）
    try:
        pub_date, boc_usd, boc_eur = retry(fetch_boc)
    except Exception as e:  # noqa: BLE001
        log(f"[异常] 中行抓取失败: {e!r}")
        pub_date, boc_usd, boc_eur = None, None, None
    if pub_date and pub_date != today:
        log(f"中行牌价发布日期为 {pub_date}（非今日），判定休市，退出。")
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

    def bank_rows_of(blk):
        return {r[3].strip(): r for r in blk}

    def block_by_time(target_day, target_min):
        v = idx.get((target_day, target_min))
        return bank_rows_of(v[1]) if v else None

    def prev_block_rows(slot_name):
        """变动列基准：今日更早时点块 > 上一日最后一块。"""
        si = SLOT_INDEX[slot_name]
        for j in range(si - 1, -1, -1):
            r_ = block_by_time(today, to_min(SLOT_ORDER[j]))
            if r_:
                return r_
        if prev_day:
            cand = sorted(t[1] for t in idx if t[0] == prev_day)
            if cand:
                return block_by_time(prev_day, cand[-1])
        return None

    def disc_source():
        """优惠点来源：今日最新一块 > 上一日最后一块。"""
        if today_blocks:
            return bank_rows_of(today_blocks[max(today_blocks, key=lambda t: t[1])][1])
        if prev_day:
            cand = sorted(t[1] for t in idx if t[0] == prev_day)
            if cand:
                return block_by_time(prev_day, cand[-1])
        return None

    written = []
    for slot in todo:
        slot_name = SLOT_NAMES[slot]
        slot_min = slot[0] * 60 + slot[1]
        new_key = (today, slot_min)
        delta = now_min - slot_min
        tag = "窗口" if delta <= 20 else "补漏"
        prev_slot_rows = prev_block_rows(slot_name)
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

        # 公司名称列从来源块复制
        for r in new_rows:
            if r[3] in (disc_src or {}):
                r[2] = disc_src[r[3]][2]

        if os.environ.get("FX_DRYRUN") == "1":
            log(f"【DRYRUN】{slot_name}（{tag}）逐行:")
            for s_, r_ in zip(summary, new_rows):
                log(f"  {s_}  | {'|'.join(r_)}")
            continue

        # 4. 写入：已有块→ 就地刷新；新块 → 倒序位置插入 BLOCK_ROWS 行后写入
        if new_key in idx:
            start = idx[new_key][0]
            action = "刷新"
            retry(lambda st=start, tx=_csv(new_rows): sheet_write(st, 0, tx))
        else:
            pos = insert_pos(new_key, blocks, sheet_tail_pos(blocks, len(rows)))
            retry(lambda p=pos: sheet_insert_rows(p, BLOCK_ROWS))
            start = pos
            action = "新写入(倒序插入)"
            retry(lambda st=start, tx=_csv(new_rows): sheet_write(st, 0, tx))
        log(f"{slot_name}（{tag}）{action}完成：第 {start + 1} 行起 8 行")
        for s_ in summary:
            log("  " + s_)
        written.append((slot_name, start))
        # 同步内存态：插入后其下方所有块行号下移 BLOCK_ROWS，再加入新块并按行号排序
        # （表中块顺序 == 时间倒序 == 行号升序）
        blocks = [(k, st + (BLOCK_ROWS if st >= start else 0), rs) for k, st, rs in blocks]
        blocks.append((new_key, start, new_rows))
        blocks.sort(key=lambda x: x[1])
        idx = {k: (st, rs) for k, st, rs in blocks}
        today_blocks = {k: v for k, v in idx.items() if k[0] == today}

    if os.environ.get("FX_DRYRUN") == "1" or not written:
        return 0

    # 5. 回读校验（失败→非零退出码，GitHub 会邮件提醒）
    try:
        rows2 = retry(sheet_read)
    except QuotaExceeded as e:
        log(f"[校验跳过] {e}")
        return 0
    blocks2 = parse_blocks(rows2)
    idx2 = {k: rs for k, _, rs in blocks2}
    fails = []
    for name, _start in written:
        k = (today, to_min(name))
        blk = idx2.get(k)
        if not blk:
            fails.append(name)
            log(f"[校验失败] {name} 未回读到块")
            continue
        ok = sum(1 for r in blk if r[0] == today and r[1] == name)
        log(f"回读校验 {name}: {ok}/8 行匹配")
        if ok < 8:
            fails.append(name)
    if prices["note"]:
        log("异常源: " + json.dumps(prices["note"], ensure_ascii=False))
    log(f"文档: https://docs.qq.com/sheet/{FILE_ID}")
    if fails:
        log(f"校验失败时点: {fails}，请人工检查！")
        return 1
    return 0


def _csv(rows):
    return "\n".join(",".join("" if x in ("", None) else str(x) for x in r) for r in rows)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except QuotaExceeded as e:
        # 配额打爆：安静退出，等下一棒（schedule 每15分钟/保活每30分钟）自然恢复
        log(f"[配额受限] {e}")
        sys.exit(0)
    except Exception as e:  # noqa: BLE001
        log(f"[异常] {e!r}")
        sys.exit(1)
