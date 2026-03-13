"""
바이낸스 실시간 체결 → Telegram 알림 + Google Sheets 기록
현물(Spot) + 선물(Futures) 동시 감지
"""

import asyncio
import json
import os
import requests
import gspread
import websockets
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials

load_dotenv()

BINANCE_API_KEY    = os.getenv("BINANCE_API_KEY")
TELEGRAM_TOKEN     = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID")
GOOGLE_SHEET_NAME  = os.getenv("GOOGLE_SHEET_NAME", "바이낸스 매매기록")
GOOGLE_CREDS_FILE  = os.getenv("GOOGLE_CREDS_FILE", "google_credentials.json")

SPOT_REST    = "https://api.binance.com"
FUTURES_REST = "https://fapi.binance.com"
SPOT_WS      = "wss://stream.binance.com:9443/ws"
FUTURES_WS   = "wss://fstream.binance.com/ws"

KST = timezone(timedelta(hours=9))

# ── Google Sheets 초기화 ────────────────────────────────────

SHEET_HEADERS = ["체결시간", "계정", "거래쌍", "방향", "수량", "가격", "총액(USDT)"]

def init_sheet():
    creds = Credentials.from_service_account_file(
        GOOGLE_CREDS_FILE,
        scopes=["https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"]
    )
    gc = gspread.authorize(creds)
    try:
        sh = gc.open(GOOGLE_SHEET_NAME)
    except gspread.SpreadsheetNotFound:
        sh = gc.create(GOOGLE_SHEET_NAME)
        print(f"📄 새 시트 생성됨: {GOOGLE_SHEET_NAME}")

    ws = sh.sheet1
    # 헤더가 없으면 추가
    if ws.row_values(1) != SHEET_HEADERS:
        ws.insert_row(SHEET_HEADERS, 1)
        # 헤더 굵게 (선택)
        ws.format("A1:G1", {"textFormat": {"bold": True}})
        print("✅ 시트 헤더 설정 완료")
    return ws

try:
    sheet = init_sheet()
    print("✅ Google Sheets 연결 완료")
except Exception as e:
    print(f"❌ Google Sheets 연결 실패: {e}")
    sheet = None

# ── Telegram 전송 ───────────────────────────────────────────

def send_telegram(trade: dict):
    total = float(trade["qty"]) * float(trade["price"])
    emoji = "🟢" if "매수" in trade["side"] else "🔴"
    text = (
        f"{emoji} *{trade['account']}* 체결\n"
        f"━━━━━━━━━━━━━━\n"
        f"🪙 거래쌍: `{trade['symbol']}`\n"
        f"📌 방향: *{trade['side']}*\n"
        f"📦 수량: `{trade['qty']}`\n"
        f"💵 가격: `{trade['price']} USDT`\n"
        f"💰 총액: `{round(total, 2)} USDT`\n"
        f"🕐 시간: {trade['time'][11:19]}"
    )
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"},
            timeout=10
        )
        if r.status_code == 200:
            print(f"📨 텔레그램 전송 완료")
        else:
            print(f"❌ 텔레그램 오류: {r.text}")
    except Exception as e:
        print(f"❌ 텔레그램 예외: {e}")

# ── Google Sheets 기록 ──────────────────────────────────────

def record_to_sheet(trade: dict):
    if not sheet:
        return
    total = round(float(trade["qty"]) * float(trade["price"]), 4)
    row = [
        trade["time"][:19].replace("T", " "),  # 2024-01-01 14:30:00
        trade["account"],
        trade["symbol"],
        trade["side"],
        float(trade["qty"]),
        float(trade["price"]),
        total,
    ]
    try:
        sheet.append_row(row, value_input_option="USER_ENTERED")
        print(f"📊 시트 기록 완료: {trade['symbol']} {trade['side']} {total} USDT")
    except Exception as e:
        print(f"❌ 시트 기록 오류: {e}")

# ── 체결 처리 ───────────────────────────────────────────────

def process_trade(trade: dict):
    print(f"\n🔔 체결 감지 | {trade['account']} | {trade['symbol']} {trade['side']}")
    send_telegram(trade)
    record_to_sheet(trade)

# ── 바이낸스 ListenKey ──────────────────────────────────────

def get_listen_key(rest_base, is_futures=False):
    endpoint = "/fapi/v1/listenKey" if is_futures else "/api/v3/userDataStream"
    r = requests.post(
        f"{rest_base}{endpoint}",
        headers={"X-MBX-APIKEY": BINANCE_API_KEY},
        timeout=10
    )
    r.raise_for_status()
    return r.json()["listenKey"]


def keepalive(rest_base, listen_key, is_futures=False):
    endpoint = "/fapi/v1/listenKey" if is_futures else "/api/v3/userDataStream"
    requests.put(
        f"{rest_base}{endpoint}",
        headers={"X-MBX-APIKEY": BINANCE_API_KEY},
        params={"listenKey": listen_key},
        timeout=10
    )

# ── 메시지 파싱 ─────────────────────────────────────────────

def parse_spot(data: dict):
    if data.get("e") != "executionReport" or data.get("X") != "FILLED":
        return None
    return {
        "symbol":  data["s"],
        "side":    "매수" if data["S"] == "BUY" else "매도",
        "qty":     data["l"],
        "price":   data["L"],
        "account": "현물(Spot)",
        "time":    datetime.fromtimestamp(data["T"] / 1000, tz=KST).isoformat(),
    }


def parse_futures(data: dict):
    if data.get("e") != "ORDER_TRADE_UPDATE":
        return None
    o = data["o"]
    if o.get("X") != "FILLED":
        return None
    return {
        "symbol":  o["s"],
        "side":    "매수(롱)" if o["S"] == "BUY" else "매도(숏)",
        "qty":     o["l"],
        "price":   o["L"],
        "account": "선물(Futures)",
        "time":    datetime.fromtimestamp(data["T"] / 1000, tz=KST).isoformat(),
    }

# ── 웹소켓 스트림 ───────────────────────────────────────────

async def run_stream(ws_base, rest_base, is_futures, parser):
    account = "선물(Futures)" if is_futures else "현물(Spot)"
    while True:
        try:
            listen_key = get_listen_key(rest_base, is_futures)
            print(f"🔗 {account} 연결 중...")

            async with websockets.connect(f"{ws_base}/{listen_key}") as ws:
                print(f"✅ {account} 스트림 연결됨")

                async def keepalive_loop():
                    while True:
                        await asyncio.sleep(30 * 60)
                        keepalive(rest_base, listen_key, is_futures)
                        print(f"🔄 {account} listenKey 갱신")

                ka_task = asyncio.create_task(keepalive_loop())
                try:
                    async for raw in ws:
                        data = json.loads(raw)
                        trade = parser(data)
                        if trade:
                            process_trade(trade)
                finally:
                    ka_task.cancel()

        except Exception as e:
            print(f"⚠️  {account} 오류: {e}")
            print(f"   5초 후 재연결...")
            await asyncio.sleep(5)

# ── 진입점 ──────────────────────────────────────────────────

async def main():
    print("=" * 50)
    print("  바이낸스 → Telegram + Google Sheets")
    print("  종료: Ctrl + C")
    print("=" * 50)

    await asyncio.gather(
        run_stream(SPOT_WS,    SPOT_REST,    False, parse_spot),
        run_stream(FUTURES_WS, FUTURES_REST, True,  parse_futures),
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 종료됨")
