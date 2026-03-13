"""
바이낸스 실시간 체결 → 노션 자동 기록
현물(Spot) + 선물(Futures) 동시 감지
"""

import asyncio
import json
import os
import requests
import websockets
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

load_dotenv()

BINANCE_API_KEY    = os.getenv("BINANCE_API_KEY")
NOTION_TOKEN       = os.getenv("NOTION_TOKEN")
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID")

SPOT_REST    = "https://api.binance.com"
FUTURES_REST = "https://fapi.binance.com"
SPOT_WS      = "wss://stream.binance.com:9443/ws"
FUTURES_WS   = "wss://fstream.binance.com/ws"

KST = timezone(timedelta(hours=9))

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

# ── 노션 기록 ───────────────────────────────────────────────

def record_to_notion(trade: dict):
    headers = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json",
    }
    total = round(float(trade["qty"]) * float(trade["price"]), 4)

    body = {
        "parent": {"database_id": NOTION_DATABASE_ID},
        "properties": {
            "거래쌍":    {"title":  [{"text": {"content": trade["symbol"]}}]},
            "방향":      {"select": {"name": trade["side"]}},
            "수량":      {"number": float(trade["qty"])},
            "가격":      {"number": float(trade["price"])},
            "총액(USDT)":{"number": total},
            "계정":      {"select": {"name": trade["account"]}},
            "체결시간":   {"date":   {"start": trade["time"]}},
        },
    }

    r = requests.post(
        "https://api.notion.com/v1/pages",
        headers=headers,
        json=body,
        timeout=10
    )
    if r.status_code == 200:
        print(f"✅ 기록 완료 | {trade['account']:12s} | "
              f"{trade['symbol']:12s} | {trade['side']:6s} | "
              f"{trade['qty']} @ {trade['price']} = {total} USDT")
    else:
        print(f"❌ 노션 오류 {r.status_code}: {r.text}")

# ── 메시지 파싱 ─────────────────────────────────────────────

def parse_spot(data: dict) -> dict | None:
    """executionReport — 완전 체결(FILLED)만 처리"""
    if data.get("e") != "executionReport":
        return None
    if data.get("X") != "FILLED":
        return None
    return {
        "symbol":  data["s"],
        "side":    "매수" if data["S"] == "BUY" else "매도",
        "qty":     data["l"],   # 마지막 체결 수량
        "price":   data["L"],   # 마지막 체결 가격
        "account": "현물(Spot)",
        "time":    datetime.fromtimestamp(data["T"] / 1000, tz=KST).isoformat(),
    }


def parse_futures(data: dict) -> dict | None:
    """ORDER_TRADE_UPDATE — 완전 체결(FILLED)만 처리"""
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

                # 30분마다 listenKey 갱신 (만료 방지)
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
                            record_to_notion(trade)
                finally:
                    ka_task.cancel()

        except Exception as e:
            print(f"⚠️  {account} 오류: {e}")
            print(f"   5초 후 재연결...")
            await asyncio.sleep(5)

# ── 진입점 ──────────────────────────────────────────────────

async def main():
    print("=" * 50)
    print("  바이낸스 → 노션 자동 기록 시작")
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
