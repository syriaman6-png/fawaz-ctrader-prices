import asyncio
import json
import os
import threading
import websockets
from ctrader_open_api import Client, Protobuf, TcpProtocol, EndPoints
from ctrader_open_api.messages.OpenApiMessages_pb2 import *
from ctrader_open_api.messages.OpenApiModelMessages_pb2 import *
from twisted.internet import reactor

CLIENT_ID = os.environ['CTRADER_CLIENT_ID']
CLIENT_SECRET = os.environ['CTRADER_CLIENT_SECRET']
ACCESS_TOKEN = os.environ['CTRADER_ACCESS_TOKEN']
ACCOUNT_ID = int(os.environ['CTRADER_ACCOUNT_ID'])

clients = set()
client = None
SYMBOL_MAP = {}
main_loop = None

async def ws_handler(websocket):
    clients.add(websocket)
    try:
        await websocket.wait_closed()
    finally:
        clients.remove(websocket)

def broadcast_sync(symbol, price):
    if main_loop and not main_loop.is_closed():
        asyncio.run_coroutine_threadsafe(broadcast(symbol, price), main_loop)

async def broadcast(symbol, price):
    if not clients: return
    msg = json.dumps({"type": "price.update", "data": {"symbol": symbol, "price": price, "timestamp": ""}})
    await asyncio.gather(*[c.send(msg) for c in clients], return_exceptions=True)

def on_connected(c):
    print("✅ Connected to cTrader TCP", flush=True)
    try:
        req = ProtoOAApplicationAuthReq()
        req.clientId = CLIENT_ID
        req.clientSecret = CLIENT_SECRET
        c.send(req).addCallbacks(on_app_auth, on_error)
    except Exception as e:
        print(f"❌ on_connected error: {e}", flush=True)

def on_app_auth(result):
    print("✅ App authenticated", flush=True)
    try:
        req = ProtoOAAccountAuthReq()
        req.ctidTraderAccountId = ACCOUNT_ID
        req.accessToken = ACCESS_TOKEN
        client.send(req).addCallbacks(on_account_auth, on_error)
    except Exception as e:
        print(f"❌ on_app_auth error: {e}", flush=True)

def on_account_auth(result):
    print("✅ Account authenticated", flush=True)
    try:
        req = ProtoOASymbolsListReq()
        req.ctidTraderAccountId = ACCOUNT_ID
        req.includeArchivedSymbols = True
        client.send(req).addCallbacks(on_symbols_list, on_error)
    except Exception as e:
        print(f"❌ on_account_auth error: {e}", flush=True)

def on_symbols_list(result):
    try:
        print(f"🔍 Response type: {type(result).__name__}", flush=True)
        
        # إذا كان رد خطأ
        if type(result).__name__ == "ProtoOAErrorRes":
            print(f"❌❌ ERROR CODE: {result.errorCode}", flush=True)
            print(f"❌❌ DESCRIPTION: {result.description}", flush=True)
            return
        
        # استخراج الرسالة الفعلية من الـ wrapper
        try:
            payload = Protobuf.extract(result)
            print(f"🔍 Extracted type: {type(payload).__name__}", flush=True)
        except Exception as e:
            print(f"⚠️ Extract failed: {e}, using result directly", flush=True)
            payload = result
        
        # إذا كان الرد المستخرج خطأ
        if type(payload).__name__ == "ProtoOAErrorRes":
            print(f"❌❌ ERROR CODE: {payload.errorCode}", flush=True)
            print(f"❌❌ DESCRIPTION: {payload.description}", flush=True)
            return
        
        # الحصول على قائمة الرموز
        symbols = getattr(payload, 'symbol', None)
        if symbols is None:
            print(f"❌ No 'symbol' attribute. Available: {[a for a in dir(payload) if not a.startswith('_')]}", flush=True)
            return
        
        print(f"✅ Got {len(symbols)} symbols", flush=True)
        
        targets = {"US30": None, "US100": None, "XAUUSD": None}
        for s in symbols:
            try:
                name = s.symbolName.upper() if hasattr(s, 'symbolName') else ""
                sid = s.symbolId if hasattr(s, 'symbolId') else None
                if not sid: continue
                if "US30" in name or "DOW" in name or "WS30" in name:
                    targets["US30"] = sid
                elif "US100" in name or "NAS" in name or "NDX" in name:
                    targets["US100"] = sid
                elif "XAU" in name or "GOLD" in name:
                    targets["XAUUSD"] = sid
            except Exception:
                continue
        
        print(f"🎯 Found targets: {targets}", flush=True)
        
        ids = [v for v in targets.values() if v]
        for name, sid in targets.items():
            if sid: SYMBOL_MAP[sid] = name
        
        if ids:
            req = ProtoOASubscribeSpotsReq()
            req.ctidTraderAccountId = ACCOUNT_ID
            req.symbolId.extend(ids)
            client.send(req)
            print(f"✅ Subscribed to symbols: {ids}", flush=True)
        else:
            print("❌ No matching symbols found!", flush=True)
    except Exception as e:
        print(f"❌ on_symbols_list error: {e}", flush=True)

def on_message(c, message):
    try:
        if message.payloadType == ProtoOASpotEvent().payloadType:
            event = Protobuf.extract(message)
            symbol = SYMBOL_MAP.get(event.symbolId, f"ID_{event.symbolId}")
            price = event.bid / (10 ** event.digits) if event.bid else 0
            if price:
                print(f"💰 {symbol}: {price}", flush=True)
                broadcast_sync(symbol, price)
    except Exception as e:
        print(f"❌ on_message error: {e}", flush=True)

def on_error(failure):
    print(f"❌ Twisted error: {failure}", flush=True)

def start_twisted():
    global client
    try:
        print("🔌 Starting cTrader client (DEMO host)...", flush=True)
        # استخدام سيرفر Demo (لأن الحساب Demo)
        client = Client(EndPoints.PROTOBUF_DEMO_HOST, EndPoints.PROTOBUF_PORT, TcpProtocol)
        client.setConnectedCallback(on_connected)
        client.setMessageReceivedCallback(on_message)
        client.startService()
        reactor.run(installSignalHandlers=False)
    except Exception as e:
        print(f"❌ Twisted failed: {e}", flush=True)

async def main():
    global main_loop
    main_loop = asyncio.get_event_loop()
    port = int(os.environ.get("PORT", 8080))
    print(f"🚀 Starting WebSocket on port {port}", flush=True)
    t = threading.Thread(target=start_twisted, daemon=True)
    t.start()
    async with websockets.serve(ws_handler, "0.0.0.0", port):
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
