from __future__ import annotations
import json, logging, threading, time
from queue import Queue, Full, Empty
from typing import Callable, Iterable
import websocket

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
log = logging.getLogger("market_feed")

class MarketTradeFeed:
    """Resilient public CLOB trade-print feed.

    Only `last_trade_price` events are forwarded. The feed reconnects with
    exponential backoff and supports dynamic token subscriptions.
    """
    def __init__(self, on_trade: Callable[[str,float,float,float,dict],None],
                 ping_seconds: float = 10.0):
        self.on_trade=on_trade; self.ping_seconds=ping_seconds
        self._tokens=set(); self._lock=threading.RLock(); self._stop=threading.Event()
        self._ws=None; self._thread=threading.Thread(target=self._run,daemon=True,name="clob-trade-feed")
        self._worker=threading.Thread(target=self._dispatch_loop,daemon=True,name="clob-trade-dispatch")
        self._queue=Queue(maxsize=max(1000, int(__import__("os").getenv("TRADE_FEED_QUEUE_MAX","50000"))))

    def start(self):
        if not self._worker.is_alive(): self._worker.start()
        if not self._thread.is_alive(): self._thread.start()

    def stop(self):
        self._stop.set()
        with self._lock:
            ws=self._ws
        if ws:
            try: ws.close()
            except Exception: pass

    def set_tokens(self, tokens: Iterable[str]):
        new={str(x) for x in tokens if x}
        with self._lock:
            added=new-self._tokens; self._tokens=new; ws=self._ws
        if ws and added:
            try:
                ws.send(json.dumps({"assets_ids": sorted(added), "operation":"subscribe"}))
            except Exception as exc: log.warning("WS subscribe failed: %s",exc)

    def _snapshot_tokens(self):
        with self._lock: return sorted(self._tokens)

    def _run(self):
        backoff=1.0
        while not self._stop.is_set():
            tokens=self._snapshot_tokens()
            if not tokens:
                self._stop.wait(1.0); continue
            try:
                ws=websocket.WebSocketApp(
                    WS_URL, on_open=self._on_open, on_message=self._on_message,
                    on_error=self._on_error, on_close=self._on_close,
                )
                with self._lock: self._ws=ws
                ws.run_forever(ping_interval=0, ping_timeout=None)
                backoff=min(30.0,backoff*1.8)
            except Exception as exc:
                log.warning("WS loop error: %s",exc)
                backoff=min(30.0,backoff*1.8)
            finally:
                with self._lock: self._ws=None
            self._stop.wait(backoff)

    def _on_open(self,ws):
        tokens=self._snapshot_tokens()
        if tokens:
            ws.send(json.dumps({"assets_ids":tokens,"type":"market"}))
        threading.Thread(target=self._heartbeat,args=(ws,),daemon=True).start()
        log.info("CLOB trade feed connected | tokens=%d",len(tokens))

    def _heartbeat(self,ws):
        while not self._stop.is_set():
            if self._stop.wait(self.ping_seconds): return
            try: ws.send("PING")
            except Exception: return

    def _on_message(self,ws,message):
        if message=="PONG": return
        try: obj=json.loads(message)
        except Exception: return
        events=obj if isinstance(obj,list) else [obj]
        for event in events:
            if not isinstance(event,dict) or event.get("event_type")!="last_trade_price": continue
            try:
                token=str(event["asset_id"]); price=float(event["price"]); size=float(event["size"])
                ts=float(event.get("timestamp",0))/1000.0 if event.get("timestamp") else time.time()
                # Unit-test/direct-use compatibility: before start(), there is
                # no dispatcher thread, so deliver synchronously. In production
                # start() is called first and all socket callbacks are queued.
                if not self._worker.is_alive():
                    try:
                        self.on_trade(token, price, size, ts, event)
                    except Exception:
                        log.exception("CLOB trade callback failed | token=%s price=%s size=%s", token, price, size)
                    continue
                try:
                    self._queue.put_nowait((token, price, size, ts, event))
                except Full:
                    # Never block the websocket reader on the application
                    # callback. A full queue means the consumer is unhealthy;
                    # reconnecting is preferable to letting the server close
                    # the socket with 1013/slow-consumer.
                    log.error("CLOB trade dispatch queue full | size=%d", self._queue.qsize())
                    try: ws.close()
                    except Exception: pass
                    return
            except (TypeError,ValueError,KeyError):
                continue


    def _dispatch_loop(self):
        while not self._stop.is_set():
            try:
                token, price, size, ts, event = self._queue.get(timeout=0.5)
            except Empty:
                continue
            try:
                self.on_trade(token, price, size, ts, event)
            except Exception:
                log.exception("CLOB trade callback failed | token=%s price=%s size=%s", token, price, size)
            finally:
                self._queue.task_done()

    def _on_error(self,ws,error): log.warning("CLOB trade feed error: %s",error)
    def _on_close(self,ws,code,msg): log.warning("CLOB trade feed closed | code=%s msg=%s",code,msg)
