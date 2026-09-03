import json,re,time,logging,os
from datetime import datetime,timezone
import requests
import threading
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
GAMMA='https://gamma-api.polymarket.com'; CLOB=os.getenv('CLOB_API','https://clob.polymarket.com').rstrip('/')
ASSET_SLUGS={'BTC':'btc','ETH':'eth','SOL':'sol','BNB':'bnb'}
if os.getenv('V17_SIX_ASSET_MODE','false').lower() in ('1','true','yes','on'):
    ASSET_SLUGS.update({'DOGE':'doge','HYPE':'hyperliquid'})
LOG=logging.getLogger('market')
SESSION=requests.Session(); SESSION.mount('https://',HTTPAdapter(max_retries=Retry(total=3,connect=3,read=3,backoff_factor=.4,status_forcelist=[429,500,502,503,504],allowed_methods=frozenset(['GET'])),pool_connections=20,pool_maxsize=20)); SESSION.headers.update({'User-Agent':'polymarket-self-trader-paper/4.0'})
_BOOK_LOCAL = threading.local()
def _book_session():
    session = getattr(_BOOK_LOCAL, 'session', None)
    if session is None:
        session = requests.Session()
        session.mount('https://', HTTPAdapter(max_retries=Retry(total=3, connect=3, read=3, backoff_factor=.4, status_forcelist=[429,500,502,503,504], allowed_methods=frozenset(['GET'])), pool_connections=4, pool_maxsize=4))
        session.headers.update({'User-Agent':'polymarket-self-trader-paper/4.0'})
        _BOOK_LOCAL.session = session
    return session
def _as_bool(v, default=True):
    if isinstance(v, bool):
        return v
    if v is None:
        return default
    if isinstance(v, (int, float)):
        return bool(v)
    text = str(v).strip().lower()
    if text in ('1', 'true', 'yes', 'on'):
        return True
    if text in ('0', 'false', 'no', 'off', ''):
        return False
    return default

def _parse(v):
    if isinstance(v,list):return v
    if isinstance(v,str):
        try:x=json.loads(v);return x if isinstance(x,list) else []
        except Exception:return []
    return []
def _rows(r):
    d=r.json()
    if isinstance(d,list):return d
    if isinstance(d,dict):
        for k in ('data','markets','results'):
            if isinstance(d.get(k),list):return d[k]
    return []
def _normalize(m):
    slug=str(m.get('slug') or ''); mm=re.search(r'^(btc|eth|sol|bnb|doge|hyperliquid)-updown-5m-(\d{9,12})$',slug.lower())
    if not mm:return None
    asset=mm.group(1).upper(); start=float(mm.group(2)); end=start+300; tokens=_parse(m.get('clobTokenIds') or m.get('clob_token_ids')); outcomes=_parse(m.get('outcomes'))
    if len(tokens)<2:return None
    mapping={str(o).strip().lower():str(t) for o,t in zip(outcomes,tokens)}; up=mapping.get('up'); down=mapping.get('down')
    if not up or not down:up,down=str(tokens[0]),str(tokens[1])
    condition=str(m.get('conditionId') or m.get('condition_id') or m.get('id') or '')
    if not condition:return None
    return {'id':str(m.get('id') or condition),'condition':condition,'market':str(m.get('question') or m.get('title') or slug),'slug':slug,'asset':asset,'up':up,'down':down,'start_ts':start,'end_ts':end,'raw':m,'accepting_orders':_as_bool(m.get('acceptingOrders'), True),'enable_order_book':_as_bool(m.get('enableOrderBook'), True)}
def _get_market_by_slug(slug):
    r=SESSION.get(f'{GAMMA}/markets/slug/{slug}',timeout=10)
    if r.status_code==200:
        d=r.json()
        if isinstance(d,dict) and d.get('slug'):return d
    if r.status_code not in (404,400):r.raise_for_status()
    r=SESSION.get(f'{GAMMA}/markets',params={'slug':slug},timeout=10)
    if r.status_code==200:
        rows=_rows(r)
        if rows:return rows[0]
    if r.status_code not in (404,400):r.raise_for_status()
    return None
def discover(now=None,lookahead=600):
    now=time.time() if now is None else float(now); base=int(now//300)*300; out={}; diagnostics=[]
    for asset,prefix in ASSET_SLUGS.items():
        found=False
        for start in (base-300,base,base+300,base+600):
            slug=f'{prefix}-updown-5m-{start}'
            try:raw=_get_market_by_slug(slug)
            except Exception as e:diagnostics.append(f'{asset}:{slug}:ERROR:{type(e).__name__}:{e}');continue
            if not raw:diagnostics.append(f'{asset}:{slug}:MISS');continue
            x=_normalize(raw)
            if not x:diagnostics.append(f'{asset}:{slug}:INVALID');continue
            if x['end_ts']<now-5 or x['start_ts']>now+lookahead:continue
            if not x['enable_order_book']:diagnostics.append(f'{asset}:{slug}:NO_ORDERBOOK');continue
            out[x['condition']]=x;found=True;diagnostics.append(f'{asset}:{slug}:FOUND');break
        if not found:diagnostics.append(f'{asset}:NO_CURRENT_MARKET')
    if not out:
        offset=0
        while offset<2000:
            try:r=SESSION.get(f'{GAMMA}/markets',params={'active':'true','closed':'false','limit':500,'offset':offset,'order':'endDate','ascending':'true'},timeout=15);r.raise_for_status();rows=_rows(r)
            except Exception as e:diagnostics.append(f'GLOBAL_FALLBACK:ERROR:{type(e).__name__}:{e}');break
            if not rows:break
            for raw in rows:
                x=_normalize(raw)
                if not x or x['end_ts']<now-5 or x['start_ts']>now+lookahead or not x['enable_order_book']:continue
                out[x['condition']]=x
            if len(rows)<500:break
            offset+=500
    if out:LOG.info('DISCOVERY | discovered=%d | assets=%s | %s',len(out),','.join(sorted({x['asset'] for x in out.values()})),' | '.join(diagnostics))
    else:LOG.warning('DISCOVERY | discovered=0 | %s',' | '.join(diagnostics))
    return list(out.values())
def book(token):
    r=_book_session().get(f'{CLOB}/book',params={'token_id':token},timeout=5);r.raise_for_status();d=r.json()
    def levels(rows,side):
        vals=[]
        for x in rows or []:
            try:price=float(x.get('price') if isinstance(x,dict) else x[0]);size=float(x.get('size') if isinstance(x,dict) else x[1]);
            except Exception:continue
            if 0 < price <= 1 and size > 0:vals.append((price,size))
        if not vals:return None,None
        return (max(vals,key=lambda z:z[0]) if side=='bid' else min(vals,key=lambda z:z[0]))
    b=levels(d.get('bids'),'bid');a=levels(d.get('asks'),'ask');return (b[0] if b else None),(a[0] if a else None),(b[1] if b else 0.),(a[1] if a else 0.)
def resolve(market):
    ident=market.get('id') if isinstance(market,dict) else str(market);slug=market.get('slug') if isinstance(market,dict) else None;raw=None
    if ident:
        r=SESSION.get(f'{GAMMA}/markets/{ident}',timeout=10)
        if r.status_code==200:raw=r.json()
    if raw is None and slug:
        r=SESSION.get(f'{GAMMA}/markets',params={'slug':slug},timeout=10);r.raise_for_status();rows=_rows(r)
        if rows:raw=rows[0]
    if raw is None:return None,None,'NOT_FOUND'
    token,outcome=_winner(raw)
    if token:return token,outcome,'WINNER_FIELD_OR_PRICE'
    if _as_bool(raw.get('closed'), False) or _as_bool(raw.get('resolved'), False):return None,None,'CLOSED_UNRESOLVED'
    return None,None,'PENDING'
def _winner(m):
    objs=m.get('tokens')
    if isinstance(objs,list):
        for o in objs:
            if isinstance(o,dict) and o.get('winner') is True:
                tok=o.get('token_id') or o.get('tokenId');out=o.get('outcome') or ''
                if tok:return str(tok),str(out)
    tokens=_parse(m.get('clobTokenIds') or m.get('clob_token_ids'));outcomes=_parse(m.get('outcomes'));prices=_parse(m.get('outcomePrices'))
    if len(tokens)==len(outcomes)==len(prices) and tokens:
        for i,p in enumerate(prices):
            try:
                if float(p)>=.999999:return str(tokens[i]),str(outcomes[i])
            except Exception:pass
    return None,None


def book_snapshot(token):
    """Return the full CLOB snapshot needed by V18 queue-depth simulation.

    `depth_at_target` is taken from the displayed bid size at the exact target
    price. If the target is not currently displayed, it is zero and the first
    qualifying trade print is the queue proxy trigger.
    """
    token = str(token)
    r = SESSION.get(f"{CLOB}/book", params={"token_id": token}, timeout=5)
    r.raise_for_status()
    data = r.json()
    bids = []
    asks = []
    for side_name, target in (("bids", bids), ("asks", asks)):
        for level in data.get(side_name) or []:
            try:
                price=float(level.get("price")); size=float(level.get("size"))
                if 0 < price <= 1 and size >= 0:
                    target.append((price,size))
            except (TypeError, ValueError, AttributeError):
                continue
    bids.sort(reverse=True); asks.sort()
    best_bid=bids[0][0] if bids else None
    best_ask=asks[0][0] if asks else None
    bid_depth=bids[0][1] if bids else 0.0
    ask_depth=asks[0][1] if asks else 0.0
    return {
        "best_bid": best_bid, "best_ask": best_ask,
        "bid_depth": bid_depth, "ask_depth": ask_depth,
        "bids": bids, "asks": asks,
        "last_trade_price": data.get("last_trade_price"),
    }
