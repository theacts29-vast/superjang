#!/usr/bin/env python3
import os, json, datetime as dt
import feedparser
import yfinance as yf
import requests

KST = dt.timezone(dt.timedelta(hours=9))
TODAY = dt.datetime.now(KST)

FRED = os.getenv('FRED_API_KEY', '')
FMP  = os.getenv('FMP_API_KEY', '')

def pct(a,b):
    try:
        if a is None or b is None or b == 0: return None
        return (a-b)/b*100.0
    except Exception:
        return None

def fetch_quote(ticker):
    t = yf.Ticker(ticker)
    p = t.history(period='2d')
    if p.empty:
        return {"close": None, "changePct": None}
    close = float(p['Close'].iloc[-1])
    prev  = float(p['Close'].iloc[-2]) if len(p)>=2 else None
    return {"close": close, "changePct": pct(close, prev)}

snapshot = {
    'sp500':     fetch_quote('^GSPC'),
    'nasdaq100': fetch_quote('^NDX'),
    'vix':       fetch_quote('^VIX'),
}

def fetch_top20():
    url = f"https://financialmodelingprep.com/api/v3/stock-screener?limit=200&exchange=NYSE,NASDAQ&apikey={FMP}"
    r = requests.get(url, timeout=30); r.raise_for_status()
    data = r.json()
    data = [x for x in data if x.get('marketCap')]
    data.sort(key=lambda x: x['marketCap'], reverse=True)
    top = []
    for x in data[:20]:
        sym = x['symbol']
        name = x.get('companyName') or sym
        q = fetch_quote(sym)
        top.append({
            'symbol': sym,
            'name': name,
            'price': q['close'],
            'marketCap': x['marketCap'],
            'changePct': q['changePct'] if q['changePct'] is not None else 0.0,
        })
    return top

ASSETS = {'WTI':'CL=F','Gold':'GC=F','Copper':'HG=F','NatGas':'NG=F','Corn':'ZC=F'}
FX = {'DXY':'DX-Y.NYB','USDKRW':'KRW=X'}

def fetch_asset_map(symbols):
    out = {}
    for k,t in symbols.items():
        q = fetch_quote(t)
        out[k] = {'value': q['close'], 'changePct': q['changePct'] or 0.0}
    return out

assets = fetch_asset_map(ASSETS)
fx     = fetch_asset_map(FX)

def fred_series(series):
    url = f"https://api.stlouisfed.org/fred/series/observations?series_id={series}&api_key={FRED}&file_type=json&observation_start={(TODAY.date()-dt.timedelta(days=14))}"
    r = requests.get(url, timeout=30); r.raise_for_status()
    obs = r.json().get('observations', [])
    if not obs: return None, None
    vals = [float(x['value']) for x in obs if x['value'] != '.']
    if not vals: return None, None
    return vals[-1], (vals[-1]-vals[-2]) if len(vals)>=2 else None

r10, d10 = fred_series('DGS10')
r2,  d2  = fred_series('DGS2')
ff,  dff = fred_series('DFF')

fedwatch = {"next": "Hold bias (est.)"}

RSS = [
    ('Reuters','https://feeds.reuters.com/reuters/businessNews'),
    ('CNBC','https://www.cnbc.com/id/100003114/device/rss/rss.html'),
    ('WSJ','https://feeds.a.dj.com/rss/RSSMarketsMain.xml'),
]
news = []
for name,url in RSS:
    try:
        f = feedparser.parse(url)
        for e in f.entries[:5]:
            news.append({'source':name,'title':e.title,'link':e.link,'published':e.get('published','')[:25]})
    except Exception:
        pass

DATA = {
    'updated_at': TODAY.strftime('%Y-%m-%d %H:%M'),
    'us': {'snapshot': snapshot, 'top20': fetch_top20()},
    'assets': assets,
    'fx': fx,
    'rates': {'t10y': r10, 'delta10y': d10 or 0.0, 't2y': r2, 'delta2y': d2 or 0.0, 'fedfunds': ff},
    'fedwatch': fedwatch,
    'news': news,
}

with open(os.path.join(os.path.dirname(__file__),'data.json'),'w',encoding='utf-8') as f:
    json.dump(DATA, f, ensure_ascii=False, indent=2)

print('data.json updated at', DATA['updated_at'])
