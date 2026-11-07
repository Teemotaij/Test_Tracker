"""Tracker: public market data, recorded on a schedule. Standard library only.

Every endpoint read here is public and needs no account; nothing here authenticates or places an order. Order books are not kept by
any of these venues, so they have to be recorded as they stand; trades are recorded as they print.

One job = one venue (Novig is split into shards by event, so each shard reads from its own machine). Each pass writes ONE file:
    data/<UTC date>/<job>/<HHMMSS>.jsonl.gz      one JSON record per line:
        t     event | list | book | trade | odds | sports | pass
        k     the record's key (event id, market id, ticker, market slug, token id, game id)
        seen  when this pass read it (UTC, seconds)
        d     the venue's own JSON, unchanged
    event / list / book / odds / sports: written only when different from the last one this job wrote for the same key (state in the
    Actions cache), so an absent record means "read, unchanged". trade: every print not written before. pass: what the pass did (calls,
    rate-limit waits, errors, what was left unread for lack of time), so a gap can be told from a quiet market.
    status/<job>.json                            the last pass record, overwritten.

Books: a market starting within FRESH_H hours is read every pass; an earlier one when its listing (best prices, volume) changed, or
at least every STALE_MIN minutes. Markets nearest their start are read first, so a pass that runs out of time leaves the far ones.

    python3 record.py --job novig --shard 0 --shards 4
    python3 record.py --job kalshi | pmus | poly | espn
    python3 record.py --job novig_daily              # the venue's own daily trade files, NCAAB rows only
"""
import argparse, csv, datetime as dt, gzip, hashlib, io, json, os, re, sys, time, urllib.error, urllib.parse, urllib.request

NOVIG = "https://api.novig.com"
NOVIG_DAILY = "https://data.novig.com/reporting/trade-data"
KALSHI = "https://api.elections.kalshi.com/trade-api/v2"
KALSHI_SERIES = ("KXNCAAMBGAME", "KXNCAAMBSPREAD", "KXNCAAMBTOTAL", "KXNCAAMB1HSPREAD", "KXNCAAMB1HTOTAL", "KXNCAAMBTEAMTOTAL",
                 "KXNCAAMBWINMARGIN")
PMUS = "https://gateway.polymarket.us"
GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"
PDATA = "https://data-api.polymarket.com"
POLY_SERIES = 10470
ESPN_SITE = "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball"
ESPN_CORE = "https://sports.core.api.espn.com/v2/sports/basketball/leagues/mens-college-basketball"
LEAGUE = "NCAAB"

FRESH_H = 12.0
STALE_MIN = 60.0
PRUNE_DAYS = 4
PAUSE = {"api.novig.com": 0.45, "api.elections.kalshi.com": 0.07, "gateway.polymarket.us": 0.06, "gamma-api.polymarket.com": 0.05,
         "clob.polymarket.com": 0.05, "data-api.polymarket.com": 0.08, "site.api.espn.com": 0.03, "sports.core.api.espn.com": 0.03,
         "data.novig.com": 0.2}
NOW = dt.datetime.now(dt.timezone.utc)
STAMP = NOW.strftime("%Y-%m-%dT%H:%M:%SZ")


def ts_now():
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def hours_to(when):
    """Hours from now to an ISO time or epoch ms; None if unknown."""
    if when in (None, ""):
        return None
    try:
        if isinstance(when, (int, float)) or str(when).isdigit():
            t = dt.datetime.fromtimestamp(int(when) / 1000, dt.timezone.utc)
        else:
            s = re.sub(r"(\.\d{6})\d+", r"\1", str(when).strip().replace("Z", "+00:00").replace(" ", "T", 1))
            s = s + ":00" if re.search(r"[+-]\d{2}$", s) else s
            t = dt.datetime.fromisoformat(s)
            if t.tzinfo is None:
                t = t.replace(tzinfo=dt.timezone.utc)
        return (t - dt.datetime.now(dt.timezone.utc)).total_seconds() / 3600
    except Exception:
        return None


def sig(obj):
    return hashlib.md5(json.dumps(obj, sort_keys=True, separators=(",", ":")).encode()).hexdigest()[:12]


class Rec:
    """One pass: HTTP with a per-host pause that backs off on 429, the change-only writer, the state, the pass record."""

    def __init__(self, job, out, state_path, budget_s):
        self.job, self.out, self.state_path, self.budget = job, out, state_path, budget_s
        self.t0 = time.time()
        self.rows, self.calls, self.waits, self.errors = [], {}, {}, []
        self.pause = dict(PAUSE)
        self.last = {}
        self.counts = {}
        self.deferred = {}
        self.notes = {}
        try:
            with open(state_path) as f:
                self.state = json.load(f)
        except Exception:
            self.state = {}
        for k in ("sig", "full", "vol", "tr", "touch"):
            self.state.setdefault(k, {})

    def left(self, reserve=0.0):
        return time.time() - self.t0 < self.budget - reserve

    def defer(self, what, n=1):
        self.deferred[what] = self.deferred.get(what, 0) + n

    def count(self, what, n=1):
        self.counts[what] = self.counts.get(what, 0) + n

    def get(self, url, body=None, tries=5, raw=False):
        host = urllib.parse.urlparse(url).netloc
        data = json.dumps(body).encode() if body is not None else None
        for attempt in range(tries):
            gap = self.pause.get(host, 0.05) - (time.time() - self.last.get(host, 0))
            if gap > 0:
                time.sleep(gap)
            self.last[host] = time.time()
            self.calls[host] = self.calls.get(host, 0) + 1
            try:
                r = urllib.request.Request(url, data=data, headers={"Accept": "application/json", "User-Agent": "tracker/1",
                                                                    **({"Content-Type": "application/json"} if data else {})})
                with urllib.request.urlopen(r, timeout=40) as resp:
                    return resp if raw else json.load(resp)
            except urllib.error.HTTPError as e:
                if e.code == 429:
                    self.waits[host] = self.waits.get(host, 0) + 1
                    self.pause[host] = min(self.pause.get(host, 0.05) * 1.3 + 0.05, 2.0)
                    ra = e.headers.get("Retry-After") if e.headers else None
                    time.sleep(min(float(ra) if ra and ra.replace(".", "", 1).isdigit() else 3.0 * (attempt + 1), 30))
                    continue
                if e.code in (500, 502, 503, 504) and attempt < tries - 1:
                    time.sleep(2.0 * (attempt + 1))
                    continue
                raise
            except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
                if attempt < tries - 1:
                    time.sleep(2.0 * (attempt + 1))
                    continue
                raise
        raise RuntimeError(f"{host}: still rate-limited after {tries} tries")

    def err(self, where, e):
        self.count("errors")
        if len(self.errors) < 20:
            self.errors.append(f"{where}: {type(e).__name__}: {str(e)[:200]}")

    def emit(self, t, k, d, change_only=True, sig_of=None):
        """Write a record; with change_only, only when it differs from the last one written for (t, k)."""
        key = f"{t}:{k}"
        self.state["touch"][key] = int(time.time())
        if change_only:
            s = sig(d if sig_of is None else sig_of)
            if self.state["sig"].get(key) == s:
                self.count(f"{t}_same")
                return False
            self.state["sig"][key] = s
        self.rows.append({"t": t, "k": str(k), "seen": ts_now(), "d": d})
        self.count(t)
        return True

    def due(self, key, h, changed):
        """Is a full book due for this market?"""
        last = self.state["full"].get(key)
        if last is None or changed or (h is not None and h <= FRESH_H):
            return True
        return time.time() - last >= STALE_MIN * 60

    def booked(self, key):
        self.state["full"][key] = int(time.time())

    def finish(self):
        cut = time.time() - PRUNE_DAYS * 86400
        for key, t in list(self.state["touch"].items()):
            if t < cut:
                self.state["touch"].pop(key, None)
                for part in ("sig", "full", "vol", "tr"):
                    self.state[part].pop(key, None)
                    self.state[part].pop(key.split(":", 1)[-1], None)
        p = dict(job=self.job, started=STAMP, seconds=round(time.time() - self.t0, 1), budget_s=self.budget, calls=self.calls,
                 rate_limit_waits=self.waits, final_pause=self.pause, written=self.counts, deferred=self.deferred, notes=self.notes,
                 errors=self.errors)
        self.rows.append({"t": "pass", "k": self.job, "seen": ts_now(), "d": p})
        day = NOW.strftime("%Y-%m-%d")
        path = os.path.join(self.out, "data", day, self.job, NOW.strftime("%H%M%S") + ".jsonl.gz")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with gzip.open(path, "wt", encoding="utf-8") as f:
            for r in self.rows:
                f.write(json.dumps(r, separators=(",", ":")) + "\n")
        os.makedirs(os.path.join(self.out, "status"), exist_ok=True)
        with open(os.path.join(self.out, "status", f"{self.job}.json"), "w") as f:
            json.dump(p, f, indent=1, sort_keys=True)
        os.makedirs(os.path.dirname(os.path.abspath(self.state_path)), exist_ok=True)
        with open(self.state_path, "w") as f:
            json.dump(self.state, f, separators=(",", ":"))
        print(json.dumps(p, indent=1, sort_keys=True))


# ------------------------------------------------------------------ Novig
def shard_of(key, shards):
    return int(hashlib.md5(str(key).encode()).hexdigest(), 16) % shards


def novig_pages(r, path, params, key="items"):
    out, after = [], None
    for _ in range(50):
        q = dict(params, **({"after": after} if after else {}))
        d = r.get(f"{NOVIG}{path}?{urllib.parse.urlencode(q)}")
        items = d.get(key) or []
        out += items
        after = d.get("next")
        if not after or not items:
            break
    return out


def trade_id(t):
    return str(t.get("tradeId") or t.get("trade_id") or t.get("id") or sig(t))


def trade_ms(t):
    v = t.get("ts") or t.get("createdTs") or t.get("timestamp") or t.get("created_time")
    try:
        return int(v) if str(v).isdigit() else int(dt.datetime.fromisoformat(str(v).replace("Z", "+00:00")).timestamp() * 1000)
    except Exception:
        return 0


def new_trades(r, key, pages_iter, cap_pages):
    """Walk a newest-first tape page by page until a print we already wrote (or older than the last one); write the new ones."""
    st = r.state["tr"].get(key) or {}
    known, last_ms = set(st.get("ids") or []), int(st.get("ms") or 0)
    got, n_pages = [], 0
    for items in pages_iter:
        n_pages += 1
        stop = False
        for t in items:
            tid, ms = trade_id(t), trade_ms(t)
            if tid in known or (last_ms and ms and ms < last_ms):
                stop = True
                continue
            got.append(t)
        if stop or n_pages >= cap_pages:
            if not stop and n_pages >= cap_pages:
                r.defer("trade_pages_capped")
            break
    seen_ids = set()
    for t in sorted(got, key=trade_ms):
        tid = trade_id(t)
        if tid in seen_ids:
            continue
        seen_ids.add(tid)
        r.emit("trade", key, t, change_only=False)
    allms = [trade_ms(t) for t in got] + [last_ms]
    ids = (list(known) + [trade_id(t) for t in got])[-300:]
    r.state["tr"][key] = {"ms": max(allms), "ids": ids}
    return len(got)


def job_novig(r, shard, shards):
    events = novig_pages(r, "/v3/public/catalog/events", dict(league=LEAGUE, status="OPEN_PREGAME", limit=200))
    mine = [e for e in events if shard_of(e.get("eventId"), shards) == shard]
    r.notes.update(events_listed=len(events), events_mine=len(mine))
    mine.sort(key=lambda e: e.get("startsTs") or 0)
    books, trades = [], []
    for e in mine:
        r.emit("event", e["eventId"], e)
    for e in mine:                                                  # 1) every market's listing: the cheap top of book for all of them
        if not r.left(reserve=60):
            r.defer("events_unlisted")
            continue
        h = hours_to(e.get("startsTs"))
        try:
            ms = novig_pages(r, "/v3/public/catalog/markets", dict(league=LEAGUE, event=e["eventId"], limit=200))
        except Exception as ex:
            r.err(f"markets {e['eventId']}", ex)
            continue
        for m in ms:
            mid = m.get("marketId")
            if not mid:
                continue
            m["_eventId"] = e["eventId"]
            changed = r.emit("list", mid, m)
            r.count(f"markets_{m.get('marketType', '?')}")
            if r.due(f"book:{mid}", h, changed):
                books.append((h if h is not None else 1e9, mid))
            vol = m.get("volume")
            if vol not in (None, 0, "0") and r.state["vol"].get(mid) != vol:
                trades.append((h if h is not None else 1e9, mid, vol))
    depth = [100]
    for _, mid in sorted(books):                                     # 2) full books, nearest start first
        if not r.left(reserve=30):
            r.defer("books")
            continue
        try:
            try:
                b = r.get(f"{NOVIG}/v3/public/catalog/markets/{mid}/book?depth={depth[0]}")
            except urllib.error.HTTPError as ex:
                if ex.code != 400 or depth[0] <= 20:
                    raise
                depth[0] = 50 if depth[0] == 100 else 20
                b = r.get(f"{NOVIG}/v3/public/catalog/markets/{mid}/book?depth={depth[0]}")
            r.emit("book", mid, b)
            r.booked(f"book:{mid}")
        except Exception as ex:
            r.err(f"book {mid}", ex)
    r.notes["book_depth"] = depth[0]
    for _, mid, vol in sorted(trades):                               # 3) the tape of every market whose volume moved
        if not r.left(reserve=10):
            r.defer("tapes")
            continue

        def pages(mid=mid):
            after = None
            while True:
                q = dict(limit=200, **({"after": after} if after else {}))
                d = r.get(f"{NOVIG}/v3/public/catalog/markets/{mid}/trades?{urllib.parse.urlencode(q)}")
                yield d.get("items") or []
                after = d.get("next")
                if not after or not d.get("items"):
                    return
        try:
            new_trades(r, mid, pages(), cap_pages=60)
            r.state["vol"][mid] = vol
        except Exception as ex:
            r.err(f"trades {mid}", ex)


def job_novig_daily(r):
    """The venue's own daily trade files (every trade, all sports), NCAAB rows only, for recent days not yet done."""
    done = set(r.state.setdefault("daily_done", []))
    try:
        idx = r.get(f"{NOVIG_DAILY}/index.json")
        r.emit("daily_index", "index", idx)
    except Exception as ex:
        r.err("index", ex)
        idx = None
    days = [(NOW - dt.timedelta(days=k)).strftime("%Y-%m-%d") for k in range(1, 6)]
    for day in sorted(days):
        if day in done or not r.left(reserve=120):
            continue
        try:
            mk_rows, mk_hdr, ids = [], None, set()
            resp = r.get(f"{NOVIG_DAILY}/{day}/markets.csv", raw=True)
            rd = csv.reader(io.TextIOWrapper(resp, encoding="utf-8", newline=""))
            mk_hdr = next(rd)
            id_cols = [i for i, c in enumerate(mk_hdr) if "market" in c.lower() and "id" in c.lower()]
            for row in rd:
                if any(LEAGUE in c for c in row):
                    mk_rows.append(row)
                    ids.update(row[i] for i in id_cols if i < len(row))
            tr_rows, tr_hdr = [], None
            resp = r.get(f"{NOVIG_DAILY}/{day}/trades.csv", raw=True)
            rd = csv.reader(io.TextIOWrapper(resp, encoding="utf-8", newline=""))
            tr_hdr = next(rd)
            n_all = 0
            for row in rd:
                n_all += 1
                if any(c == LEAGUE or c in ids for c in row):
                    tr_rows.append(row)
            base = os.path.join(r.out, "data", "novig_daily", day)
            os.makedirs(base, exist_ok=True)
            for name, hdr, rows in (("markets", mk_hdr, mk_rows), ("trades", tr_hdr, tr_rows)):
                with gzip.open(os.path.join(base, f"{name}_{LEAGUE.lower()}.csv.gz"), "wt", encoding="utf-8", newline="") as f:
                    w = csv.writer(f)
                    w.writerow(hdr)
                    w.writerows(rows)
            r.notes[day] = dict(markets=len(mk_rows), trades=len(tr_rows), trades_all_sports=n_all, market_cols=mk_hdr, trade_cols=tr_hdr)
            done.add(day)
        except urllib.error.HTTPError as ex:
            if ex.code in (403, 404):
                r.notes[day] = f"not published yet ({ex.code})"
            else:
                r.err(f"daily {day}", ex)
        except Exception as ex:
            r.err(f"daily {day}", ex)
    r.state["daily_done"] = sorted(done)[-30:]


# ------------------------------------------------------------------ Kalshi
def kalshi_vol(m):
    for k in ("volume_fp", "volume"):
        if m.get(k) not in (None, ""):
            return m.get(k)
    return None


def job_kalshi(r):
    markets = []
    for s in KALSHI_SERIES:
        cur = None
        for _ in range(40):
            q = dict(series_ticker=s, status="open", limit=1000, **({"cursor": cur} if cur else {}))
            try:
                d = r.get(f"{KALSHI}/markets?{urllib.parse.urlencode(q)}")
            except Exception as ex:
                r.err(f"markets {s}", ex)
                break
            markets += d.get("markets") or []
            cur = d.get("cursor")
            if not cur or not d.get("markets"):
                break
    r.notes["markets_open"] = len(markets)
    books, trades = [], []
    for m in markets:
        t = m.get("ticker")
        if not t:
            continue
        changed = r.emit("list", t, m)
        r.count("markets_" + t.split("-")[0])
        end = m.get("expected_expiration_time") or m.get("close_time")
        h = hours_to(end)
        h = None if h is None else h - 2.5                            # a game's markets are expected to expire ~2.5 h after the tip
        if r.due(f"book:{t}", h, changed):
            books.append((h if h is not None else 1e9, t))
        v = kalshi_vol(m)
        if v not in (None, 0, "0", "0.00") and r.state["vol"].get(t) != v:
            trades.append((h if h is not None else 1e9, t, v))
    for _, t in sorted(books):
        if not r.left(reserve=40):
            r.defer("books")
            continue
        try:
            r.emit("book", t, r.get(f"{KALSHI}/markets/{t}/orderbook"))
            r.booked(f"book:{t}")
        except Exception as ex:
            r.err(f"orderbook {t}", ex)
    for _, t, v in sorted(trades):
        if not r.left(reserve=10):
            r.defer("tapes")
            continue
        st = r.state["tr"].get(t) or {}
        min_ts = max(int(st.get("ms") or 0) // 1000 - 5, 0)

        def pages(t=t, min_ts=min_ts):
            cur = None
            while True:
                q = dict(ticker=t, limit=1000, **({"min_ts": min_ts} if min_ts else {}), **({"cursor": cur} if cur else {}))
                d = r.get(f"{KALSHI}/markets/trades?{urllib.parse.urlencode(q)}")
                yield d.get("trades") or []
                cur = d.get("cursor")
                if not cur or not d.get("trades"):
                    return
        try:
            new_trades(r, t, pages(), cap_pages=30)
            r.state["vol"][t] = v
        except Exception as ex:
            r.err(f"trades {t}", ex)


# ------------------------------------------------------------------ Polymarket US
def find_leagues(obj, out):
    """Every league/sport slug in /v2/sports that names men's college basketball."""
    if isinstance(obj, dict):
        text = " ".join(str(obj.get(k, "")) for k in ("slug", "name", "title", "label", "shortName", "description")).lower()
        slug = obj.get("slug") or obj.get("leagueSlug")
        if slug and re.search(r"ncaab|\bcbb\b|college basketball|ncaa basketball|ncaam", text) and not re.search(r"wom|ncaaw|wcbb|wbb", text):
            out.add(str(slug))
        for v in obj.values():
            find_leagues(v, out)
    elif isinstance(obj, list):
        for v in obj:
            find_leagues(v, out)
    return out


def job_pmus(r):
    sports = r.get(f"{PMUS}/v2/sports")
    r.emit("sports", "sports", sports)
    leagues = sorted(find_leagues(sports, set()))
    r.notes["leagues"] = leagues
    books = []
    for lg in leagues:
        off = 0
        while off < 3000:
            try:
                d = r.get(f"{PMUS}/v2/leagues/{lg}/events?" + urllib.parse.urlencode(dict(limit=100, offset=off)))
            except Exception as ex:
                r.err(f"events {lg}", ex)
                break
            evs = d.get("events") or []
            for e in evs:
                if e.get("ended") or e.get("closed"):
                    continue
                ek = e.get("slug") or e.get("id")
                r.emit("event", ek, {k: v for k, v in e.items() if k != "markets"})
                h = hours_to(e.get("startTime"))
                for m in e.get("markets") or []:
                    if m.get("closed") or not m.get("slug"):
                        continue
                    m["_event"] = ek
                    changed = r.emit("list", m["slug"], m)
                    r.count("markets_" + str(m.get("marketType", "?")))
                    hh = h if h is not None else hours_to(m.get("gameStartTime"))
                    if r.due(f"book:{m['slug']}", hh, changed):
                        books.append((hh if hh is not None else 1e9, m["slug"]))
            off += len(evs)
            if len(evs) < 100:
                break
    for _, s in sorted(books):
        if not r.left(reserve=10):
            r.defer("books")
            continue
        try:
            r.emit("book", s, r.get(f"{PMUS}/v1/markets/{s}/book"))
            r.booked(f"book:{s}")
        except Exception as ex:
            r.err(f"book {s}", ex)


# ------------------------------------------------------------------ Polymarket (international)
def job_poly(r):
    events = []
    for off in range(0, 3000, 100):
        try:
            d = r.get(f"{GAMMA}/events?" + urllib.parse.urlencode(dict(series_id=POLY_SERIES, closed="false", active="true", limit=100,
                                                                      offset=off)))
        except Exception as ex:
            r.err("gamma events", ex)
            break
        if not isinstance(d, list) or not d:
            break
        events += d
        if len(d) < 100:
            break
    r.notes["events"] = len(events)
    tokens, trades = [], []
    for e in events:
        r.emit("event", e.get("id"), {k: v for k, v in e.items() if k != "markets"})
        for m in e.get("markets") or []:
            cid = m.get("conditionId")
            if not cid or m.get("closed"):
                continue
            m["_event"] = e.get("id")
            changed = r.emit("list", cid, m)
            h = hours_to(m.get("gameStartTime") or e.get("startDate"))
            try:
                toks = json.loads(m.get("clobTokenIds") or "[]")
            except Exception:
                toks = []
            for tok in toks:
                if r.due(f"book:{tok}", h, changed):
                    tokens.append((h if h is not None else 1e9, tok))
            v = m.get("volumeNum", m.get("volume"))
            if v not in (None, 0, "0") and r.state["vol"].get(cid) != v:
                trades.append((h if h is not None else 1e9, cid, v))
    tokens.sort()
    for i in range(0, len(tokens), 100):
        if not r.left(reserve=20):
            r.defer("books", len(tokens) - i)
            break
        batch = [t for _, t in tokens[i:i + 100]]
        try:
            res = r.get(f"{CLOB}/books", body=[{"token_id": t} for t in batch])
            for b in res or []:
                tok = b.get("asset_id")
                if not tok:
                    continue
                r.emit("book", tok, b, sig_of={"bids": b.get("bids"), "asks": b.get("asks")})
                r.booked(f"book:{tok}")
        except Exception as ex:
            r.err("clob books", ex)
    for _, cid, v in sorted(trades):
        if not r.left(reserve=10):
            r.defer("tapes")
            continue

        def pages(cid=cid):
            for off in range(0, 5000, 500):
                d = r.get(f"{PDATA}/trades?" + urllib.parse.urlencode(dict(market=cid, limit=500, offset=off)))
                yield d if isinstance(d, list) else []
                if not isinstance(d, list) or len(d) < 500:
                    return
        try:
            new_trades(r, cid, pages(), cap_pages=10)
            r.state["vol"][cid] = v
        except Exception as ex:
            r.err(f"trades {cid}", ex)


# ------------------------------------------------------------------ DraftKings via ESPN (the reference line)
def job_espn(r):
    from zoneinfo import ZoneInfo
    today = dt.datetime.now(ZoneInfo("America/New_York")).date()
    games = []
    for k in range(-1, 4):
        day = (today + dt.timedelta(days=k)).strftime("%Y%m%d")
        try:
            d = r.get(f"{ESPN_SITE}/scoreboard?" + urllib.parse.urlencode(dict(dates=day, groups=50, limit=500)))
        except Exception as ex:
            r.err(f"scoreboard {day}", ex)
            continue
        for e in d.get("events") or []:
            games.append(e)
    r.notes["games"] = len(games)
    for e in games:
        st = ((e.get("status") or {}).get("type") or {}).get("state")
        r.emit("event", e.get("id"), e, sig_of={"date": e.get("date"), "status": st,
                                                "comp": [(c.get("id"), c.get("score")) for c in ((e.get("competitions") or [{}])[0].get("competitors") or [])],
                                                "odds": (e.get("competitions") or [{}])[0].get("odds")})
    pre = [e for e in games if ((e.get("status") or {}).get("type") or {}).get("state") == "pre"]
    pre.sort(key=lambda e: e.get("date") or "")
    for e in pre:
        if not r.left(reserve=10):
            r.defer("odds")
            continue
        gid = e.get("id")
        try:
            d = r.get(f"{ESPN_CORE}/events/{gid}/competitions/{gid}/odds")
            r.emit("odds", gid, d)
        except Exception as ex:
            r.err(f"odds {gid}", ex)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--job", required=True, choices=["novig", "novig_daily", "kalshi", "pmus", "poly", "espn"])
    ap.add_argument("--shard", type=int, default=0)
    ap.add_argument("--shards", type=int, default=1)
    ap.add_argument("--out", default="out")
    ap.add_argument("--state", default="state/state.json")
    ap.add_argument("--budget", type=float, default=780.0)
    a = ap.parse_args()
    name = a.job if a.shards == 1 else f"{a.job}-{a.shard}"
    r = Rec(name, a.out, a.state, a.budget)
    try:
        if a.job == "novig":
            job_novig(r, a.shard, a.shards)
        elif a.job == "novig_daily":
            job_novig_daily(r)
        else:
            {"kalshi": job_kalshi, "pmus": job_pmus, "poly": job_poly, "espn": job_espn}[a.job](r)
    except Exception as ex:
        r.err("pass", ex)
    r.finish()


if __name__ == "__main__":
    main()
