"""
MD Meta Forge – FastAPI Backend
================================
* Auto-Refresh-Engine (APScheduler, alle REFRESH_HOURS Stunden + Webhook)
* SQLite-Cache für Meta-Decks, Tier-Liste, Banliste, Kartendaten
* Konsens-Deckbau aus Turnier-/Top-Decks, Budget-Filter (UR/SR-CP)
* Exakte (multivariate) hypergeometrische Hand-Analyse
* Playbook-Gerüst, Handtrap-Guidance, Deck Doctor
* Bild-Proxy mit Festplatten-Cache (YGOPRODeck verbietet Dauer-Hotlinking)

Start lokal:  uvicorn main:app --reload   (oder: python main.py)
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import re
import sqlite3
import statistics
import threading
import time
from collections import Counter, defaultdict
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import FileResponse, JSONResponse, Response
from pydantic import BaseModel, Field

# --------------------------------------------------------------------------- #
# Konfiguration (alles per Umgebungsvariable überschreibbar)
# --------------------------------------------------------------------------- #
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = Path(os.getenv("DATA_DIR", BASE_DIR / "data"))
IMG_DIR = DATA_DIR / "img"
DB_PATH = DATA_DIR / "cache.sqlite3"
COMBOS_PATH = Path(os.getenv("COMBOS_PATH", BASE_DIR / "combos.json"))

REFRESH_HOURS = float(os.getenv("REFRESH_HOURS", "6"))
REFRESH_TOKEN = os.getenv("REFRESH_TOKEN", "")          # Pflicht für den Webhook
META_WINDOW_DAYS = int(os.getenv("META_WINDOW_DAYS", "30"))
MDM_BASE = os.getenv("MDM_BASE", "https://www.masterduelmeta.com/api/v1").rstrip("/")
YGO_API = "https://db.ygoprodeck.com/api/v7/cardinfo.php"
YGO_IMG = "https://images.ygoprodeck.com/images/cards_small/{id}.jpg"
CONTACT = os.getenv("CONTACT_EMAIL", "unknown")
USER_AGENT = f"MDMetaForge/1.0 (hobby project; contact: {CONTACT})"

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
ANTHROPIC_MODEL = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-5-5")

DECK_SIZE = 40
EXTRA_MAX = 15
CRAFT_COST = 30  # Master Duel: 30 CP der jeweiligen Seltenheit pro Karte

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("mdforge")

# --------------------------------------------------------------------------- #
# Wissensbasis: Rollen-Seeds (Heuristik ergänzt den Rest automatisch)
# --------------------------------------------------------------------------- #
HANDTRAPS = {
    'Ash Blossom & Joyous Spring', 'Maxx "C"', 'Infinite Impermanence', 'Effect Veiler',
    'Ghost Belle & Haunted Mansion', 'Ghost Ogre & Snow Rabbit', 'Ghost Mourner & Moonlit Chill',
    'Droll & Lock Bird', 'Nibiru, the Primal Being', 'D.D. Crow', 'PSY-Framegear Gamma',
    'Dimension Shifter', 'Crossout Designator', 'Mulcharmy Fuwalos', 'Mulcharmy Purulia',
    'Mulcharmy Meowls', 'Contact "C" from the Cosmos', 'Retaliating "C"', 'Skull Meister',
    'Artifact Lancea', 'Bystial Magnamhut', 'Bystial Druiswurm', 'Fantastical Dragon Phantazmay',
    'Talents of the Fabled Lady', 'Gnomaterial',
}
BREAKERS = {
    'Lightning Storm', 'Dark Ruler No More', 'Evenly Matched', 'Forbidden Droplet',
    "Harpie's Feather Duster", 'Raigeki', 'Triple Tactics Talent', 'Triple Tactics Thrust',
    'Super Polymerization', 'Book of Eclipse', 'Cosmic Cyclone', 'Twin Twisters',
    'Heavy Storm Duster', 'Called by the Grave', 'Droplet', 'Kaiju Slumber',
    'Gozuma the Kaiju', 'Dogoran, the Mad Flame Kaiju', 'Gameciel, the Sea Turtle Kaiju',
    'Radian, the Multidimensional Kaiju', 'Kumongous, the Sticky String Kaiju',
    'Santa Claws', 'Lava Golem', 'Sphere Kuriboh', 'Ice Dragon\'s Prison',
}
DRAW_SPELLS = {'Pot of Prosperity', 'Pot of Desires', 'Pot of Extravagance', 'Upstart Goblin',
               'Pot of Duality', 'Card of Demise', 'Allure of Darkness', 'Into the Void'}
# Karten, die in der Handtrap-Guidance als "Schutz" zählen
PROTECTION = {'Called by the Grave', 'Crossout Designator', 'Triple Tactics Talent',
              'Forbidden Droplet', 'Cosmic Cyclone', 'Solemn Judgment', 'Lightning Storm'}

ROLE_ORDER = ["starter", "extender", "handtrap", "breaker", "engine", "draw", "tech", "extra"]
ROLE_LABEL = {
    "starter": "Starter", "extender": "Extender", "handtrap": "Handtrap",
    "breaker": "Board Breaker", "engine": "Engine/Brick", "draw": "Draw",
    "tech": "Tech", "extra": "Extra Deck",
}

# --------------------------------------------------------------------------- #
# SQLite-Cache
# --------------------------------------------------------------------------- #
_db_lock = threading.Lock()


def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    IMG_DIR.mkdir(parents=True, exist_ok=True)
    with _db_lock, db() as c:
        c.execute("CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT, updated REAL)")
        c.execute("CREATE TABLE IF NOT EXISTS cards (name TEXT PRIMARY KEY, id INTEGER, data TEXT)")
        c.execute("CREATE INDEX IF NOT EXISTS cards_id ON cards(id)")


def kv_get(key: str, default: Any = None) -> Any:
    with _db_lock, db() as c:
        row = c.execute("SELECT value FROM kv WHERE key=?", (key,)).fetchone()
    return json.loads(row[0]) if row else default


def kv_set(key: str, value: Any) -> None:
    with _db_lock, db() as c:
        c.execute("INSERT OR REPLACE INTO kv VALUES (?,?,?)", (key, json.dumps(value), time.time()))


def cards_get(names: list[str]) -> dict[str, dict]:
    out: dict[str, dict] = {}
    if not names:
        return out
    with _db_lock, db() as c:
        for i in range(0, len(names), 500):
            chunk = names[i:i + 500]
            q = f"SELECT name, data FROM cards WHERE name IN ({','.join('?' * len(chunk))})"
            for name, data in c.execute(q, chunk):
                out[name] = json.loads(data)
    return out


def cards_by_ids(ids: list[int]) -> dict[int, dict]:
    out: dict[int, dict] = {}
    with _db_lock, db() as c:
        for i in range(0, len(ids), 500):
            chunk = ids[i:i + 500]
            q = f"SELECT id, data FROM cards WHERE id IN ({','.join('?' * len(chunk))})"
            for cid, data in c.execute(q, chunk):
                out[cid] = json.loads(data)
    return out


def cards_put(cards: list[dict]) -> None:
    with _db_lock, db() as c:
        c.executemany("INSERT OR REPLACE INTO cards VALUES (?,?,?)",
                      [(x["name"], x["id"], json.dumps(x)) for x in cards])


def known_card_names() -> set[str]:
    with _db_lock, db() as c:
        return {r[0] for r in c.execute("SELECT name FROM cards")}


# --------------------------------------------------------------------------- #
# Normalisierung
# --------------------------------------------------------------------------- #
def norm_rarity(r: Any) -> str:
    s = str(r or "").strip().lower()
    if s in ("ur", "ultra rare", "ultra"):
        return "UR"
    if s in ("sr", "super rare", "super"):
        return "SR"
    if s in ("r", "rare"):
        return "R"
    if s in ("n", "common", "normal"):
        return "N"
    return "?"


def norm_ban(b: Any) -> int | None:
    """Maximal erlaubte Kopien laut Banliste (None = unbeschränkt)."""
    s = str(b or "").lower()
    if not s:
        return None
    if "forbid" in s or "banned" in s:
        return 0
    if "semi" in s or "2" in s:
        return 2
    if "limit" in s or "1" in s:
        return 1
    return None


def slim_card(raw: dict) -> dict:
    misc = (raw.get("misc_info") or [{}])[0]
    return {
        "id": int(raw.get("id") or 0),
        "name": raw.get("name", ""),
        "type": raw.get("type", ""),
        "frame": raw.get("frameType", ""),
        "desc": raw.get("desc", ""),
        "archetype": raw.get("archetype"),
        "race": raw.get("race"),
        "attribute": raw.get("attribute"),
        "level": raw.get("level") or raw.get("linkval"),
        "atk": raw.get("atk"),
        "def": raw.get("def"),
        "rarity": norm_rarity(misc.get("md_rarity")),
    }


def is_extra_card(card: dict) -> bool:
    return any(k in (card.get("frame") or "") for k in ("fusion", "synchro", "xyz", "link"))


# --------------------------------------------------------------------------- #
# HTTP-Clients mit Höflichkeits-Rate-Limit (YGOPRODeck: max. 20 req/s)
# --------------------------------------------------------------------------- #
_ygo_sem = asyncio.Semaphore(4)
_last_ygo_call = 0.0


async def ygo_get(client: httpx.AsyncClient, url: str, params: dict | None = None) -> httpx.Response:
    global _last_ygo_call
    async with _ygo_sem:
        wait = 0.12 - (time.monotonic() - _last_ygo_call)  # ≤ ~8 req/s, weit unter dem Limit
        if wait > 0:
            await asyncio.sleep(wait)
        _last_ygo_call = time.monotonic()
        return await client.get(url, params=params)


async def fetch_ygo_cards(client: httpx.AsyncClient, *, names: list[str] | None = None,
                          ids: list[int] | None = None) -> list[dict]:
    """Holt Kartendaten in Batches; bei 400 (unbekannter Name) wird der Batch halbiert."""
    found: list[dict] = []

    async def batch(key: str, values: list[str]) -> None:
        if not values:
            return
        r = await ygo_get(client, YGO_API, {key: "|".join(values) if key == "name" else ",".join(values),
                                            "misc": "yes"})
        if r.status_code == 200:
            found.extend(slim_card(x) for x in r.json().get("data", []))
        elif r.status_code == 400 and len(values) > 1:
            mid = len(values) // 2
            await batch(key, values[:mid])
            await batch(key, values[mid:])
        elif r.status_code != 400:
            log.warning("YGOPRODeck %s → HTTP %s", key, r.status_code)

    if names:
        for i in range(0, len(names), 40):
            await batch("name", names[i:i + 40])
    if ids:
        for i in range(0, len(ids), 40):
            await batch("id", [str(x) for x in ids[i:i + 40]])
    return found


# --------------------------------------------------------------------------- #
# Auto-Refresh-Engine
# --------------------------------------------------------------------------- #
_refresh_lock = asyncio.Lock()


def _entry_name(e: Any) -> tuple[str | None, int, dict]:
    """MDM-Deck-Eintrag → (Name, Anzahl, eingebettete Karteninfos)."""
    if not isinstance(e, dict):
        return None, 0, {}
    card = e.get("card") if isinstance(e.get("card"), dict) else e
    name = card.get("name") or e.get("name")
    amount = int(e.get("amount") or e.get("count") or 1)
    return name, amount, card


def normalize_decks(raw: list[dict], embedded: dict[str, dict]) -> list[dict]:
    decks = []
    for d in raw:
        if not isinstance(d, dict):
            continue
        dt = d.get("deckType")
        arche = (dt.get("name") if isinstance(dt, dict) else dt) or d.get("deckTypeName") or "Unbekannt"
        parts: dict[str, list[dict]] = {}
        for part in ("main", "extra", "side"):
            entries = []
            for e in d.get(part) or []:
                name, amount, card = _entry_name(e)
                if not name:
                    continue
                entries.append({"name": name, "amount": amount})
                info = embedded.setdefault(name, {})
                if card.get("rarity"):
                    info["rarity"] = norm_rarity(card.get("rarity"))
                if card.get("banStatus"):
                    info["ban"] = norm_ban(card.get("banStatus"))
            parts[part] = entries
        if not parts.get("main"):
            continue
        tt = d.get("tournamentType")
        decks.append({
            "archetype": arche,
            "created": d.get("created") or d.get("createdAt") or "",
            "event": (tt.get("name") if isinstance(tt, dict) else tt) or d.get("rankedType") or "",
            "author": (d.get("author") or {}).get("username") if isinstance(d.get("author"), dict) else d.get("author"),
            "url": d.get("url") or "",
            **parts,
        })
    return decks


def parse_date(s: str) -> datetime | None:
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except Exception:
        return None


async def refresh_meta(reason: str = "scheduler") -> dict:
    if _refresh_lock.locked():
        return {"ok": False, "message": "Refresh läuft bereits"}
    async with _refresh_lock:
        started = time.time()
        errors: list[str] = []
        headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
        async with httpx.AsyncClient(timeout=60, headers=headers, follow_redirects=True) as client:
            # 1) Top-Decks (Turnier + Ranked-Einsendungen)
            raw_decks: list[dict] = []
            for url in (f"{MDM_BASE}/top-decks?sort=-created&limit=400",
                        f"{MDM_BASE}/top-decks?limit=400",
                        f"{MDM_BASE}/top-decks?limit=0"):
                try:
                    r = await client.get(url)
                    if r.status_code == 200 and isinstance(r.json(), list) and r.json():
                        raw_decks = r.json()
                        break
                    errors.append(f"top-decks {r.status_code} @ {url}")
                except Exception as ex:  # noqa: BLE001
                    errors.append(f"top-decks: {ex!r}")
            embedded: dict[str, dict] = {}
            decks = normalize_decks(raw_decks, embedded)

            if not decks:
                status = kv_get("status", {}) or {}
                status.update({"ok": False, "last_attempt": now_iso(), "errors": errors[-5:]})
                kv_set("status", status)
                log.error("Refresh fehlgeschlagen: %s", errors)
                return status

            # Zeitfenster: letzte META_WINDOW_DAYS Tage (mindestens 60 neueste Decks)
            decks.sort(key=lambda d: d["created"], reverse=True)
            cutoff = datetime.now(timezone.utc) - timedelta(days=META_WINDOW_DAYS)
            recent = [d for d in decks if (parse_date(d["created"]) or cutoff) >= cutoff]
            decks = recent if len(recent) >= 60 else decks[:max(60, len(recent))]

            # 2) Tier-Liste: offizielle MDM-Tiers falls verfügbar + berechneter Meta-Anteil
            tiers: dict[str, Any] = {}
            try:
                r = await client.get(f"{MDM_BASE}/deck-types", params={"tier[$exists]": "true", "limit": 0})
                if r.status_code == 200 and isinstance(r.json(), list):
                    for t in r.json():
                        if isinstance(t, dict) and t.get("name") and t.get("tier") is not None:
                            tiers[t["name"]] = t.get("tier")
            except Exception as ex:  # noqa: BLE001
                errors.append(f"deck-types: {ex!r}")

            share = Counter(d["archetype"] for d in decks)
            tierlist = []
            for arche, n in share.most_common():
                tierlist.append({
                    "archetype": arche, "decks": n,
                    "share": round(n / len(decks), 4),
                    "tier": tiers.get(arche),
                    "events": sum(1 for d in decks if d["archetype"] == arche and d["event"]),
                })

            # 3) Banliste (MDM-Karten mit banStatus)
            banlist: dict[str, int] = {n: v["ban"] for n, v in embedded.items() if v.get("ban") is not None}
            try:
                r = await client.get(f"{MDM_BASE}/cards", params={"banStatus[$exists]": "true", "limit": 0})
                if r.status_code == 200 and isinstance(r.json(), list):
                    for c in r.json():
                        b = norm_ban(c.get("banStatus"))
                        if c.get("name") and b is not None:
                            banlist[c["name"]] = b
            except Exception as ex:  # noqa: BLE001
                errors.append(f"banlist: {ex!r}")

            # 4) Kartendaten (nur fehlende) von YGOPRODeck
            needed = {e["name"] for d in decks for p in ("main", "extra", "side") for e in d.get(p, [])}
            needed |= HANDTRAPS | BREAKERS | DRAW_SPELLS
            missing = sorted(needed - known_card_names())
            if missing:
                try:
                    fetched = await fetch_ygo_cards(client, names=missing)
                    for c in fetched:  # MDM-Seltenheit hat Vorrang (ist Master-Duel-spezifisch)
                        if embedded.get(c["name"], {}).get("rarity"):
                            c["rarity"] = embedded[c["name"]]["rarity"]
                    cards_put(fetched)
                except Exception as ex:  # noqa: BLE001
                    errors.append(f"ygoprodeck: {ex!r}")

        kv_set("decks", decks)
        kv_set("tierlist", tierlist)
        kv_set("banlist", banlist)
        status = {
            "ok": True, "last_update": now_iso(), "last_attempt": now_iso(), "reason": reason,
            "decks": len(decks), "archetypes": len(tierlist), "banlist_entries": len(banlist),
            "official_tiers": bool(tiers), "duration_s": round(time.time() - started, 1),
            "errors": errors[-5:], "window_days": META_WINDOW_DAYS,
        }
        kv_set("status", status)
        log.info("Meta-Refresh ok: %s Decks, %s Archetypen", len(decks), len(tierlist))
        return status


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# --------------------------------------------------------------------------- #
# Rollen-Klassifikation
# --------------------------------------------------------------------------- #
RE_SEARCH = re.compile(r"add (?:1|one|up to \w+)[^.]*?from your deck to your hand", re.I)
RE_SS_DECK = re.compile(r"special summon (?:1|one|up to \w+)[^.]*?from your deck", re.I)
RE_SEND_DECK = re.compile(r"send (?:1|one)[^.]*?from your deck to the gy", re.I)
RE_SELF_SS = re.compile(r"special summon this card from your (?:hand|gy)", re.I)
RE_BOARD_WIPE = re.compile(r"(destroy|banish|negate)[^.]*all[^.]*your opponent controls", re.I)


def classify(card: dict, main_archetype: str | None = None) -> str:
    name, desc = card.get("name", ""), card.get("desc", "") or ""
    if is_extra_card(card):
        return "extra"
    if name in HANDTRAPS:
        return "handtrap"
    if name in BREAKERS:
        return "breaker"
    if name in DRAW_SPELLS:
        return "draw"
    searches = bool(RE_SEARCH.search(desc) or RE_SS_DECK.search(desc) or RE_SEND_DECK.search(desc))
    self_ss = bool(RE_SELF_SS.search(desc))
    generic = not card.get("archetype")
    if generic and "discard this card" in desc.lower() and "quick effect" in desc.lower():
        return "handtrap"
    if generic and RE_BOARD_WIPE.search(desc):
        return "breaker"
    if searches:
        return "starter"
    if self_ss:
        return "extender"
    if card.get("archetype") and (not main_archetype or card["archetype"] in (main_archetype or "")):
        return "engine"
    return "engine" if not generic else "tech"


def role_of(card: dict, overrides: dict[str, str], arche: str | None = None) -> str:
    r = overrides.get(card.get("name", ""))
    return r if r in ROLE_LABEL else classify(card, arche)


# --------------------------------------------------------------------------- #
# Stochastik: exakte multivariate Hypergeometrie
# --------------------------------------------------------------------------- #
CATS = ("starter", "extender", "handtrap", "breaker", "other")


def to_cat(role: str) -> str:
    return role if role in CATS[:-1] else "other"


def hand_tier(c: dict[str, int]) -> str:
    s, e, h, b = c.get("starter", 0), c.get("extender", 0), c.get("handtrap", 0), c.get("breaker", 0)
    combo = s >= 1 or e >= 2
    if combo and (s + e) >= 2 and (h + b) >= 1:
        return "S"
    if combo:
        return "A"
    if h + b >= 2:
        return "B"
    return "Brick"


def hand_distribution(counts: dict[str, int], hand: int) -> list[tuple[dict[str, int], float]]:
    deck = sum(counts.values())
    if deck < hand:
        return []
    total = math.comb(deck, hand)
    keys = list(counts)
    out: list[tuple[dict[str, int], float]] = []

    def rec(i: int, left: int, acc: dict[str, int], ways: int) -> None:
        if i == len(keys) - 1:
            k = keys[i]
            if left <= counts[k]:
                acc2 = {**acc, k: left}
                out.append((acc2, ways * math.comb(counts[k], left) / total))
            return
        k = keys[i]
        for x in range(0, min(left, counts[k]) + 1):
            rec(i + 1, left - x, {**acc, k: x}, ways * math.comb(counts[k], x))

    rec(0, hand, {}, 1)
    return out


def analyze_counts(counts: dict[str, int], hand: int = 5) -> dict:
    counts = {k: max(0, int(counts.get(k, 0))) for k in CATS}
    dist = hand_distribution(counts, hand)
    tiers = Counter()
    p_combo = p_one = p_inter = 0.0
    for c, p in dist:
        tiers[hand_tier(c)] += p
        if c["starter"] >= 1 or c["extender"] >= 2:
            p_combo += p
        if c["starter"] >= 1:
            p_one += p
        if c["handtrap"] + c["breaker"] >= 1:
            p_inter += p
    return {
        "hand_size": hand, "deck_size": sum(counts.values()), "counts": counts,
        "p_one_card_starter": round(p_one, 4),
        "p_combo": round(p_combo, 4),           # 1-Karten-Starter ODER 2 Extender
        "p_interaction": round(p_inter, 4),
        "tiers": {t: round(tiers.get(t, 0.0), 4) for t in ("S", "A", "B", "Brick")},
    }


def min_starters(target: float, deck: int = DECK_SIZE, hand: int = 5) -> int:
    for k in range(0, deck + 1):
        if 1 - math.comb(deck - k, hand) / math.comb(deck, hand) >= target:
            return k
    return deck


def ratio_table(deck: int = DECK_SIZE) -> list[dict]:
    return [{
        "starters": k,
        "p5": round(1 - math.comb(deck - k, 5) / math.comb(deck, 5), 4),
        "p6": round(1 - math.comb(deck - k, 6) / math.comb(deck, 6), 4),
    } for k in range(0, 22)]


# --------------------------------------------------------------------------- #
# Meta-Statistik & Konsens-Deckbau
# --------------------------------------------------------------------------- #
def load_meta() -> tuple[list[dict], dict[str, int]]:
    decks = kv_get("decks", []) or []
    if not decks:
        raise HTTPException(503, "Noch keine Meta-Daten im Cache – Refresh läuft oder ist fehlgeschlagen.")
    return decks, kv_get("banlist", {}) or {}


def card_stats(decks: list[dict], part: str) -> dict[str, dict]:
    n = max(1, len(decks))
    total: Counter = Counter()
    present: Counter = Counter()
    copies: dict[str, list[int]] = defaultdict(list)
    for d in decks:
        seen: Counter = Counter()
        for e in d.get(part, []):
            seen[e["name"]] += e["amount"]
        for name, a in seen.items():
            total[name] += a
            present[name] += 1
            copies[name].append(a)
    return {name: {"expected": total[name] / n, "inclusion": present[name] / n,
                   "median": statistics.median(copies[name])} for name in total}


def cap_for(name: str, banlist: dict[str, int]) -> int:
    b = banlist.get(name)
    return 3 if b is None else min(3, b)


def allocate(stats: dict[str, dict], slots: int, banlist: dict[str, int]) -> dict[str, int]:
    """Largest-Remainder-Verfahren auf den erwarteten Kopien je Turnierliste."""
    alloc = {n: min(cap_for(n, banlist), int(math.floor(s["expected"] + 1e-9))) for n, s in stats.items()}
    size = sum(alloc.values())
    while size > slots:
        n = min((n for n, a in alloc.items() if a > 0), key=lambda n: stats[n]["expected"] / alloc[n])
        alloc[n] -= 1
        size -= 1
    while size < slots:
        cands = [n for n in stats if alloc.get(n, 0) < cap_for(n, banlist)]
        if not cands:
            break
        n = max(cands, key=lambda n: (stats[n]["expected"] - alloc[n], stats[n]["inclusion"]))
        if stats[n]["expected"] <= 0:
            break
        alloc[n] += 1
        size += 1
    return {n: a for n, a in alloc.items() if a > 0}


def cp_usage(main: dict[str, int], extra: dict[str, int], info: dict[str, dict]) -> dict[str, int]:
    used = Counter()
    for part in (main, extra):
        for n, c in part.items():
            used[info.get(n, {}).get("rarity", "?")] += c * CRAFT_COST
    return {r: used.get(r, 0) for r in ("UR", "SR", "R", "N")}


class BuildRequest(BaseModel):
    archetype: str
    max_ur_cp: int | None = Field(None, ge=0, description="Max. UR-CP für das ganze Deck")
    max_sr_cp: int | None = Field(None, ge=0)
    target: float = Field(0.85, ge=0.5, le=0.99)
    hand_size: int = Field(5, ge=5, le=6)
    overrides: dict[str, str] = Field(default_factory=dict)


def build_deck(req: BuildRequest) -> dict:
    decks, banlist = load_meta()
    arch_decks = [d for d in decks if d["archetype"].lower() == req.archetype.lower()]
    if not arch_decks:
        raise HTTPException(404, f"Keine Meta-Listen für '{req.archetype}' im Zeitfenster.")
    arche = arch_decks[0]["archetype"]
    main_stats = card_stats(arch_decks, "main")
    extra_stats = card_stats(arch_decks, "extra")
    global_stats = card_stats(decks, "main")
    names = set(main_stats) | set(extra_stats) | set(global_stats) | HANDTRAPS | BREAKERS
    info = cards_get(sorted(names))

    main = allocate(main_stats, DECK_SIZE, banlist)
    avg_extra = statistics.mean(sum(e["amount"] for e in d.get("extra", [])) for d in arch_decks)
    extra = allocate(extra_stats, min(EXTRA_MAX, round(avg_extra)), banlist)

    def role(n: str) -> str:
        return role_of(info.get(n, {"name": n}), req.overrides, arche)

    def value(n: str) -> float:
        bonus = {"starter": 2.0, "extender": 1.0, "handtrap": 0.6}.get(role(n), 0.0)
        return main_stats.get(n, {}).get("expected", 0) + bonus

    changes: list[dict] = []
    budgets = {"UR": req.max_ur_cp, "SR": req.max_sr_cp}

    def over_budget() -> str | None:
        used = cp_usage(main, extra, info)
        for r, lim in budgets.items():
            if lim is not None and used[r] > lim:
                return r
        return None

    def affordable(n: str) -> bool:
        r = info.get(n, {}).get("rarity", "?")
        lim = budgets.get(r)
        return lim is None or cp_usage(main, extra, info)[r] + CRAFT_COST <= lim

    def replacement(for_role: str, exclude: str) -> str | None:
        pool = []
        for n in names:
            if n == exclude or n not in info or is_extra_card(info[n]):
                continue
            if main.get(n, 0) >= cap_for(n, banlist) or not affordable(n):
                continue
            # nur Karten aus Listen dieses Archetyps oder generische Staples (keine fremden Engines)
            if n not in main_stats and (info[n].get("archetype") or role(n) not in ("handtrap", "breaker", "draw", "tech")):
                continue
            score = main_stats.get(n, {}).get("inclusion", 0) * 2 + global_stats.get(n, {}).get("inclusion", 0)
            same = role(n) == for_role
            pool.append((same, score, n))
        pool.sort(reverse=True)
        return pool[0][2] if pool else None

    # ---------- Budget-Filter ----------
    guard = 0
    while (r := over_budget()) and guard < 120:
        guard += 1
        ex_cands = [n for n, c in extra.items() if info.get(n, {}).get("rarity") == r]
        main_cands = [n for n, c in main.items() if info.get(n, {}).get("rarity") == r]
        # zuerst Extra-Deck-Karten mit geringster Nutzung entfernen (kosten keine Konsistenz)
        if ex_cands and (not main_cands or min(extra_stats[n]["expected"] for n in ex_cands)
                         < min(value(n) for n in main_cands) * 0.5):
            n = min(ex_cands, key=lambda n: extra_stats[n]["expected"])
            extra[n] -= 1
            alt = next((a for a in sorted(extra_stats, key=lambda a: -extra_stats[a]["expected"])
                        if a != n and extra.get(a, 0) < cap_for(a, banlist) and a in info
                        and info[a].get("rarity") != r and affordable(a)
                        and sum(extra.values()) < EXTRA_MAX), None)
            if alt:
                extra[alt] = extra.get(alt, 0) + 1
            changes.append({"out": n, "in": alt, "part": "extra", "reason": f"{r}-Budget"})
            extra = {k: v for k, v in extra.items() if v > 0}
            continue
        if not main_cands:
            break
        n = min(main_cands, key=value)
        main[n] -= 1
        alt = replacement(role(n), n)
        if alt:
            main[alt] = main.get(alt, 0) + 1
        changes.append({"out": n, "in": alt, "part": "main", "reason": f"{r}-Budget"})
        main = {k: v for k, v in main.items() if v > 0}

    # Lücken auffüllen (falls Budget-Ersatz nicht für jede Kopie möglich war)
    while sum(main.values()) < DECK_SIZE and (alt := replacement("tech", "")):
        main[alt] = main.get(alt, 0) + 1
        changes.append({"out": None, "in": alt, "part": "main", "reason": "Auffüllen"})

    # ---------- Konsistenz-Optimierung (Ziel: P(Combo) ≥ target) ----------
    def counts() -> dict[str, int]:
        cc = Counter()
        for n, c in main.items():
            cc[to_cat(role(n))] += c
        return dict(cc)

    remove_rank = {"engine": 0, "tech": 1, "draw": 2, "breaker": 3, "handtrap": 4, "extender": 5}
    swaps = 0
    while analyze_counts(counts(), req.hand_size)["p_combo"] < req.target and swaps < 20:
        adds = [n for n in names if n in info and not is_extra_card(info[n])
                and role(n) == "starter" and main.get(n, 0) < cap_for(n, banlist) and affordable(n)
                and n in main_stats]  # nur Starter, die dieser Archetyp tatsächlich spielt
        if not adds:
            adds = [n for n in names if n in info and role(n) == "extender" and not is_extra_card(info[n])
                    and main.get(n, 0) < cap_for(n, banlist) and affordable(n) and main_stats.get(n)]
        removes = [n for n in main if role(n) in remove_rank]
        if not adds or not removes:
            break
        add = max(adds, key=lambda n: (main_stats.get(n, {}).get("expected", 0), global_stats.get(n, {}).get("inclusion", 0)))
        rem = min(removes, key=lambda n: (remove_rank[role(n)], value(n)))
        main[rem] -= 1
        main[add] = main.get(add, 0) + 1
        main = {k: v for k, v in main.items() if v > 0}
        changes.append({"out": rem, "in": add, "part": "main", "reason": "Konsistenz"})
        swaps += 1

    analysis = analyze_counts(counts(), req.hand_size)
    hand_avg = statistics.mean(sum(e["amount"] for e in d["main"] if e["name"] in HANDTRAPS) for d in arch_decks)

    def card_out(n: str, c: int, part: str) -> dict:
        i = info.get(n, {})
        st = (main_stats if part == "main" else extra_stats).get(n, {})
        return {
            "name": n, "count": c, "id": i.get("id", 0), "rarity": i.get("rarity", "?"),
            "type": i.get("type", ""), "desc": i.get("desc", ""), "frame": i.get("frame", ""),
            "role": "extra" if part == "extra" else role(n),
            "inclusion": round(st.get("inclusion", 0), 3), "ban": banlist.get(n),
        }

    sort_key = lambda x: (ROLE_ORDER.index(x["role"]), -x["inclusion"], x["name"])  # noqa: E731
    main_out = sorted((card_out(n, c, "main") for n, c in main.items()), key=sort_key)
    extra_out = sorted((card_out(n, c, "extra") for n, c in extra.items()), key=lambda x: (-x["inclusion"], x["name"]))
    return {
        "archetype": arche, "sample_size": len(arch_decks),
        "main": main_out, "extra": extra_out,
        "analysis": analysis, "target": req.target,
        "target_reached": analysis["p_combo"] >= req.target,
        "missing_slots": max(0, DECK_SIZE - sum(main.values())),
        "min_starters_one_card": min_starters(req.target, DECK_SIZE, req.hand_size),
        "meta_avg_handtraps": round(hand_avg, 1),
        "role_counts": dict(Counter(c["role"] for c in main_out for _ in range(c["count"]))),
        "cp": cp_usage(main, extra, info), "changes": changes,
        "sources": [{"event": d["event"], "created": d["created"][:10], "url": d["url"]} for d in arch_decks[:12]],
    }


# --------------------------------------------------------------------------- #
# Playbook & Handtrap-Guidance
# --------------------------------------------------------------------------- #
def first_clause(desc: str, pattern: re.Pattern) -> str:
    for sent in re.split(r"(?<=[.;])\s+", desc):
        if pattern.search(sent):
            return sent.strip()[:220]
    return ""


def load_curated() -> dict:
    try:
        return json.loads(COMBOS_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def build_playbook(arche: str, main: list[dict], extra: list[dict]) -> dict:
    curated = load_curated().get(arche)
    starters = sorted([c for c in main if c["role"] == "starter"], key=lambda c: (-c["inclusion"], -c["count"]))
    extenders = sorted([c for c in main if c["role"] == "extender"], key=lambda c: -c["inclusion"])
    extra_top = sorted(extra, key=lambda c: -c["inclusion"])[:5]

    steps: list[dict] = []
    if curated and curated.get("turn1"):
        steps = [{"text": s, "card": None} for s in curated["turn1"]]
        source = "kuratiert (combos.json)"
    else:
        source = "automatisch erzeugtes Gerüst – Reihenfolge vor Turnier-Einsatz testen"
        monsters = [c for c in starters if "Monster" in c["type"]]
        spells = [c for c in starters if "Spell" in c["type"]]
        for c in spells[:1]:
            steps.append({"card": c["name"], "text": f"Aktiviere {c['name']}: "
                          + (first_clause(c["desc"], RE_SEARCH) or "Suche/Setup-Effekt") })
        for c in monsters[:1]:
            clause = first_clause(c["desc"], RE_SEARCH) or first_clause(c["desc"], RE_SS_DECK)
            steps.append({"card": c["name"], "text": f"Normalbeschwörung: {c['name']} → {clause or 'Effekt aktivieren'}"})
        for c in monsters[1:2]:
            steps.append({"card": c["name"], "text": f"Hole bzw. nutze {c['name']} als zweiten Starter: "
                          + (first_clause(c["desc"], RE_SEARCH) or first_clause(c["desc"], RE_SS_DECK) or "Effekt aktivieren")})
        for c in extenders[:2]:
            steps.append({"card": c["name"], "text": f"Spezialbeschwörung: {c['name']} → "
                          + (first_clause(c["desc"], RE_SELF_SS) or "als Material verwenden")})
        if extra_top:
            steps.append({"card": extra_top[0]["name"], "text": "Materialien in die Extra-Deck-Ziele überführen: "
                          + ", ".join(f"{c['name']} ({round(c['inclusion'] * 100)}% der Listen)" for c in extra_top)})
        steps.append({"card": None, "text": "Endboard: Negationen/Unterbrechungen aufbauen, Hand-Interaktion für den Gegnerzug halten."})

    # Chokepoints: wo greifen Ash / Imperm / Veiler / Maxx "C"?
    chokes = []
    for c in (starters + extenders)[:8]:
        d = c["desc"]
        ash = bool(RE_SEARCH.search(d) or RE_SS_DECK.search(d) or RE_SEND_DECK.search(d))
        on_field = "Monster" in c["type"] and "discard this card" not in d.lower()
        chokes.append({
            "card": c["name"], "ash": ash, "imperm": on_field, "role": c["role"],
            "tip": ("Ash Blossom trifft den Deck-Zugriff – mit Called by the Grave/Crossout schützen oder "
                    "zuerst einen weniger wichtigen Starter als Köder ausspielen." if ash else
                    "Effekt wird auf dem Feld aktiviert – anfällig für Infinite Impermanence/Effect Veiler; "
                    "nach Möglichkeit erst nach dem Auflösen der Handtraps beschwören." if on_field else
                    "Kein typischer Handtrap-Chokepoint."),
        })
    protection = [c["name"] for c in main if c["name"] in PROTECTION]
    maxx = {"text": "Maxx \"C\" / Mulcharmy: Nach der Auflösung entscheiden, ob ein kleineres Board "
                    "(≤ 4 Spezialbeschwörungen) reicht. Kombos mit vielen Beschwörungen nicht blind durchspielen."}
    if curated and curated.get("chokepoints"):
        chokes = [{"card": None, "ash": None, "imperm": None, "role": "", "tip": t} for t in curated["chokepoints"]] + chokes
    return {"archetype": arche, "source": source, "turn1": steps, "chokepoints": chokes,
            "protection_in_deck": protection, "maxx_c": maxx, "notes": (curated or {}).get("notes", "")}


async def ask_claude(prompt: str, max_tokens: int = 1600) -> str | None:
    if not ANTHROPIC_API_KEY:
        return None
    try:
        async with httpx.AsyncClient(timeout=90) as c:
            r = await c.post("https://api.anthropic.com/v1/messages", headers={
                "x-api-key": ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01",
                "content-type": "application/json"},
                json={"model": ANTHROPIC_MODEL, "max_tokens": max_tokens,
                      "messages": [{"role": "user", "content": prompt}]})
            r.raise_for_status()
            return "".join(b.get("text", "") for b in r.json().get("content", []) if b.get("type") == "text")
    except Exception as ex:  # noqa: BLE001
        log.warning("KI-Aufruf fehlgeschlagen: %r", ex)
        return None


def deck_prompt_block(main: list[dict], extra: list[dict]) -> str:
    lines = [f"{c['count']}x {c['name']} [{ROLE_LABEL.get(c['role'], c['role'])}]: {c['desc'][:400]}" for c in main]
    lines += [f"{c['count']}x {c['name']} [Extra]: {c['desc'][:300]}" for c in extra]
    return "\n".join(lines)


# --------------------------------------------------------------------------- #
# Deck Doctor
# --------------------------------------------------------------------------- #
LINE_RE = re.compile(r"^\s*(?:(\d+)\s*[xX×]?\s+(.+?)|(.+?)\s+[xX×]\s*(\d+))\s*$")


async def parse_decklist(text: str) -> tuple[dict[str, int], dict[str, int], list[str]]:
    main: Counter = Counter()
    extra: Counter = Counter()
    warnings: list[str] = []
    lines = [l.strip() for l in text.replace("\r", "").split("\n") if l.strip()]
    is_ydk = any(l.lower() in ("#main", "#extra", "!side") for l in lines)
    if is_ydk:
        section, ids = "main", {"main": Counter(), "extra": Counter()}
        for l in lines:
            low = l.lower()
            if low == "#main":
                section = "main"
            elif low == "#extra":
                section = "extra"
            elif low == "!side":
                section = "side"
            elif l.isdigit() and section in ids:
                ids[section][int(l)] += 1
        all_ids = list(set(ids["main"]) | set(ids["extra"]))
        known = cards_by_ids(all_ids)
        missing = [i for i in all_ids if i not in known]
        if missing:
            async with httpx.AsyncClient(timeout=60, headers={"User-Agent": USER_AGENT}) as client:
                fetched = await fetch_ygo_cards(client, ids=missing)
            cards_put(fetched)
            known.update({c["id"]: c for c in fetched})
        for sec, target in (("main", main), ("extra", extra)):
            for cid, n in ids[sec].items():
                if cid in known:
                    target[known[cid]["name"]] += n
                else:
                    warnings.append(f"Unbekannte Karten-ID {cid} ignoriert")
        return dict(main), dict(extra), warnings

    section = "main"
    for l in lines:
        low = l.lower().strip("#: ")
        if low in ("main", "main deck", "maindeck"):
            section = "main"; continue
        if low in ("extra", "extra deck", "extradeck"):
            section = "extra"; continue
        if low in ("side", "side deck"):
            section = "side"; continue
        m = LINE_RE.match(l)
        name, n = ((m.group(2), int(m.group(1))) if m and m.group(1) else
                   (m.group(3), int(m.group(4))) if m else (l, 1))
        if section == "main":
            main[name.strip()] += n
        elif section == "extra":
            extra[name.strip()] += n
    names = list(set(main) | set(extra))
    known = cards_get(names)
    missing = [n for n in names if n not in known]
    if missing:
        async with httpx.AsyncClient(timeout=60, headers={"User-Agent": USER_AGENT}) as client:
            fetched = await fetch_ygo_cards(client, names=missing)
        cards_put(fetched)
        known.update({c["name"]: c for c in fetched})
    for n in missing:
        if n not in known:
            warnings.append(f"Karte nicht gefunden (Schreibweise prüfen): {n}")
    # Extra-Deck-Karten, die versehentlich im Main stehen, verschieben
    for n in list(main):
        if n in known and is_extra_card(known[n]):
            extra[n] += main.pop(n)
    return dict(main), dict(extra), warnings


class DoctorRequest(BaseModel):
    text: str = Field(..., max_length=20000)
    archetype: str | None = None
    hand_size: int = Field(5, ge=5, le=6)
    overrides: dict[str, str] = Field(default_factory=dict)
    ai: bool = False


async def deck_doctor(req: DoctorRequest) -> dict:
    decks, banlist = load_meta()
    main, extra, warnings = await parse_decklist(req.text)
    if not main:
        raise HTTPException(400, "Keine Karten erkannt. Format: YDK oder Zeilen wie '3 Ash Blossom & Joyous Spring'.")

    # Archetyp automatisch erkennen (größte Überschneidung mit Meta-Listen)
    archetypes = {d["archetype"] for d in decks}
    if req.archetype and req.archetype in archetypes:
        arche = req.archetype
    else:
        def overlap(a: str) -> float:
            st = card_stats([d for d in decks if d["archetype"] == a], "main")
            return sum(min(main.get(n, 0), round(s["expected"])) for n, s in st.items())
        arche = max(archetypes, key=overlap)
    arch_decks = [d for d in decks if d["archetype"] == arche]
    st = card_stats(arch_decks, "main")
    gst = card_stats(decks, "main")
    info = cards_get(list(set(main) | set(extra) | set(st)))

    def role(n: str) -> str:
        return role_of(info.get(n, {"name": n}), req.overrides, arche)

    counts = Counter()
    for n, c in main.items():
        counts[to_cat(role(n))] += c
    analysis = analyze_counts(dict(counts), req.hand_size)
    size = sum(main.values())

    findings: list[dict] = []
    def add(sev: str, text: str) -> None:
        findings.append({"severity": sev, "text": text})

    if size < 40:
        add("error", f"Main Deck hat nur {size} Karten (Minimum 40).")
    elif size > 40:
        add("warn", f"Main Deck hat {size} Karten – jede Karte über 40 senkt die Starter-Chance.")
    if sum(extra.values()) > EXTRA_MAX:
        add("error", f"Extra Deck hat {sum(extra.values())} Karten (Maximum 15).")
    for n, c in {**main, **extra}.items():
        cap = cap_for(n, banlist)
        if c > cap:
            add("error", f"{n}: {c} Kopien, erlaubt sind {cap} (Banliste/Regel).")
    if analysis["p_combo"] < 0.85:
        need = min_starters(0.85, max(size, 40), req.hand_size)
        add("warn", f"Combo-Chance nur {analysis['p_combo']:.1%} – für 85 % (1-Karten-Starter) werden ca. "
                    f"{need} Starter bei {max(size, 40)} Karten benötigt, aktuell {counts['starter']}.")
    else:
        add("ok", f"Combo-Chance {analysis['p_combo']:.1%} – Konsistenzziel erreicht.")

    plus = [(n, s) for n, s in sorted(st.items(), key=lambda x: -x[1]["inclusion"] * x[1]["median"])
            if s["inclusion"] >= 0.6 and main.get(n, 0) < s["median"]]
    for n, s in plus[:8]:
        add("suggest", f"+ {n}: in {s['inclusion']:.0%} der {arche}-Listen (meist {s['median']:g}x), "
                       f"bei dir {main.get(n, 0)}x.")
    minus = sorted((n for n in main if st.get(n, {}).get("inclusion", 0) < 0.1
                    and gst.get(n, {}).get("inclusion", 0) < 0.15), key=lambda n: st.get(n, {}).get("inclusion", 0))
    for n in minus[:6]:
        add("suggest", f"− {n}: kaum gespielt (in {st.get(n, {}).get('inclusion', 0):.0%} der {arche}-Listen) – Ersatz prüfen.")
    ht = sum(c for n, c in main.items() if role(n) == "handtrap")
    avg_ht = statistics.mean(sum(e["amount"] for e in d["main"] if e["name"] in HANDTRAPS) for d in arch_decks)
    if ht + 2 < avg_ht:
        add("suggest", f"Nur {ht} Handtraps – {arche}-Listen spielen im Schnitt {avg_ht:.1f}.")

    penalty = sum({"error": 15, "warn": 8, "suggest": 3}.get(f["severity"], 0) for f in findings)
    score = max(0, min(100, round(analysis["p_combo"] * 100) - penalty + 15))

    ai_text = None
    if req.ai:
        cards = [{"name": n, "count": c, "role": role(n), "desc": info.get(n, {}).get("desc", "")} for n, c in main.items()]
        xc = [{"name": n, "count": c, "role": "extra", "desc": info.get(n, {}).get("desc", "")} for n, c in extra.items()]
        meta = "\n".join(f"{n}: {s['inclusion']:.0%} Inklusion, Median {s['median']:g}x"
                         for n, s in sorted(st.items(), key=lambda x: -x[1]["inclusion"])[:35])
        ai_text = await ask_claude(
            f"Du bist ein Yu-Gi-Oh! Master Duel Deck-Coach. Antworte auf Deutsch, knapp und konkret.\n"
            f"Archetyp: {arche}. Combo-Chance {analysis['p_combo']:.1%}.\n"
            f"Meta-Statistik (MDM-Top-Decks, letzte {META_WINDOW_DAYS} Tage):\n{meta}\n\n"
            f"Deck des Nutzers:\n{deck_prompt_block(cards, xc)}\n\n"
            "Gib 5–8 priorisierte Verbesserungen (rein/raus mit Begründung) und nenne die wichtigsten "
            "Handtrap-Chokepoints. Erfinde keine Kartennamen und kennzeichne Unsicherheiten.")

    return {"archetype": arche, "sample_size": len(arch_decks), "main_size": size,
            "analysis": analysis, "findings": findings, "score": score,
            "warnings": warnings, "ai": ai_text,
            "deck": {"main": [{"name": n, "count": c, "id": info.get(n, {}).get("id", 0),
                               "role": role(n), "rarity": info.get(n, {}).get("rarity", "?"),
                               "desc": info.get(n, {}).get("desc", ""), "type": info.get(n, {}).get("type", "")}
                              for n, c in main.items()],
                     "extra": [{"name": n, "count": c, "id": info.get(n, {}).get("id", 0), "role": "extra",
                                "rarity": info.get(n, {}).get("rarity", "?"), "desc": info.get(n, {}).get("desc", ""),
                                "type": info.get(n, {}).get("type", "")} for n, c in extra.items()]}}


# --------------------------------------------------------------------------- #
# FastAPI-App
# --------------------------------------------------------------------------- #
scheduler = AsyncIOScheduler(timezone="UTC")


@asynccontextmanager
async def lifespan(_: FastAPI):
    init_db()
    status = kv_get("status", {}) or {}
    last = parse_date(status.get("last_update", "")) if status.get("ok") else None
    stale = not last or datetime.now(timezone.utc) - last > timedelta(hours=REFRESH_HOURS)
    scheduler.add_job(refresh_meta, "interval", hours=REFRESH_HOURS, id="meta_refresh",
                      next_run_time=datetime.now(timezone.utc) + timedelta(seconds=3) if stale else None,
                      max_instances=1, coalesce=True, misfire_grace_time=3600)
    scheduler.start()
    yield
    scheduler.shutdown(wait=False)


app = FastAPI(title="MD Meta Forge", version="1.0", lifespan=lifespan)
app.add_middleware(GZipMiddleware, minimum_size=1000)


@app.get("/", include_in_schema=False)
async def index() -> FileResponse:
    return FileResponse(BASE_DIR / "index.html")


@app.get("/healthz", include_in_schema=False)
async def healthz() -> dict:
    return {"ok": True}


@app.get("/api/status")
async def api_status() -> dict:
    st = kv_get("status", {}) or {}
    job = scheduler.get_job("meta_refresh") if scheduler.running else None
    st["next_run"] = job.next_run_time.isoformat() if job and job.next_run_time else None
    st["refreshing"] = _refresh_lock.locked()
    st["ai_enabled"] = bool(ANTHROPIC_API_KEY)
    return st


@app.get("/api/tierlist")
async def api_tierlist() -> list[dict]:
    return kv_get("tierlist", []) or []


@app.post("/api/build")
async def api_build(req: BuildRequest) -> dict:
    return await asyncio.to_thread(build_deck, req)


class AnalyzeRequest(BaseModel):
    counts: dict[str, int]
    hand_size: int = Field(5, ge=5, le=6)


@app.post("/api/analyze")
async def api_analyze(req: AnalyzeRequest) -> dict:
    if sum(req.counts.values()) > 60:
        raise HTTPException(400, "Maximal 60 Karten")
    return analyze_counts(req.counts, req.hand_size)


@app.get("/api/ratio-table")
async def api_ratio(deck: int = Query(DECK_SIZE, ge=40, le=60)) -> list[dict]:
    return ratio_table(deck)


class PlaybookRequest(BaseModel):
    archetype: str
    main: list[dict]
    extra: list[dict] = Field(default_factory=list)
    ai: bool = False


@app.post("/api/playbook")
async def api_playbook(req: PlaybookRequest) -> dict:
    pb = build_playbook(req.archetype, req.main, req.extra)
    if req.ai:
        pb["ai"] = await ask_claude(
            "Du bist ein Yu-Gi-Oh! Master Duel Combo-Experte. Antworte auf Deutsch.\n"
            f"Archetyp: {req.archetype}. Deckliste mit Kartentexten:\n{deck_prompt_block(req.main, req.extra)}\n\n"
            "Schreibe (1) eine Zug-1-Standardkombo als nummerierte Schritte ausgehend vom häufigsten "
            "1-Karten-Starter, (2) das erwartete Endboard, (3) Handtrap-Chokepoints (Ash, Imperm/Veiler, Maxx C, "
            "Nibiru) mit Gegenstrategie. Nutze nur Karten aus der Liste und markiere Schritte, bei denen du dir "
            "über Effekt-Details unsicher bist, mit (prüfen).", max_tokens=2200)
    return pb


@app.post("/api/doctor")
async def api_doctor(req: DoctorRequest) -> dict:
    return await deck_doctor(req)


@app.api_route("/api/refresh", methods=["GET", "POST"])
async def api_refresh(token: str | None = Query(None), x_refresh_token: str | None = Header(None)) -> JSONResponse:
    """Webhook: z. B. per cron-job.org oder GitHub Actions aufrufen."""
    if not REFRESH_TOKEN or (token or x_refresh_token) != REFRESH_TOKEN:
        raise HTTPException(401, "Ungültiges oder fehlendes REFRESH_TOKEN")
    asyncio.create_task(refresh_meta("webhook"))
    return JSONResponse({"ok": True, "message": "Refresh gestartet"}, status_code=202)


_img_locks: dict[int, asyncio.Lock] = defaultdict(asyncio.Lock)


@app.get("/img/{card_id}.jpg", include_in_schema=False)
async def card_image(card_id: int) -> Response:
    """Lädt jedes Kartenbild genau einmal von YGOPRODeck und liefert es danach lokal aus."""
    if card_id <= 0 or card_id > 999_999_999:
        raise HTTPException(404)
    path = IMG_DIR / f"{card_id}.jpg"
    headers = {"Cache-Control": "public, max-age=2592000, immutable"}
    if path.exists():
        return FileResponse(path, media_type="image/jpeg", headers=headers)
    async with _img_locks[card_id]:
        if not path.exists():
            async with httpx.AsyncClient(timeout=30, headers={"User-Agent": USER_AGENT}) as client:
                r = await ygo_get(client, YGO_IMG.format(id=card_id))
            if r.status_code != 200 or not r.headers.get("content-type", "").startswith("image"):
                raise HTTPException(404)
            tmp = path.with_suffix(".tmp")
            tmp.write_bytes(r.content)
            tmp.replace(path)
    return FileResponse(path, media_type="image/jpeg", headers=headers)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
