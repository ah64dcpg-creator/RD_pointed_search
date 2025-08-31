import os, re, json, asyncio, pytz, httpx
from datetime import datetime
from dateutil import parser as dateparser
from bs4 import BeautifulSoup

BACKEND = os.getenv("BACKEND_API_URL","").strip()
if BACKEND and not BACKEND.startswith(("http://","https://")):
    BACKEND = "https://" + BACKEND
BACKEND = BACKEND.rstrip("/")
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN")
GCS_API_KEY = os.getenv("GCS_API_KEY")
GCS_CX = os.getenv("GCS_CX")
EVENTBRITE_TOKEN = os.getenv("EVENTBRITE_TOKEN")
REGION_CODE = os.getenv("REGION_CODE","")
CITY_LIST = [c.strip() for c in os.getenv("CITY_LIST","").split(",") if c.strip()]

HEADERS = {"Authorization": f"Bearer {ADMIN_TOKEN}"} if ADMIN_TOKEN else {}
UA = {"User-Agent":"RaisingDaisiesBot/1.0 (+respect robots)"}
KEYWORDS = ["grief","bereavement","child loss","pregnancy loss","infant loss","healing","support group"]

def to_iso(dt):
    if isinstance(dt, datetime):
        if dt.tzinfo is None: return dt.isoformat()+"Z"
        return dt.isoformat()
    return str(dt)

def norm_event(e: dict):
    if not e.get("title") or not e.get("starts_at"): return None
    return {
        "title": e["title"].strip(),
        "description": e.get("description"),
        "starts_at": to_iso(e["starts_at"]),
        "ends_at": to_iso(e["ends_at"]) if e.get("ends_at") else None,
        "timezone": e.get("timezone"),
        "format": e.get("format","in_person"),
        "audience": e.get("audience", ["parents"]),
        "language": e.get("language","en"),
        "cost_min": e.get("cost_min", 0),
        "cost_max": e.get("cost_max", 0),
        "organizer_name": e.get("organizer_name"),
        "organizer_email": e.get("organizer_email"),
        "venue_name": e.get("venue_name"),
        "address": e.get("address"),
        "city": e.get("city"),
        "state": e.get("state") or REGION_CODE,
        "postal_code": e.get("postal_code"),
        "lat": e.get("lat"),
        "lng": e.get("lng"),
        "badges": e.get("badges", []),
        "verified": True,
    }

async def google_search(client, q: str, num: int = 5):
    if not GCS_API_KEY or not GCS_CX: return []
    url = "https://www.googleapis.com/customsearch/v1"
    params = {"key": GCS_API_KEY, "cx": GCS_CX, "q": q, "num": num, "safe":"active"}
    r = await client.get(url, params=params, timeout=40.0)
    r.raise_for_status()
    data = r.json()
    return [item["link"] for item in data.get("items", []) if "link" in item]

def parse_jsonld_events(html: str, fallback_city=None):
    soup = BeautifulSoup(html, "html.parser")
    out = []
    for tag in soup.find_all("script", attrs={"type":"application/ld+json"}):
        try:
            data = json.loads(tag.string)
        except Exception:
            continue
        nodes = data if isinstance(data, list) else [data]
        for node in nodes:
            t = node.get("@type")
            is_event = ("Event" in t) if isinstance(t, list) else (t == "Event")
            if not is_event: continue
            name = node.get("name"); start = node.get("startDate")
            if not name or not start: continue
            try:
                starts_at = dateparser.parse(start)
                ends_at = dateparser.parse(node.get("endDate")) if node.get("endDate") else None
            except Exception:
                continue
            loc = node.get("location") or {}
            venue = addr = city = state = postal = None
            if isinstance(loc, dict):
                venue = loc.get("name")
                a = loc.get("address")
                if isinstance(a, dict):
                    addr = a.get("streetAddress"); city = a.get("addressLocality"); state = a.get("addressRegion"); postal = a.get("postalCode")
            desc = node.get("description") or None
            out.append({
                "title": name, "description": desc,
                "starts_at": starts_at, "ends_at": ends_at,
                "format": "in_person" if venue or addr else "virtual",
                "venue_name": venue, "address": addr,
                "city": city or fallback_city, "state": state, "postal_code": postal
            })
    return out

async def fetch_text(client, url):
    r = await client.get(url, headers=UA, timeout=40.0, follow_redirects=True)
    r.raise_for_status()
    return r.text

async def discover_pages(client, city: str):
    kw = " OR ".join([f'"{k}"' for k in KEYWORDS])
    q = f"({kw}) (event OR "support group") "{city}" {REGION_CODE} site:(.org OR .edu OR .gov)"
    try:
        links = await google_search(client, q, num=8)
    except Exception:
        links = []
    return links

async def fetch_eventbrite(client, city: str):
    if not EVENTBRITE_TOKEN: return []
    url = "https://www.eventbriteapi.com/v3/events/search/"
    params = {"q":"grief OR bereavement OR healing OR support group", "location.address":city, "expand":"venue,organizer"}
    r = await client.get(url, headers={"Authorization": f"Bearer {EVENTBRITE_TOKEN}"}, params=params, timeout=40.0)
    if r.status_code >= 400: return []
    data = r.json(); out=[]
    for ev in data.get("events", []):
        try:
            starts_at = dateparser.parse(ev.get("start",{}).get("utc")) if ev.get("start") else None
        except Exception: continue
        if not starts_at: continue
        venue = ev.get("venue") or {}
        addr = venue.get("address") or {}
        out.append({
            "title": (ev.get("name") or {}).get("text"),
            "description": (ev.get("description") or {}).get("text"),
            "starts_at": starts_at,
            "ends_at": dateparser.parse(ev.get("end",{}).get("utc")) if ev.get("end") else None,
            "format": "in_person" if addr else "virtual",
            "venue_name": venue.get("name"),
            "address": addr.get("address_1"),
            "city": addr.get("city") or city,
            "state": addr.get("region") or os.getenv("REGION_CODE"),
            "postal_code": addr.get("postal_code")
        })
    return out

async def main():
    if not BACKEND or not ADMIN_TOKEN:
        raise SystemExit("Set BACKEND_API_URL and ADMIN_TOKEN")
    if not CITY_LIST:
        raise SystemExit("Set CITY_LIST env var (comma-separated cities)")
    normalized = []
    async with httpx.AsyncClient() as client:
        for city in CITY_LIST:
            # Eventbrite
            eb = await fetch_eventbrite(client, city)
            normalized += [x for x in (norm_event(e) for e in eb) if x]
            # Discovery
            links = await discover_pages(client, city)
            for url in links:
                try:
                    html = await fetch_text(client, url)
                    items = parse_jsonld_events(html, fallback_city=city)
                    normalized += [x for x in (norm_event(e) for e in items) if x]
                except Exception:
                    continue
    # Dedup
    seen=set(); unique=[]
    for e in normalized:
        key=(e["title"], e["starts_at"][:16], e.get("city") or "")
        if key in seen: continue
        seen.add(key); unique.append(e)
    if not unique:
        print("No events found."); return
    url=f"{BACKEND}/admin/ingest_bulk"
    r = await httpx.AsyncClient(timeout=60.0).post(url, headers= {"Authorization": f"Bearer {ADMIN_TOKEN}"} , json={"events": unique})
    print("Ingest status:", r.status_code, r.text)

if __name__=="__main__":
    asyncio.run(main())
