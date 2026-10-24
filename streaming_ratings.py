#!/usr/bin/env python3
"""
Streaming by Rating
===================

A small website that runs on your own computer. Pick a country and a
streaming service from the dropdowns, and it lists every movie on that
service, sorted by Rotten Tomatoes, Metacritic and IMDb scores.

Needs Python 3.8 or newer. Nothing else to install.

START
  python3 streaming_ratings.py          (on Windows: python streaming_ratings.py)

  The first time, it asks for two free API keys and saves them in
  config.json next to this script:
    TMDB (catalogs):  https://www.themoviedb.org/settings/api
    OMDb (scores):    https://www.omdbapi.com/apikey.aspx

  Your browser opens http://localhost:8000. Leave the window running
  while you use the site; press Ctrl+C there to stop it.

OPTIONS
  --port 8123     use another port
  --no-open       don't open the browser

HOW THE DATA FILLS IN
  * A country + service catalog takes a minute or so the first time,
    then it's cached and refreshed once a day in the background.
  * Scores come from OMDb. The free key allows 1,000 lookups a day, so
    movies you haven't seen before get scores gradually, most popular
    first, while the site is running. Scores are shared between all
    countries and services, and kept in the data folder.
  * Star a movie to add it to your favourites. The Favourites view shows
    their current scores and which of your loaded catalogs they're on.
    Favourites are saved in data/favourites.json.
  * Everything stays on your computer. The site only listens on
    localhost, so other devices on your network can't reach it.
"""

import argparse
import json
import os
import re
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import webbrowser
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

TMDB_BASE = "https://api.themoviedb.org/3"
OMDB_BASE = "https://www.omdbapi.com/"
HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, "data")
CONFIG_PATH = os.path.join(HERE, "config.json")

CATALOG_MAX_AGE = 24 * 3600
DETAILS_MAX_AGE = 90 * 86400
SCORES_MAX_AGE = 30 * 86400
LISTS_MAX_AGE = 7 * 86400

DEFAULT_CONFIG = {
    "tmdb_key": "",
    "omdb_key": "",
    "omdb_daily_budget": 950,
    "port": 8000,
    "last_country": "ES",
    "last_service": 8,
}

LANGUAGE_FOR_COUNTRY = {
    "ES": "es-ES", "MX": "es-MX", "AR": "es-AR", "CO": "es-CO", "CL": "es-CL", "PE": "es-PE",
    "FR": "fr-FR", "BE": "fr-BE", "DE": "de-DE", "AT": "de-AT", "CH": "de-CH",
    "IT": "it-IT", "PT": "pt-PT", "BR": "pt-BR", "NL": "nl-NL", "PL": "pl-PL",
    "CZ": "cs-CZ", "SE": "sv-SE", "DK": "da-DK", "NO": "no-NO", "FI": "fi-FI",
    "JP": "ja-JP", "KR": "ko-KR", "TR": "tr-TR", "GR": "el-GR", "HU": "hu-HU", "RO": "ro-RO",
}


class ApiError(Exception):
    """A problem the person can fix (bad key, no connection)."""


class OmdbLimitReached(Exception):
    pass


# ------------------------------------------------------------------ storage

def load_json(path, default):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return default
    except json.JSONDecodeError:
        if path == CONFIG_PATH:
            sys.exit(f"{path} isn't valid JSON. Fix it or delete it to start over.")
        return default


def save_json(path, data, pretty=False):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2 if pretty else None)
    os.replace(tmp, path)


class Store:
    """A JSON dict on disk, safe to use from several threads."""

    def __init__(self, path):
        self.path = path
        self.lock = threading.RLock()
        self.data = load_json(path, {})
        self.dirty = False

    def get(self, key, default=None):
        with self.lock:
            return self.data.get(key, default)

    def set(self, key, value):
        with self.lock:
            self.data[key] = value
            self.dirty = True

    def items(self):
        with self.lock:
            return list(self.data.items())

    def pop(self, key):
        with self.lock:
            if key in self.data:
                del self.data[key]
                self.dirty = True

    def save(self):
        with self.lock:
            if self.dirty:
                save_json(self.path, self.data)
                self.dirty = False


# ------------------------------------------------------------------ HTTP clients

def http_json(url, headers=None, retries=4):
    req = urllib.request.Request(url, headers={"Accept": "application/json",
                                               "User-Agent": "streaming-ratings/2.0", **(headers or {})})
    last = None
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", "replace")
            if e.code == 401:
                if "omdbapi" in url and "limit" in body.lower():
                    raise OmdbLimitReached()
                which = "OMDb" if "omdbapi" in url else "TMDB"
                raise ApiError(f"{which} didn't accept the API key. Check it in config.json, then restart.")
            if e.code == 404:
                return None
            last = e
            if e.code == 429 or e.code >= 500:
                time.sleep(2 * (attempt + 1))
                continue
            raise ApiError(f"{url.split('?')[0]} answered with error {e.code}.")
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            last = e
            time.sleep(2 * (attempt + 1))
    raise ApiError(f"Couldn't reach {urllib.parse.urlparse(url).netloc}. Check your internet connection. ({last})")


class Tmdb:
    def __init__(self, key):
        # A "Read Access Token" is a long JWT starting with eyJ; an "API Key" is 32 characters.
        if key.startswith("eyJ"):
            self.headers, self.params = {"Authorization": f"Bearer {key}"}, {}
        else:
            self.headers, self.params = {}, {"api_key": key}

    def get(self, path, **params):
        q = urllib.parse.urlencode({**self.params, **params})
        return http_json(f"{TMDB_BASE}{path}?{q}", self.headers)


def parse_omdb(o):
    def num(s, cast=float):
        try:
            return cast(str(s).replace(",", "").replace("%", "").split("/")[0])
        except (ValueError, TypeError):
            return None

    rt = None
    for r in o.get("Ratings", []) or []:
        if r.get("Source") == "Rotten Tomatoes":
            rt = num(r.get("Value"), int)
    return {"imdb": num(o.get("imdbRating")), "imdb_votes": num(o.get("imdbVotes"), int),
            "rt": rt, "mc": num(o.get("Metascore"), int)}


def omdb_lookup(key, imdb_id):
    q = urllib.parse.urlencode({"i": imdb_id, "apikey": key})
    o = http_json(f"{OMDB_BASE}?{q}")
    if o is None:
        return None
    if o.get("Response") == "False":
        err = (o.get("Error") or "").lower()
        if "limit" in err:
            raise OmdbLimitReached()
        if "invalid api key" in err:
            raise ApiError("OMDb didn't accept the API key. Check it in config.json, then restart.")
        return {}
    return parse_omdb(o)


# ------------------------------------------------------------------ app

class App:
    def __init__(self, cfg):
        self.cfg = cfg
        self.tmdb = Tmdb(cfg["tmdb_key"])
        os.makedirs(os.path.join(DATA, "catalogs"), exist_ok=True)
        self.details = Store(os.path.join(DATA, "tmdb_details.json"))
        self.scores = Store(os.path.join(DATA, "omdb_scores.json"))
        self.lists = Store(os.path.join(DATA, "lists.json"))
        self.usage = Store(os.path.join(DATA, "omdb_usage.json"))
        self.favs = Store(os.path.join(DATA, "favourites.json"))
        self.id_sets = {}
        self.catalogs = {}
        self.cat_lock = threading.Lock()
        self.jobs = {}
        self.jobs_lock = threading.Lock()
        self.scorer = Scorer(self)
        self.scorer.start()
        threading.Thread(target=self._autosave, daemon=True).start()

    def _autosave(self):
        while True:
            time.sleep(10)
            self.save_all()

    def save_all(self):
        for s in (self.details, self.scores, self.lists, self.usage, self.favs):
            try:
                s.save()
            except OSError:
                pass

    def save_config(self, **changes):
        changed = {k: v for k, v in changes.items() if self.cfg.get(k) != v}
        if changed:
            self.cfg.update(changed)
            try:
                save_json(CONFIG_PATH, self.cfg, pretty=True)
            except OSError:
                pass

    # ---- lists (countries, services)

    def cached_list(self, key, fetch):
        entry = self.lists.get(key)
        if entry and time.time() - entry["t"] < LISTS_MAX_AGE:
            return entry["v"]
        try:
            value = fetch()
        except ApiError:
            if entry:
                return entry["v"]
            raise
        self.lists.set(key, {"t": time.time(), "v": value})
        return value

    def regions(self):
        def fetch():
            data = self.tmdb.get("/watch/providers/regions", language="en-US") or {}
            out = [{"code": r["iso_3166_1"], "name": r.get("english_name") or r["iso_3166_1"]}
                   for r in data.get("results", [])]
            return sorted(out, key=lambda r: r["name"])
        return self.cached_list("regions", fetch)

    def services(self, country):
        def fetch():
            data = self.tmdb.get("/watch/providers/movie", watch_region=country, language="en-US") or {}
            res = data.get("results", [])
            res.sort(key=lambda p: (p.get("display_priorities", {}).get(country, p.get("display_priority", 999)),
                                    p["provider_name"]))
            return [{"id": p["provider_id"], "name": p["provider_name"]} for p in res]
        return self.cached_list(f"services:{country}", fetch)

    def service_name(self, country, pid):
        for s in self.lists.get(f"services:{country}", {}).get("v", []):
            if s["id"] == pid:
                return s["name"]
        return f"Service {pid}"

    # ---- catalogs

    def catalog_path(self, key):
        return os.path.join(DATA, "catalogs", f"{key}.json")

    def get_catalog(self, key):
        with self.cat_lock:
            if key not in self.catalogs:
                self.catalogs[key] = load_json(self.catalog_path(key), None)
            return self.catalogs[key]

    def job(self, key):
        with self.jobs_lock:
            j = self.jobs.get(key)
            return dict(j) if j else None

    def start_job(self, country, pid):
        key = f"{country}_{pid}"
        with self.jobs_lock:
            j = self.jobs.get(key)
            if j and j["state"] == "running":
                return
            self.jobs[key] = {"state": "running", "step": "catalog", "done": 0, "total": 0, "error": None}
        threading.Thread(target=self._build_catalog, args=(country, pid, key), daemon=True).start()

    def _job_update(self, key, **kw):
        with self.jobs_lock:
            self.jobs[key].update(kw)

    def _build_catalog(self, country, pid, key):
        try:
            language = LANGUAGE_FOR_COUNTRY.get(country, "en-US")
            movies, page, total = {}, 1, 1
            while page <= min(total, 500):
                data = self.tmdb.get("/discover/movie",
                                     watch_region=country,
                                     with_watch_providers=pid,
                                     with_watch_monetization_types="flatrate|free|ads",
                                     language=language,
                                     sort_by="primary_release_date.desc",
                                     include_adult="false",
                                     page=page)
                if not data:
                    break
                total = data.get("total_pages", 1) or 1
                for m in data.get("results", []):
                    movies[m["id"]] = {
                        "id": m["id"],
                        "title_local": m.get("title"),
                        "original_title": m.get("original_title"),
                        "release_date": m.get("release_date"),
                        "overview_local": m.get("overview") or "",
                        "poster": m.get("poster_path"),
                        "popularity": m.get("popularity") or 0,
                    }
                self._job_update(key, done=page, total=min(total, 500))
                page += 1

            now = time.time()
            todo = [i for i in movies
                    if now - (self.details.get(str(i)) or {}).get("t", 0) > DETAILS_MAX_AGE]
            self._job_update(key, step="details", done=0, total=len(todo))
            if todo:
                done = 0
                with ThreadPoolExecutor(max_workers=8) as pool:
                    for mid, det in zip(todo, pool.map(self._fetch_details, todo)):
                        self.details.set(str(mid), det)
                        done += 1
                        if done % 25 == 0:
                            self._job_update(key, done=done)
                self.details.save()

            cat = {"country": country, "service": pid, "fetched": time.time(),
                   "movies": sorted(movies.values(), key=lambda m: -m["popularity"])}
            save_json(self.catalog_path(key), cat)
            with self.cat_lock:
                self.catalogs[key] = cat
            self._job_update(key, state="done", step="done")
            self.scorer.wake.set()
        except ApiError as e:
            self._job_update(key, state="error", error=str(e))
        except Exception as e:  # keep the server alive whatever happens
            self._job_update(key, state="error", error=f"Something went wrong while loading: {e}")

    def _fetch_details(self, movie_id):
        d = self.tmdb.get(f"/movie/{movie_id}", language="en-US", append_to_response="external_ids")
        if not d:
            return {"t": time.time()}
        return {
            "imdb_id": (d.get("external_ids") or {}).get("imdb_id") or d.get("imdb_id"),
            "title": d.get("title"),
            "runtime": d.get("runtime") or None,
            "genres": [g["name"] for g in d.get("genres", [])],
            "overview": d.get("overview") or "",
            "t": time.time(),
        }

    def assemble(self, cat):
        out = []
        for m in cat["movies"]:
            d = self.details.get(str(m["id"])) or {}
            s = self.scores.get(d.get("imdb_id") or "") or {}
            year = (m.get("release_date") or "")[:4]
            out.append({
                "tmdb_id": m["id"],
                "imdb_id": d.get("imdb_id"),
                "title": d.get("title") or m.get("title_local"),
                "title_local": m.get("title_local"),
                "original_title": m.get("original_title"),
                "year": int(year) if year.isdigit() else None,
                "runtime": d.get("runtime"),
                "genres": d.get("genres", []),
                "overview": d.get("overview") or m.get("overview_local") or "",
                "poster": m.get("poster"),
                "imdb": s.get("imdb"), "imdb_votes": s.get("imdb_votes"),
                "rt": s.get("rt"), "mc": s.get("mc"),
            })
        return out

    def pending_ids(self, key, limit=None):
        """IMDb ids in this catalog without scores, most popular first."""
        cat = self.get_catalog(key) if key else None
        if not cat:
            return []
        seen, out = set(), []
        for m in cat["movies"]:
            i = (self.details.get(str(m["id"])) or {}).get("imdb_id")
            if i and i not in seen:
                seen.add(i)
                if self.scores.get(i) is None:
                    out.append(i)
                    if limit and len(out) >= limit:
                        break
        return out

    def stale_ids(self, key, limit):
        cat = self.get_catalog(key) if key else None
        if not cat:
            return []
        now, out = time.time(), []
        for m in cat["movies"]:
            i = (self.details.get(str(m["id"])) or {}).get("imdb_id")
            s = self.scores.get(i) if i else None
            if s is not None and now - s.get("t", 0) > SCORES_MAX_AGE:
                out.append(i)
                if len(out) >= limit:
                    break
        return out

    # ---- favourites

    def country_name(self, code):
        for r in self.lists.get("regions", {}).get("v", []):
            if r["code"] == code:
                return r["name"]
        return code

    def availability_index(self):
        """tmdb id -> catalog keys it appears in, over every catalog loaded so far."""
        index = {}
        folder = os.path.join(DATA, "catalogs")
        for name in sorted(os.listdir(folder)):
            if not name.endswith(".json"):
                continue
            key = name[:-5]
            cat = self.get_catalog(key)
            if not cat:
                continue
            cached = self.id_sets.get(key)
            if not cached or cached[0] != cat["fetched"]:
                cached = (cat["fetched"], {m["id"] for m in cat["movies"]})
                self.id_sets[key] = cached
            for mid in cached[1]:
                index.setdefault(mid, []).append(key)
        return index

    def favourites(self):
        index = self.availability_index()
        out = []
        for fid, f in self.favs.items():
            mid = int(fid)
            d = self.details.get(fid) or {}
            imdb_id = d.get("imdb_id") or f.get("imdb_id")
            s = self.scores.get(imdb_id or "") or {}
            where = []
            for key in index.get(mid, []):
                country, pid = key.split("_", 1)
                where.append({"country": country, "service": int(pid),
                              "label": f"{self.service_name(country, int(pid))} · {self.country_name(country)}"})
            origin = f"{f.get('country')}_{f.get('service')}"
            out.append({
                "tmdb_id": mid,
                "imdb_id": imdb_id,
                "title": d.get("title") or f.get("title"),
                "title_local": f.get("title_local"),
                "original_title": f.get("original_title"),
                "year": f.get("year"),
                "runtime": d.get("runtime"),
                "genres": d.get("genres", []),
                "overview": d.get("overview") or "",
                "poster": f.get("poster"),
                "imdb": s.get("imdb"), "imdb_votes": s.get("imdb_votes"),
                "rt": s.get("rt"), "mc": s.get("mc"),
                "added": f.get("added"),
                "where": where,
                "origin": {"country": f.get("country"), "service": f.get("service"),
                           "label": f"{self.service_name(f.get('country'), f.get('service'))} · {self.country_name(f.get('country'))}",
                           "available": origin in index.get(mid, [])},
            })
        return out

    def add_favourite(self, body):
        mid = int(body["tmdb_id"])
        country = str(body.get("country") or "")[:2].upper()
        try:
            service = int(body.get("service"))
        except (TypeError, ValueError):
            service = None
        clip = lambda v: (str(v)[:300] if v is not None else None)
        year = body.get("year")
        self.favs.set(str(mid), {
            "added": time.time(),
            "title": clip(body.get("title")),
            "title_local": clip(body.get("title_local")),
            "original_title": clip(body.get("original_title")),
            "year": year if isinstance(year, int) else None,
            "poster": clip(body.get("poster")),
            "imdb_id": clip(body.get("imdb_id")),
            "country": country if re.fullmatch(r"[A-Z]{2}", country) else None,
            "service": service,
        })
        self.favs.save()

    def remove_favourite(self, mid):
        self.favs.pop(str(int(mid)))
        self.favs.save()

    # ---- OMDb daily budget

    def used_today(self):
        today = date.today().isoformat()
        return self.usage.get("count", 0) if self.usage.get("date") == today else 0

    def count_lookup(self, n=1):
        today = date.today().isoformat()
        if self.usage.get("date") != today:
            self.usage.set("date", today)
            self.usage.set("count", 0)
        self.usage.set("count", self.usage.get("count", 0) + n)

    def budget(self):
        return int(self.cfg.get("omdb_daily_budget", 950))


class Scorer(threading.Thread):
    """Looks up OMDb scores in the background for the catalog being viewed."""

    def __init__(self, app):
        super().__init__(daemon=True)
        self.app = app
        self.active = None
        self.state = "idle"
        self.error = None
        self.wake = threading.Event()

    def run(self):
        app = self.app
        while True:
            key = self.active
            if app.used_today() >= app.budget():
                self.state = "limit"
                self.wake.wait(300)
                self.wake.clear()
                continue
            batch = app.pending_ids(key, limit=20) or app.stale_ids(key, 20)
            if not batch:
                self.state = "idle"
                self.wake.wait(60)
                self.wake.clear()
                continue
            self.state, self.error = "running", None
            for imdb_id in batch:
                if self.active != key or app.used_today() >= app.budget():
                    break
                try:
                    res = omdb_lookup(app.cfg["omdb_key"], imdb_id)
                except OmdbLimitReached:
                    app.count_lookup(app.budget())
                    break
                except ApiError as e:
                    self.state, self.error = "error", str(e)
                    self.wake.wait(300)
                    self.wake.clear()
                    break
                except Exception:
                    time.sleep(5)
                    continue
                app.count_lookup()
                if res is not None:
                    app.scores.set(imdb_id, {**res, "t": time.time()})
                time.sleep(0.05)


# ------------------------------------------------------------------ web server

def make_handler(app):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def send(self, code, body, ctype):
            data = body.encode("utf-8") if isinstance(body, str) else body
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(data)

        def json(self, obj, code=200):
            self.send(code, json.dumps(obj, ensure_ascii=False, separators=(",", ":")), "application/json; charset=utf-8")

        def params(self):
            q = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            country = (q.get("country", [""])[0] or "").upper()
            if not re.fullmatch(r"[A-Z]{2}", country):
                country = None
            try:
                pid = int(q.get("service", [""])[0])
            except ValueError:
                pid = None
            try:
                since = float(q.get("since", ["0"])[0])
            except ValueError:
                since = 0.0
            return country, pid, since, q

        def do_GET(self):
            path = urllib.parse.urlparse(self.path).path
            try:
                if path == "/":
                    boot = {"country": app.cfg.get("last_country", "ES"), "service": app.cfg.get("last_service", 8),
                            "favs": [int(k) for k, _ in app.favs.items()]}
                    self.send(200, PAGE.replace("__BOOT__", json.dumps(boot)), "text/html; charset=utf-8")
                elif path == "/api/regions":
                    self.json({"regions": app.regions()})
                elif path == "/api/services":
                    country, _, _, _ = self.params()
                    if not country:
                        return self.json({"error": "Pick a country."}, 400)
                    self.json({"services": app.services(country)})
                elif path == "/api/catalog":
                    self.catalog()
                elif path == "/api/scores":
                    self.scores()
                elif path == "/api/favourites":
                    self.json({"favourites": app.favourites()})
                else:
                    self.send(404, "Not found", "text/plain; charset=utf-8")
            except ApiError as e:
                self.json({"error": str(e)}, 502)
            except (BrokenPipeError, ConnectionResetError):
                pass

        def do_POST(self):
            path = urllib.parse.urlparse(self.path).path
            # Only the page served from this computer may change favourites.
            origin = self.headers.get("Origin")
            host = self.headers.get("Host", "")
            if origin and urllib.parse.urlparse(origin).netloc != host:
                return self.json({"error": "Not allowed."}, 403)
            try:
                length = min(int(self.headers.get("Content-Length") or 0), 100_000)
                body = json.loads(self.rfile.read(length) or b"{}")
                if not isinstance(body, dict):
                    raise ValueError
            except (ValueError, json.JSONDecodeError):
                return self.json({"error": "That request couldn't be read."}, 400)
            try:
                if path == "/api/favourites/add":
                    app.add_favourite(body)
                elif path == "/api/favourites/remove":
                    app.remove_favourite(body["tmdb_id"])
                else:
                    return self.send(404, "Not found", "text/plain; charset=utf-8")
            except (KeyError, TypeError, ValueError):
                return self.json({"error": "That request is missing the movie."}, 400)
            except OSError:
                return self.json({"error": "Your favourites couldn't be saved. Check the data folder can be written to."}, 500)
            self.json({"ok": True, "ids": [int(k) for k, _ in app.favs.items()]})

        def catalog(self):
            country, pid, _, q = self.params()
            if not country or pid is None:
                return self.json({"error": "Pick a country and a service."}, 400)
            key = f"{country}_{pid}"
            app.scorer.active = key
            app.scorer.wake.set()
            app.save_config(last_country=country, last_service=pid)
            cat = app.get_catalog(key)
            job = app.job(key)
            retry = q.get("retry", ["0"])[0] == "1"
            if cat is None:
                if job is None or (job["state"] == "error" and retry):
                    app.start_job(country, pid)
                    job = app.job(key)
                return self.json({"movies": None, "job": job, "service_name": app.service_name(country, pid)})
            if time.time() - cat["fetched"] > CATALOG_MAX_AGE and (job is None or job["state"] != "running"):
                app.start_job(country, pid)
                job = app.job(key)
            if q.get("lite", ["0"])[0] == "1":
                return self.json({"movies": None, "fetched": cat["fetched"], "job": job})
            self.json({"movies": app.assemble(cat), "fetched": cat["fetched"], "job": job,
                       "service_name": app.service_name(country, pid), "now": time.time()})

        def scores(self):
            country, pid, since, _ = self.params()
            key = f"{country}_{pid}" if country and pid is not None else None
            out = {}
            if since:
                for i, s in app.scores.items():
                    if s.get("t", 0) > since:
                        out[i] = {k: s.get(k) for k in ("imdb", "imdb_votes", "rt", "mc")}
            pending = len(app.pending_ids(key)) if key else 0
            self.json({"now": time.time(), "scores": out,
                       "scorer": {"state": app.scorer.state, "error": app.scorer.error,
                                  "active": app.scorer.active == key,
                                  "used_today": app.used_today(), "budget": app.budget(),
                                  "pending": pending}})

    return Handler


def load_config():
    cfg = {**DEFAULT_CONFIG, **load_json(CONFIG_PATH, {})}
    changed = not os.path.exists(CONFIG_PATH)
    if not cfg["tmdb_key"]:
        print("A TMDB key is needed for the catalogs. Get one free at https://www.themoviedb.org/settings/api")
        print('(either the "API Key" or the "API Read Access Token" works)')
        cfg["tmdb_key"] = input("TMDB key: ").strip()
        changed = True
    if not cfg["omdb_key"]:
        print("\nAn OMDb key is needed for the scores. Get one free at https://www.omdbapi.com/apikey.aspx")
        cfg["omdb_key"] = input("OMDb key: ").strip()
        changed = True
    if not cfg["tmdb_key"] or not cfg["omdb_key"]:
        sys.exit("Both keys are needed.")
    if changed:
        save_json(CONFIG_PATH, cfg, pretty=True)
        print(f"\nSaved your keys in {CONFIG_PATH}")
    return cfg


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--port", type=int, help="Port for the local site (default 8000)")
    ap.add_argument("--no-open", action="store_true", help="Don't open the browser")
    args = ap.parse_args()

    cfg = load_config()
    app = App(cfg)
    port = args.port or int(cfg.get("port", 8000))
    try:
        server = ThreadingHTTPServer(("127.0.0.1", port), make_handler(app))
    except OSError:
        sys.exit(f"Port {port} is already in use. Is the site already running? "
                 f"Otherwise start it with --port 8123.")
    server.daemon_threads = True
    url = f"http://localhost:{port}/"
    print(f"\nStreaming by Rating is running at {url}")
    print("Leave this window open while you use it. Press Ctrl+C to stop.\n")
    if not args.no_open:
        threading.Timer(0.8, lambda: webbrowser.open(url)).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        app.save_all()
        print("\nStopped. Everything found so far is saved.")


PAGE = r'''<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Streaming by Rating</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Big+Shoulders+Display:wght@700;800&family=IBM+Plex+Mono:wght@500;600&family=IBM+Plex+Sans:wght@400;500;600&display=swap">
<style>
/* Layout: a source bar (country, service), the title it produces, filters, then a ranked list ending in a four-number scoreboard. */
:root {
  --bg: #EDEFF3; --surface: #FFFFFF; --ink: #15171C; --muted: #5B6170; --line: #D6DAE2;
  --accent: #2740C4; --accent-ink: #FFFFFF; --chip: #E4E8F7;
  --fresh: #1E7A4C; --mixed: #A0640A; --low: #B3332A;
  --fresh-bg: #E2F2E9; --mixed-bg: #F8EDD8; --low-bg: #F7E2DF;
  --f-display: "Big Shoulders Display", "Arial Narrow", "Roboto Condensed", sans-serif;
  --f-body: "IBM Plex Sans", system-ui, -apple-system, "Segoe UI", sans-serif;
  --f-mono: "IBM Plex Mono", ui-monospace, "SF Mono", Menlo, Consolas, monospace;
  color-scheme: light;
}
@media (prefers-color-scheme: dark) { :root {
  --bg: #0F1218; --surface: #171B23; --ink: #E8EAF0; --muted: #9097A6; --line: #2A303C;
  --accent: #8A9DFF; --accent-ink: #0F1218; --chip: #1C2235;
  --fresh: #4CC38A; --mixed: #E2A53A; --low: #F0736A;
  --fresh-bg: #15291F; --mixed-bg: #2C2414; --low-bg: #2E1A18;
  color-scheme: dark;
} }
* { box-sizing: border-box; }
html { -webkit-text-size-adjust: 100%; }
body { margin: 0; background: var(--bg); color: var(--ink); font-family: var(--f-body); font-size: 15px; line-height: 1.5; }
.wrap { max-width: 1020px; margin: 0 auto; padding: 24px max(16px, env(safe-area-inset-right)) 64px max(16px, env(safe-area-inset-left)); }
a { color: var(--accent); }
:focus-visible { outline: 2px solid var(--accent); outline-offset: 2px; }
[hidden] { display: none !important; }

.source { display: flex; flex-wrap: wrap; gap: 10px; align-items: end; padding-bottom: 18px; border-bottom: 1px solid var(--line); }
.source .field { min-width: 200px; flex: 0 1 260px; }
.source select { font-weight: 600; font-size: 15px; padding: 10px 12px; }
h1 { font-family: var(--f-display); font-weight: 800; text-transform: uppercase; font-size: clamp(40px, 8vw, 68px);
  line-height: .9; letter-spacing: .01em; margin: 22px 0 0; text-wrap: balance; overflow-wrap: anywhere; }
h1 .svc { color: var(--accent); }
.status { margin-top: 8px; font-family: var(--f-mono); font-size: 12.5px; color: var(--muted); letter-spacing: .02em; }
.status b { color: var(--ink); font-weight: 600; }
.status .live { color: var(--fresh); }
.note { margin-top: 16px; background: var(--chip); border-radius: 8px; padding: 12px 16px; font-size: 14px; max-width: 80ch; }
.note.err { background: var(--low-bg); }
.note .btn { margin-top: 10px; }

.loading { margin-top: 24px; background: var(--surface); border-radius: 8px; padding: 22px; max-width: 620px; }
.loading .msg { font-weight: 600; }
.loading .sub { font-size: 13.5px; color: var(--muted); margin-top: 4px; }
.bar { margin-top: 14px; height: 8px; border-radius: 4px; background: var(--chip); overflow: hidden; }
.bar span { display: block; height: 100%; width: 0; background: var(--accent); border-radius: 4px; transition: width .4s; }

.field { display: grid; gap: 4px; min-width: 0; }
.field label { font-size: 11px; text-transform: uppercase; letter-spacing: .08em; color: var(--muted); font-weight: 600; }
.field input, .field select { font: 14px var(--f-body); color: var(--ink); background: var(--surface); border: 1px solid var(--line);
  border-radius: 6px; padding: 9px 10px; width: 100%; min-width: 0; }
.source .field select { font-weight: 600; font-size: 15px; padding: 10px 12px; }
.controls { margin-top: 22px; display: grid; grid-template-columns: 2fr repeat(4, minmax(0, 1fr)); gap: 10px; align-items: end; }
.toggles { margin-top: 10px; display: flex; flex-wrap: wrap; gap: 6px 18px; font-size: 14px; color: var(--muted); align-items: center; }
.toggles label { display: inline-flex; gap: 6px; align-items: center; cursor: pointer; }
.count { margin-left: auto; font-family: var(--f-mono); font-size: 12.5px; }
@media (max-width: 760px) {
  .controls { grid-template-columns: 1fr 1fr; }
  .controls .search { grid-column: 1 / -1; }
  .count { margin-left: 0; }
  .source .field { flex: 1 1 100%; min-width: 0; }
}

.legend, li.row { display: grid; grid-template-columns: 40px 46px minmax(0, 1fr) repeat(4, 76px); gap: 4px 12px; }
.legend { margin-top: 18px; padding: 0 14px 6px; font-size: 11px; text-transform: uppercase; letter-spacing: .08em; color: var(--muted); font-weight: 600; }
.legend span:nth-child(n+4) { text-align: center; }
ol.list { list-style: none; margin: 0; padding: 0; display: grid; gap: 6px; }
li.row { background: var(--surface); border-radius: 8px; padding: 12px 14px; align-items: center; }
.rank { font-family: var(--f-display); font-weight: 700; font-size: 26px; color: var(--muted); font-variant-numeric: tabular-nums; line-height: 1; }
.poster { width: 46px; aspect-ratio: 2 / 3; border-radius: 4px; background: var(--chip); object-fit: cover; display: block; max-width: 100%; }
.info { min-width: 0; }
.t { font-weight: 600; font-size: 16px; line-height: 1.3; }
.t .y { font-weight: 400; color: var(--muted); }
.sub { font-size: 13px; color: var(--muted); }
.meta { font-size: 13px; color: var(--muted); display: flex; flex-wrap: wrap; gap: 2px 12px; margin-top: 2px; align-items: center; }
.meta a { text-decoration: none; font-weight: 500; }
.meta a:hover { text-decoration: underline; }
.meta button { background: none; border: 0; padding: 0; font: 500 13px var(--f-body); color: var(--accent); cursor: pointer; }
.plot { font-size: 13.5px; margin: 6px 0 0; max-width: 70ch; }
.scores { display: contents; }
.score { text-align: center; font-family: var(--f-mono); font-variant-numeric: tabular-nums; }
.score .v { display: block; font-size: 17px; font-weight: 600; border-radius: 5px; padding: 4px 0; }
.score .k { display: none; font-size: 10px; letter-spacing: .06em; text-transform: uppercase; color: var(--muted); font-family: var(--f-body); font-weight: 600; }
.score .n { display: block; font-size: 10.5px; color: var(--muted); }
.score.overall .v { font-family: var(--f-display); font-size: 26px; font-weight: 800; padding: 0; }
.fresh .v { color: var(--fresh); background: var(--fresh-bg); }
.mixed .v { color: var(--mixed); background: var(--mixed-bg); }
.low .v { color: var(--low); background: var(--low-bg); }
.none .v { color: var(--muted); }
.score.overall .v { background: none; }
@media (max-width: 640px) {
  .legend { display: none; }
  li.row { grid-template-columns: 30px 46px minmax(0, 1fr); align-items: start; }
  .rank { font-size: 22px; padding-top: 2px; }
  .scores { display: grid; grid-column: 1 / -1; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 8px; margin-top: 8px; }
  .score .k { display: block; }
}
.more { margin-top: 14px; display: flex; justify-content: center; }
.btn { font: 600 14px/1 var(--f-body); border-radius: 6px; padding: 10px 14px; cursor: pointer; border: 1px solid var(--line);
  background: var(--surface); color: var(--ink); }
.btn.primary { background: var(--accent); color: var(--accent-ink); border-color: var(--accent); }
.empty { padding: 32px 16px; text-align: center; color: var(--muted); background: var(--surface); border-radius: 8px; }
nav.views { display: inline-flex; gap: 4px; padding: 4px; background: var(--chip); border-radius: 8px; margin-bottom: 18px; }
.tab { font: 600 14px/1 var(--f-body); color: var(--muted); background: none; border: 0; border-radius: 6px; padding: 9px 14px; cursor: pointer;
  display: inline-flex; gap: 8px; align-items: center; }
.tab[aria-selected="true"] { background: var(--surface); color: var(--ink); }
.tab .badge { font: 600 11.5px/1 var(--f-mono); background: var(--bg); color: var(--ink); border-radius: 4px; padding: 3px 6px; font-variant-numeric: tabular-nums; }
.tab[aria-selected="true"] .badge { background: var(--chip); }
.star { background: none; border: 0; padding: 2px; margin-left: 4px; cursor: pointer; vertical-align: -3px; color: var(--muted); border-radius: 4px; line-height: 0; }
.star svg { width: 19px; height: 19px; display: block; }
.star svg path { fill: none; stroke: currentColor; stroke-width: 1.7; stroke-linejoin: round; }
.star[aria-pressed="true"] { color: var(--mixed); }
.star[aria-pressed="true"] svg path { fill: currentColor; }
.star:hover { color: var(--mixed); }
.avail { font-size: 11.5px; font-weight: 600; border-radius: 4px; padding: 1px 6px; color: var(--fresh); background: var(--fresh-bg); }
.avail.gone { color: var(--low); background: var(--low-bg); }
.avail.unknown { color: var(--muted); background: var(--chip); }
body.favs .source, body.favs .allonly { display: none !important; }
body.favs .controls { grid-template-columns: 2fr minmax(0, 1fr); max-width: 640px; }
footer { margin-top: 32px; font-size: 12.5px; color: var(--muted); max-width: 76ch; }
@media (prefers-reduced-motion: reduce) { .bar span { transition: none; } }
</style>
</head>
<body>
<div class="wrap">
  <nav class="views" role="tablist" aria-label="View">
    <button class="tab" type="button" role="tab" id="tabAll" aria-selected="true">All movies</button>
    <button class="tab" type="button" role="tab" id="tabFavs" aria-selected="false">Favourites <span class="badge" id="favCount">0</span></button>
  </nav>
  <div class="source">
    <div class="field"><label for="country">Country</label><select id="country"><option>Loading…</option></select></div>
    <div class="field"><label for="service">Streaming service</label><select id="service"><option>Loading…</option></select></div>
  </div>

  <h1><span class="svc" id="hService">&nbsp;</span><br><span id="hCountry">&nbsp;</span><span id="hBy"> by rating</span></h1>
  <div class="status" id="status" aria-live="polite"></div>
  <div class="note" id="note" hidden></div>

  <div class="loading" id="loading" hidden>
    <div class="msg" id="loadMsg">Loading…</div>
    <div class="sub" id="loadSub"></div>
    <div class="bar" role="progressbar" aria-label="Loading progress" aria-valuemin="0" aria-valuemax="100" id="barWrap"><span id="bar"></span></div>
  </div>

  <div id="results" hidden>
    <div class="controls">
      <div class="field search"><label for="q">Search</label><input id="q" type="search" placeholder="Any title, original or translated"></div>
      <div class="field"><label for="sort">Sort by</label>
        <select id="sort">
          <option value="overall">Overall score</option>
          <option value="rt">Rotten Tomatoes</option>
          <option value="mc">Metacritic</option>
          <option value="imdb">IMDb</option>
          <option value="year">Newest</option>
          <option value="title">Title A–Z</option>
          <option value="added" id="optAdded" hidden>Recently added</option>
        </select></div>
      <div class="field allonly"><label for="genre">Genre</label><select id="genre"><option value="">All genres</option></select></div>
      <div class="field allonly"><label for="votes">IMDb votes</label>
        <select id="votes">
          <option value="0">Any</option>
          <option value="1000">1,000+</option>
          <option value="10000" selected>10,000+</option>
          <option value="50000">50,000+</option>
          <option value="200000">200,000+</option>
        </select></div>
      <div class="field allonly"><label for="from">Released</label>
        <select id="from">
          <option value="0">Any year</option>
          <option value="2020">2020 or later</option>
          <option value="2010">2010 or later</option>
          <option value="2000">2000 or later</option>
          <option value="1990">1990 or later</option>
        </select></div>
    </div>
    <div class="toggles">
      <label class="allonly"><input type="checkbox" id="needRt" checked> Has a Rotten Tomatoes score</label>
      <label class="allonly"><input type="checkbox" id="hideAnim"> Hide animation</label>
      <span class="count" id="count"></span>
    </div>

    <div class="legend" aria-hidden="true"><span>#</span><span></span><span>Movie</span><span>Tomatoes</span><span>Metacritic</span><span>IMDb</span><span>Overall</span></div>
    <ol class="list" id="list"></ol>
    <div class="more"><button class="btn" id="moreBtn" type="button" hidden>Show more</button></div>
  </div>

  <footer>
    Availability comes from TMDB, which gets it from JustWatch, and can lag the services by a day or two. Scores come from OMDb.
    Overall is the average of the scores a movie has: Rotten Tomatoes critics, Metacritic, and IMDb × 10.
    The IMDb votes filter keeps barely-rated movies from crowding the top.
    This product uses the TMDB API but is not endorsed or certified by TMDB.
  </footer>
</div>

<script>
(function () {
  "use strict";
  var BOOT = __BOOT__;
  var PAGE = 60;
  var $ = function (id) { return document.getElementById(id); };
  var els = { country: $("country"), service: $("service"), q: $("q"), sort: $("sort"), genre: $("genre"),
    votes: $("votes"), from: $("from"), needRt: $("needRt"), hideAnim: $("hideAnim") };
  var state = { country: BOOT.country, service: BOOT.service, movies: [], byImdb: {}, since: 0, shown: PAGE, token: 0,
    serviceName: "", countryName: "", fetched: 0, scorer: null,
    view: "all", favIds: {}, favMovies: [], favLoaded: false, favSort: "overall", allSort: null };
  (BOOT.favs || []).forEach(function (id) { state.favIds[id] = true; });
  var STAR = '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 3.2l2.7 5.6 6.1.8-4.5 4.2 1.1 6.1L12 17l-5.4 2.9 1.1-6.1-4.5-4.2 6.1-.8z"/></svg>';
  var timers = { job: null, scores: null };

  // ---------- helpers
  function esc(s) { return String(s == null ? "" : s).replace(/[&<>"']/g, function (c) { return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]; }); }
  function n(x) { return Number(x).toLocaleString("en"); }
  function overall(m) {
    var s = [];
    if (m.rt != null) s.push(m.rt);
    if (m.mc != null) s.push(m.mc);
    if (m.imdb != null) s.push(m.imdb * 10);
    return s.length ? Math.round(s.reduce(function (a, b) { return a + b; }, 0) / s.length) : null;
  }
  function band(v) { return v == null ? "none" : v >= 75 ? "fresh" : v >= 60 ? "mixed" : "low"; }
  function fmtVotes(v) {
    if (v == null) return "";
    if (v >= 1e6) return (v / 1e6).toFixed(1).replace(/\.0$/, "") + "M votes";
    if (v >= 1e3) return Math.round(v / 1e3) + "k votes";
    return v + " votes";
  }
  function api(path, params) {
    var qs = Object.keys(params || {}).map(function (k) { return k + "=" + encodeURIComponent(params[k]); }).join("&");
    return fetch(path + (qs ? "?" + qs : ""), { cache: "no-store" }).then(function (r) {
      return r.json().catch(function () { return {}; }).then(function (j) {
        if (!r.ok) throw new Error(j.error || "The local server answered with error " + r.status + ".");
        return j;
      });
    }, function () { throw new Error("The site can't reach its local server. Is streaming_ratings.py still running? Start it again, then reload this page."); });
  }
  function showNote(text, isErr, retry) {
    var el = $("note");
    el.hidden = false; el.className = "note" + (isErr ? " err" : "");
    el.innerHTML = esc(text) + (retry ? '<div><button class="btn" type="button" id="retryBtn">Try again</button></div>' : "");
    if (retry) $("retryBtn").addEventListener("click", function () { loadCatalog(true); });
  }
  function hideNote() { $("note").hidden = true; }
  function setHeading() {
    if (state.view === "favs") {
      $("hService").textContent = "Your";
      $("hCountry").textContent = "favourites";
      $("hBy").hidden = true;
      document.title = "Favourites · Streaming by Rating";
      return;
    }
    $("hBy").hidden = false;
    $("hService").textContent = state.serviceName || " ";
    $("hCountry").textContent = state.countryName || " ";
    document.title = (state.serviceName ? state.serviceName + " " + state.countryName + " by Rating" : "Streaming by Rating");
  }
  function stopTimers() { clearTimeout(timers.job); clearTimeout(timers.scores); }

  // ---------- dropdowns
  function loadRegions() {
    return api("/api/regions").then(function (j) {
      els.country.innerHTML = j.regions.map(function (r) {
        return '<option value="' + esc(r.code) + '">' + esc(r.name) + "</option>";
      }).join("");
      if (!j.regions.some(function (r) { return r.code === state.country; })) state.country = "ES";
      els.country.value = state.country;
    });
  }
  function loadServices() {
    els.service.disabled = true;
    return api("/api/services", { country: state.country }).then(function (j) {
      els.service.disabled = false;
      if (!j.services.length) {
        els.service.innerHTML = "<option value=''>No services listed</option>";
        state.service = null;
        return;
      }
      els.service.innerHTML = j.services.map(function (s) { return '<option value="' + s.id + '">' + esc(s.name) + "</option>"; }).join("");
      var ids = j.services.map(function (s) { return s.id; });
      if (ids.indexOf(Number(state.service)) < 0) state.service = ids.indexOf(8) >= 0 ? 8 : ids[0];
      els.service.value = String(state.service);
    });
  }
  function selectedText(sel) { var o = sel.options[sel.selectedIndex]; return o ? o.textContent : ""; }

  els.country.addEventListener("change", function () {
    state.country = els.country.value;
    stopTimers();
    loadServices().then(function () { loadCatalog(false); }).catch(function (e) { showNote(e.message, true); });
  });
  els.service.addEventListener("change", function () {
    state.service = Number(els.service.value);
    loadCatalog(false);
  });

  // ---------- catalog
  function showLoading(job) {
    if (state.view === "favs") return;
    $("results").hidden = true; $("loading").hidden = false;
    var pct = 0, msg = "Getting the " + state.serviceName + " catalog for " + state.countryName + "…", sub = "This takes a minute or so the first time. After that it's instant.";
    if (job && job.step === "catalog" && job.total) { pct = 50 * job.done / job.total; sub = "Page " + n(job.done) + " of " + n(job.total) + " of the catalog."; }
    if (job && job.step === "details") {
      msg = "Getting movie details…";
      pct = 50 + (job.total ? 50 * job.done / job.total : 50);
      sub = job.total ? n(job.done) + " of " + n(job.total) + " movies." : "";
    }
    $("loadMsg").textContent = msg; $("loadSub").textContent = sub;
    $("bar").style.width = Math.round(pct) + "%"; $("barWrap").setAttribute("aria-valuenow", Math.round(pct));
  }

  function loadCatalog(retry) {
    stopTimers();
    hideNote();
    if (!state.service) { $("results").hidden = true; $("loading").hidden = true; showNote("TMDB lists no streaming services for this country.", false); return; }
    var token = ++state.token;
    state.serviceName = selectedText(els.service); state.countryName = selectedText(els.country);
    setHeading();
    $("status").textContent = "";
    try { history.replaceState(null, "", "?country=" + state.country + "&service=" + state.service + (state.view === "favs" ? "#favourites" : "")); } catch (e) {}
    var params = { country: state.country, service: state.service };
    if (retry) params.retry = 1;
    api("/api/catalog", params).then(function (j) {
      if (token !== state.token) return;
      if (j.movies) { useCatalog(j); return; }
      handleJob(j.job, token);
    }).catch(function (e) { if (token === state.token) { $("loading").hidden = true; showNote(e.message, true, true); } });
  }

  function handleJob(job, token) {
    if (job && job.state === "error") { $("loading").hidden = true; showNote(job.error, true, true); return; }
    showLoading(job);
    timers.job = setTimeout(function () {
      api("/api/catalog", { country: state.country, service: state.service, lite: 1 }).then(function (j) {
        if (token !== state.token) return;
        if (j.job && j.job.state === "done") { loadCatalog(false); return; }
        handleJob(j.job, token);
      }).catch(function (e) { if (token === state.token) showNote(e.message, true, true); });
    }, 1000);
  }

  function useCatalog(j) {
    $("loading").hidden = true;
    if (state.view === "all") $("results").hidden = false;
    state.movies = j.movies; state.fetched = j.fetched; state.since = j.now || 0; state.shown = PAGE;
    state.byImdb = {};
    state.movies.forEach(function (m) {
      m._o = overall(m); m._s = [m.title, m.title_local, m.original_title].join(" ").toLowerCase();
      if (m.imdb_id) (state.byImdb[m.imdb_id] = state.byImdb[m.imdb_id] || []).push(m);
    });
    buildGenres();
    if (state.view === "all") { render(); updateStatus(); }
    pollScores(state.token);
    // A catalog older than a day refreshes in the background; reload it quietly when done.
    if (j.job && j.job.state === "running") watchRefresh(state.token);
  }

  function watchRefresh(token) {
    timers.job = setTimeout(function () {
      api("/api/catalog", { country: state.country, service: state.service, lite: 1 }).then(function (j) {
        if (token !== state.token) return;
        if (j.job && j.job.state === "running") watchRefresh(token);
        else if (j.job && j.job.state === "done" && j.fetched > state.fetched) {
          api("/api/catalog", { country: state.country, service: state.service }).then(function (k) {
            if (token === state.token && k.movies) { var keep = state.shown; useCatalog(k); state.shown = keep; render(); }
          });
        }
      }).catch(function () {});
    }, 3000);
  }

  // ---------- live scores
  function pollScores(token) {
    clearTimeout(timers.scores);
    api("/api/scores", { country: state.country, service: state.service, since: state.since }).then(function (j) {
      if (token !== state.token) return;
      state.since = j.now; state.scorer = j.scorer;
      var changed = false;
      Object.keys(j.scores).forEach(function (id) {
        (state.byImdb[id] || []).forEach(function (m) {
          var s = j.scores[id];
          m.imdb = s.imdb; m.imdb_votes = s.imdb_votes; m.rt = s.rt; m.mc = s.mc; m._o = overall(m);
          changed = true;
        });
      });
      if (changed && state.view === "all") render();
      if (state.view === "all") updateStatus();
      var busy = j.scorer.state === "running" && j.scorer.pending > 0;
      timers.scores = setTimeout(function () { pollScores(token); }, busy ? 8000 : 60000);
    }).catch(function () {
      if (token === state.token) timers.scores = setTimeout(function () { pollScores(token); }, 15000);
    });
  }

  function updateStatus() {
    var total = state.movies.length;
    var scored = state.movies.filter(function (m) { return m._o != null; }).length;
    var when = state.fetched ? new Date(state.fetched * 1000).toLocaleDateString("en-GB", { day: "numeric", month: "short" }) : "";
    var parts = ["<b>" + n(total) + "</b> movies", "<b>" + n(scored) + "</b> with scores"];
    var s = state.scorer;
    if (s && s.pending > 0) {
      if (s.state === "running") parts.push('<span class="live">● scoring ' + n(s.pending) + " more</span>");
      else if (s.state === "limit") parts.push(n(s.pending) + " more after today's OMDb limit");
    }
    if (when) parts.push("catalog from " + esc(when));
    $("status").innerHTML = parts.join(" · ");
    if (s && s.state === "error" && s.error) showNote(s.error, true);
    else if (s && s.state === "limit" && s.pending > 0) {
      showNote("OMDb's free key allows " + n(s.budget) + " score lookups a day, and today's are used up. " +
        "Leave the site running, or start it again tomorrow, and the rest fill in. The most popular movies are scored first.", false);
    } else if ($("note").className === "note") hideNote();
  }

  // ---------- list
  function buildGenres() {
    var genres = {}, cur = els.genre.value;
    state.movies.forEach(function (m) { (m.genres || []).forEach(function (g) { genres[g] = (genres[g] || 0) + 1; }); });
    els.genre.innerHTML = '<option value="">All genres</option>' + Object.keys(genres).sort().map(function (g) {
      return '<option value="' + esc(g) + '">' + esc(g) + " (" + genres[g] + ")</option>";
    }).join("");
    if (genres[cur]) els.genre.value = cur;
  }

  function filtered() {
    var q = els.q.value.trim().toLowerCase(), g = els.genre.value, minV = +els.votes.value, from = +els.from.value, key = els.sort.value;
    var favView = state.view === "favs";
    var list = (favView ? state.favMovies : state.movies).filter(function (m) {
      if (q && m._s.indexOf(q) < 0) return false;
      if (favView) return true;
      if (g && (m.genres || []).indexOf(g) < 0) return false;
      if (minV && !(m.imdb_votes >= minV)) return false;
      if (from && !(m.year >= from)) return false;
      if (els.needRt.checked && m.rt == null) return false;
      if (els.hideAnim.checked && (m.genres || []).indexOf("Animation") >= 0) return false;
      return true;
    });
    var f = { overall: "_o", rt: "rt", mc: "mc", imdb: "imdb", year: "year" }[key];
    list.sort(function (a, b) {
      if (key === "title") return String(a.title).localeCompare(String(b.title));
      if (key === "added") return (b.added || 0) - (a.added || 0);
      var x = a[f], y = b[f];
      if (x == null && y == null) return (b._o || 0) - (a._o || 0);
      if (x == null) return 1;
      if (y == null) return -1;
      return (y - x) || ((b._o || 0) - (a._o || 0)) || ((b.imdb_votes || 0) - (a.imdb_votes || 0));
    });
    return list;
  }

  function cell(label, v, shown, extra, cls) {
    return '<div class="score ' + (cls || "") + " " + band(v) + '"><span class="k">' + label + '</span><span class="v">' +
      (shown == null ? "–" : shown) + "</span>" + (extra ? '<span class="n">' + extra + "</span>" : "") + "</div>";
  }

  function watchLink(m) {
    if (state.view === "favs") {
      var c = (m.origin && m.origin.country) || state.country;
      return '<a href="https://www.themoviedb.org/movie/' + m.tmdb_id + "/watch?locale=" + encodeURIComponent(c) + '" target="_blank" rel="noopener">Where to watch ↗</a>';
    }
    if (/^netflix/i.test(state.serviceName))
      return '<a href="https://www.netflix.com/search?q=' + encodeURIComponent(m.title_local || m.title) + '" target="_blank" rel="noopener">Netflix ↗</a>';
    return '<a href="https://www.themoviedb.org/movie/' + m.tmdb_id + "/watch?locale=" + encodeURIComponent(state.country) + '" target="_blank" rel="noopener">Where to watch ↗</a>';
  }

  function render() {
    var list = filtered(), out = [];
    $("count").textContent = n(list.length) + " match" + (list.length === 1 ? "" : "es");
    list.slice(0, state.shown).forEach(function (m, i) {
      var meta = [];
      if (state.view === "favs") meta = meta.concat(availChips(m));
      if (m.genres && m.genres.length) meta.push("<span>" + esc(m.genres.slice(0, 3).join(", ")) + "</span>");
      if (m.runtime) meta.push("<span>" + m.runtime + " min</span>");
      meta.push(watchLink(m));
      if (m.imdb_id) meta.push('<a href="https://www.imdb.com/title/' + encodeURIComponent(m.imdb_id) + '/" target="_blank" rel="noopener">IMDb ↗</a>');
      if (m.overview) meta.push('<button type="button" data-plot="' + m.tmdb_id + '">Plot</button>');
      var local = m.title_local && m.title_local !== m.title ? '<div class="sub">' + esc(m.title_local) + "</div>" : "";
      var poster = m.poster ? '<img class="poster" loading="lazy" alt="" src="https://image.tmdb.org/t/p/w92' + esc(m.poster) + '">' : '<div class="poster"></div>';
      var fav = !!state.favIds[m.tmdb_id];
      var star = '<button type="button" class="star" data-fav="' + m.tmdb_id + '" aria-pressed="' + fav + '" aria-label="' +
        (fav ? "Remove from favourites" : "Add to favourites") + '" title="' + (fav ? "Remove from favourites" : "Add to favourites") + '">' + STAR + "</button>";
      out.push('<li class="row"><div class="rank">' + (i + 1) + "</div>" + poster + '<div class="info"><div class="t">' + esc(m.title) +
        (m.year ? ' <span class="y">' + m.year + "</span>" : "") + star + "</div>" + local + '<div class="meta">' + meta.join("") + "</div>" +
        (m.overview ? '<p class="plot"' + (openPlots[m.tmdb_id] ? "" : " hidden") + ' id="plot-' + m.tmdb_id + '">' + esc(m.overview) + "</p>" : "") + '</div><div class="scores">' +
        cell("RT", m.rt, m.rt == null ? null : m.rt + "%") +
        cell("Meta", m.mc, m.mc) +
        cell("IMDb", m.imdb == null ? null : m.imdb * 10, m.imdb == null ? null : m.imdb.toFixed(1), fmtVotes(m.imdb_votes)) +
        cell("Overall", m._o, m._o, "", "overall") + "</div></li>");
    });
    var emptyMsg = state.view === "favs"
      ? (state.favMovies.length ? "No favourites match your search." : "No favourites yet. Tap the star next to a movie's title to add it here.")
      : state.movies.length
      ? 'No movies match these filters. Try fewer IMDb votes, another genre, or unticking "Has a Rotten Tomatoes score". Movies still waiting for scores show up once they have them.'
      : "TMDB lists no movies on this service in this country.";
    $("list").innerHTML = out.length ? out.join("") : '<li class="empty">' + esc(emptyMsg) + "</li>";
    $("moreBtn").hidden = list.length <= state.shown;
    $("moreBtn").textContent = "Show " + Math.min(PAGE, list.length - state.shown) + " more";
  }

  var openPlots = {};
  $("list").addEventListener("click", function (e) {
    var st = e.target.closest("[data-fav]");
    if (st) { toggleFav(Number(st.getAttribute("data-fav"))); return; }
    var b = e.target.closest("[data-plot]");
    if (!b) return;
    var id = b.getAttribute("data-plot"), p = $("plot-" + id);
    if (p) { p.hidden = !p.hidden; openPlots[id] = !p.hidden; }
  });
  $("list").addEventListener("error", function (e) {
    if (e.target.tagName === "IMG") { var d = document.createElement("div"); d.className = "poster"; e.target.replaceWith(d); }
  }, true);
  $("moreBtn").addEventListener("click", function () { state.shown += PAGE; render(); });
  var timer;
  els.q.addEventListener("input", function () { clearTimeout(timer); timer = setTimeout(function () { state.shown = PAGE; render(); }, 150); });
  ["sort", "genre", "votes", "from", "needRt", "hideAnim"].forEach(function (k) {
    els[k].addEventListener("change", function () { state.shown = PAGE; render(); saveFilters(); });
  });

  // ---------- favourites
  function favCount() { return Object.keys(state.favIds).length; }
  function updateFavCount() { $("favCount").textContent = favCount(); }

  function availChips(m) {
    var out = [], cur = state.country + "_" + state.service;
    if (m.origin && m.origin.label && !m.origin.available)
      out.push('<span class="avail gone">No longer on ' + esc(m.origin.label) + "</span>");
    (m.where || []).forEach(function (w) {
      out.push('<span class="avail">On ' + esc(w.label) + "</span>");
    });
    if (!out.length) out.push('<span class="avail unknown">Not on any catalog you\'ve loaded</span>');
    return out;
  }

  function toggleFav(id) {
    var adding = !state.favIds[id];
    var src = state.view === "favs" ? state.favMovies : state.movies;
    var m = src.filter(function (x) { return x.tmdb_id === id; })[0];
    if (adding) state.favIds[id] = true; else delete state.favIds[id];
    updateFavCount();
    if (state.view === "favs" && !adding) state.favMovies = state.favMovies.filter(function (x) { return x.tmdb_id !== id; });
    state.favLoaded = state.favLoaded && state.view === "favs";
    render();
    if (state.view === "favs") updateFavStatus();
    var body = adding && m ? { tmdb_id: id, title: m.title, title_local: m.title_local, original_title: m.original_title, year: m.year,
      poster: m.poster, imdb_id: m.imdb_id, country: state.country, service: state.service } : { tmdb_id: id };
    fetch(adding ? "/api/favourites/add" : "/api/favourites/remove", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) })
      .then(function (r) { return r.json().then(function (j) { if (!r.ok) throw new Error(j.error || "Error " + r.status); return j; }); })
      .then(function (j) {
        state.favIds = {}; j.ids.forEach(function (x) { state.favIds[x] = true; }); updateFavCount();
      })
      .catch(function (e) {
        if (adding) delete state.favIds[id]; else state.favIds[id] = true;
        updateFavCount();
        if (state.view === "favs") loadFavourites(); else render();
        showNote((adding ? "Couldn't add that favourite. " : "Couldn't remove that favourite. ") +
          (e.message.indexOf("Failed to fetch") >= 0 ? "Is streaming_ratings.py still running?" : e.message), true);
      });
  }

  function loadFavourites() {
    return api("/api/favourites").then(function (j) {
      state.favMovies = j.favourites;
      state.favMovies.forEach(function (m) { m._o = overall(m); m._s = [m.title, m.title_local, m.original_title].join(" ").toLowerCase(); });
      state.favIds = {}; state.favMovies.forEach(function (m) { state.favIds[m.tmdb_id] = true; });
      state.favLoaded = true;
      updateFavCount();
      if (state.view === "favs") { render(); updateFavStatus(); }
    }).catch(function (e) { if (state.view === "favs") showNote(e.message, true); });
  }

  function updateFavStatus() {
    var total = state.favMovies.length, cur = state.country + "_" + state.service;
    var here = state.favMovies.filter(function (m) { return (m.where || []).some(function (w) { return w.country + "_" + w.service === cur; }); }).length;
    var gone = state.favMovies.filter(function (m) { return m.origin && !m.origin.available; }).length;
    var parts = ["<b>" + n(total) + "</b> favourite" + (total === 1 ? "" : "s")];
    if (total && state.serviceName) parts.push("<b>" + n(here) + "</b> on " + esc(state.serviceName) + " · " + esc(state.countryName));
    if (gone) parts.push("<b>" + n(gone) + "</b> no longer where you found " + (gone === 1 ? "it" : "them"));
    $("status").innerHTML = parts.join(" · ");
  }

  function setView(view) {
    if (view === state.view) return;
    // Each view keeps its own sort order.
    if (state.view === "favs") state.favSort = els.sort.value; else state.allSort = els.sort.value;
    state.view = view;
    document.body.classList.toggle("favs", view === "favs");
    $("tabAll").setAttribute("aria-selected", view === "all");
    $("tabFavs").setAttribute("aria-selected", view === "favs");
    $("optAdded").hidden = view !== "favs";
    els.sort.value = view === "favs" ? state.favSort : (state.allSort || "overall");
    if (els.sort.value === "" ) els.sort.value = "overall";
    state.shown = PAGE; hideNote(); setHeading();
    try { history.replaceState(null, "", location.pathname + location.search + (view === "favs" ? "#favourites" : "")); } catch (e) {}
    if (view === "favs") {
      $("loading").hidden = true; $("results").hidden = false;
      render(); updateFavStatus();
      loadFavourites();
    } else {
      loadCatalog(false);
    }
    window.scrollTo(0, 0);
  }
  $("tabAll").addEventListener("click", function () { setView("all"); });
  $("tabFavs").addEventListener("click", function () { setView("favs"); });
  window.addEventListener("hashchange", function () {
    var v = location.hash === "#favourites" ? "favs" : "all";
    if (v === "favs" && !state.serviceName) { state.serviceName = selectedText(els.service); state.countryName = selectedText(els.country); }
    setView(v);
  });

  function saveFilters() {
    if (state.view === "favs") { state.favSort = els.sort.value; }
    try { localStorage.setItem("sbr-filters", JSON.stringify({ sort: state.view === "favs" ? (state.allSort || "overall") : els.sort.value, favSort: state.favSort, votes: els.votes.value, from: els.from.value, needRt: els.needRt.checked, hideAnim: els.hideAnim.checked })); } catch (e) {}
  }
  function restoreFilters() {
    try {
      var f = JSON.parse(localStorage.getItem("sbr-filters") || "null");
      if (!f) return;
      ["sort", "votes", "from"].forEach(function (k) {
        if (f[k] != null && Array.prototype.some.call(els[k].options, function (o) { return o.value === f[k]; })) els[k].value = f[k];
      });
      els.needRt.checked = f.needRt !== false; els.hideAnim.checked = !!f.hideAnim;
      if (["overall", "rt", "mc", "imdb", "year", "title", "added"].indexOf(f.favSort) >= 0) state.favSort = f.favSort;
    } catch (e) {}
  }

  // ---------- start
  (function () {
    var m = /[?&]country=([A-Za-z]{2})/.exec(location.search), s = /[?&]service=(\d+)/.exec(location.search);
    if (m) state.country = m[1].toUpperCase();
    if (s) state.service = Number(s[1]);
  })();
  restoreFilters();
  updateFavCount();
  var startFavs = location.hash === "#favourites";
  loadRegions().then(loadServices).then(function () {
    if (startFavs) {
      state.serviceName = selectedText(els.service); state.countryName = selectedText(els.country);
      setView("favs");
    } else loadCatalog(false);
  }).catch(function (e) { showNote(e.message, true); });
})();
</script>
</body>
</html>
'''

if __name__ == "__main__":
    main()
