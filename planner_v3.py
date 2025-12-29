import streamlit as st
import pandas as pd
import datetime
from datetime import date, timedelta
import requests
import re
import math
import asyncio
import aiohttp
import json
import os
import gzip

# --- CONFIGURATION ---
if 'reset_id' not in st.session_state:
    st.session_state.reset_id = 0

def trigger_reset():
    st.session_state.reset_id += 1
    st.rerun()

# ============================================================
# 1. DATA: RIVER SPECS & HUBS
# ============================================================

RIVER_REGIONS = {
    "Northern California": [
        {"Name": "Gualala", "ID": "11467510", "T": "250-1200 cfs", "Hub": "Pepperwood"},
        {"Name": "Garcia", "ID": "11467600", "T": "250-1100 cfs", "Hub": "Pepperwood"},
        {"Name": "Navarro", "ID": "11468000", "T": "350-1500 cfs", "Hub": "Pepperwood"},
        {"Name": "Mattole", "ID": "11468900", "T": "500-3000 cfs", "Hub": "Pepperwood"},
        {"Name": "SF Eel", "ID": "11476500", "T": "800-4000 cfs", "Hub": "Pepperwood"},
        {"Name": "Eel (Main)", "ID": "11477000", "T": "2000-15000 cfs", "Hub": "Pepperwood"},
        {"Name": "Van Duzen", "ID": "11478500", "T": "500-2500 cfs", "Hub": "Maple Grove"},
        {"Name": "Mad River", "ID": "11481000", "T": "600-3500 cfs", "Hub": "Eureka"},
        {"Name": "Redwood Creek", "ID": "11482500", "T": "400-1500 cfs", "Hub": "Eureka"},
        {"Name": "Smith River (CA)", "ID": "11532500", "T": "1500-7500 cfs", "Hub": "Hiouchi"},
    ],
    "Southern Oregon Coast": [
        {"Name": "Winchuck", "ID": "14400200", "T": "250-900 cfs", "Hub": "Brookings"},
        {"Name": "Chetco", "ID": "14400000", "T": "1500-5000 cfs", "Hub": "Brookings"},
        {"Name": "Pistol", "ID": "14400000", "T": "300-1100 cfs", "Hub": "Gold Beach"},
        {"Name": "Illinois", "ID": "14377100", "T": "1000-4000 cfs", "Hub": "Gold Beach"},
        {"Name": "Rogue", "ID": "14372300", "T": "2000-8000 cfs", "Hub": "Gold Beach"},
        {"Name": "Elk River", "ID": "14327250", "T": "4.0-6.0 ft", "P": "00065", "Hub": "Port Orford"},
        {"Name": "Sixes River", "ID": "14327150", "T": "5.0-9.0 ft", "P": "00065", "Hub": "Port Orford"},
        {"Name": "Floras/New", "ID": "14327137", "T": "300-1100 cfs", "Hub": "Port Orford"},
    ],
    "Central Oregon Coast": [
        {"Name": "Coquille (S.F.)", "ID": "14325000", "T": "4.0-9.0 ft", "P": "00065", "Hub": "Coos Bay"},
        {"Name": "Coquille (Main)", "ID": "14326500", "T": "2500-8500 cfs", "Hub": "Coos Bay"},
        {"Name": "Tenmile", "ID": "NO_GAUGE", "T": "250-900 cfs", "Hub": "Coos Bay"},
        {"Name": "Umpqua (Main)", "ID": "14321000", "T": "3000-12000 cfs", "Hub": "Reedsport"},
        {"Name": "N. Umpqua", "ID": "14319500", "T": "1200-4000 cfs", "Hub": "Steamboat"},
        {"Name": "Siuslaw", "ID": "14307620", "T": "4.0-10.0 ft", "P": "00065", "Hub": "Reedsport"},
    ],
    "Northern Oregon Coast": [
        {"Name": "Alsea", "ID": "14306500", "T": "3.0-9.0 ft", "P": "00065", "Hub": "Newport"},
        {"Name": "Siletz", "ID": "14305500", "T": "3.5-8.0 ft", "P": "00065", "Hub": "Newport"},
        {"Name": "Nestucca", "ID": "14303600", "T": "3.5-7.5 ft", "P": "00065", "Hub": "Tillamook"},
        {"Name": "Wilson", "ID": "14301500", "T": "3.5-7.5 ft", "P": "00065", "Hub": "Tillamook"},
        {"Name": "Nehalem", "ID": "14301000", "T": "3.5-9.0 ft", "P": "00065", "Hub": "Tillamook"},
    ],
    "Washington Coast": [
        {"Name": "Willapa", "ID": "12010000", "T": "800-2600 cfs", "Hub": "Aberdeen"},
        {"Name": "Satsop", "ID": "12035000", "T": "1500-7000 cfs", "Hub": "Aberdeen"},
        {"Name": "Wynoochee", "ID": "12037400", "T": "1000-5000 cfs", "Hub": "Aberdeen"},
        {"Name": "Humptulips", "ID": "12039005", "T": "1500-6000 cfs", "Hub": "Aberdeen"},
    ],
    "Olympic Peninsula": [
        {"Name": "Quinault", "ID": "12039500", "T": "2000-10000 cfs", "Hub": "Forks"},
        {"Name": "Queets", "ID": "12040500", "T": "2000-8000 cfs", "Hub": "Queets"},
        {"Name": "Hoh", "ID": "12041200", "T": "1500-6000 cfs", "Hub": "Hoh"},
        {"Name": "Bogachiel", "ID": "12043015", "T": "1000-6000 cfs", "Hub": "Bogachiel"},
        {"Name": "Calawah", "ID": "12043000", "T": "1000-4000 cfs", "Hub": "Forks"},
        {"Name": "Sol Duc", "ID": "12041500", "T": "600-2400 cfs", "Hub": "Forks"},
    ]
}

NODE_COORDS = {
    "Home": {"lat": 37.472, "lon": -105.879},
    "Delta": {"lat": 39.352, "lon": -112.575},
    "Delta, UT": {"lat": 39.352, "lon": -112.575}, 
    "Pyramid": {"lat": 39.95, "lon": -119.60},
    "Eagle": {"lat": 40.55, "lon": -120.80},
    "Pepperwood": {"lat": 40.48, "lon": -124.03},
    "Maple Grove": {"lat": 40.52, "lon": -123.90},
    "Hiouchi": {"lat": 41.79, "lon": -124.07},
    "Eureka": {"lat": 40.80, "lon": -124.16},
    "Brookings": {"lat": 42.05, "lon": -124.27},
    "Gold Beach": {"lat": 42.40, "lon": -124.42},
    "Port Orford": {"lat": 42.74, "lon": -124.49},
    "Coos Bay": {"lat": 43.36, "lon": -124.21},
    "Reedsport": {"lat": 43.70, "lon": -124.09},
    "Newport": {"lat": 44.63, "lon": -124.05},
    "Tillamook": {"lat": 45.45, "lon": -123.84},
    "Steamboat": {"lat": 43.34, "lon": -122.73},
    "Aberdeen": {"lat": 46.97, "lon": -123.81},
    "Forks": {"lat": 47.95, "lon": -124.38},
    "Bogachiel": {"lat": 47.93, "lon": -124.45},
    "Hoh": {"lat": 47.86, "lon": -124.25},
    "Queets": {"lat": 47.53, "lon": -124.33}
}

BASE_GAS_PRICES = {
    "Home": 2.99, "Delta": 3.69, "Pyramid": 4.25, "Eagle": 4.10, "Pepperwood": 4.79, "Maple Grove": 4.79,
    "Hiouchi": 4.85, "Eureka": 4.79, "Brookings": 3.69, "Gold Beach": 3.89,
    "Port Orford": 3.99, "Coos Bay": 3.59, "Reedsport": 3.69, "Newport": 3.75, "Tillamook": 3.79,
    "Steamboat": 4.50, "Aberdeen": 4.19,
    "Forks": 4.39, "Bogachiel": 4.39, "Hoh": 4.45, "Queets": 4.50
}

HUB_TO_REGION = {}
for reg, rivers in RIVER_REGIONS.items():
    for r in rivers:
        HUB_TO_REGION[r['Hub']] = reg
HUB_TO_REGION['Pyramid'] = 'Pyramid'
HUB_TO_REGION['Eagle'] = 'Eagle'

# ============================================================
# 2. INTELLIGENCE (ASYNC FLOWS & WEATHER)
# ============================================================

async def fetch_usgs_series_async(session, site_id, param, period="P2D"):
    if site_id == "NO_GAUGE": return None
    try:
        url = "https://waterservices.usgs.gov/nwis/iv/"
        params = {"format": "json", "sites": site_id, "parameterCd": param, "period": period}
        async with session.get(url, params=params, timeout=6) as resp:
            if resp.status == 200:
                data = await resp.json()
                vals = data['value']['timeSeries'][0]['values'][0]['value']
                series = []
                for v in vals:
                    dt_val = datetime.datetime.fromisoformat(v["dateTime"].replace("Z", "+00:00"))
                    series.append((dt_val, float(v["value"])))
                return series
    except:
        return None
    return None

async def fetch_weather_async(session, name, lat, lon):
    headers = {"User-Agent": "SteelheadNavigator"}
    try:
        point_url = f"https://api.weather.gov/points/{round(lat, 4)},{round(lon, 4)}"
        async with session.get(point_url, headers=headers, timeout=5) as resp1:
            if resp1.status != 200: return (name, [])
            point_data = await resp1.json()
            forecast_url = point_data['properties']['forecast']
            
        async with session.get(forecast_url, headers=headers, timeout=5) as resp2:
            if resp2.status != 200: return (name, [])
            forecast_data = await resp2.json()
            return (name, forecast_data['properties']['periods'])
    except:
        return (name, [])

async def fetch_all_data_async(weather_locs):
    connector = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        flow_tasks = []
        flow_keys = []
        for reg, rivers in RIVER_REGIONS.items():
            for r in rivers:
                flow_keys.append(r["Name"])
                flow_tasks.append(fetch_usgs_series_async(session, r["ID"], r.get("P", "00060")))
        
        weather_tasks = []
        for name, lat, lon in weather_locs:
            weather_tasks.append(fetch_weather_async(session, name, lat, lon))
            
        flow_results = await asyncio.gather(*flow_tasks)
        weather_results = await asyncio.gather(*weather_tasks)
        
        return {
            "flows": dict(zip(flow_keys, flow_results)),
            "weather": dict(weather_results)
        }

@st.cache_data(ttl=900)
def get_live_data():
    weather_locs = [
        ("Pyramid", 40.01, -119.62), 
        ("Eureka", 40.80, -124.16),
        ("Crescent City", 41.75, -124.20),
        ("Brookings", 42.05, -124.27), 
        ("Coos Bay", 43.36, -124.21),
        ("Tillamook", 45.45, -123.84),
        ("Forks", 47.95, -124.38)
    ]
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop.run_until_complete(fetch_all_data_async(weather_locs))
    except:
        return {"flows": {}, "weather": {}}

# --- SCORING & UTILS ---

def parse_range(t_str):
    try:
        clean = t_str.lower().replace("cfs","").replace("ft","").strip()
        parts = clean.split("-")
        return float(parts[0]), float(parts[1])
    except:
        return 0, 99999

def get_trend(series):
    if not series or len(series) < 2: return "stable"
    series.sort(key=lambda x: x[0])
    
    last = series[-1]
    start = None
    target_time = last[0] - timedelta(hours=6)
    for p in reversed(series):
        if p[0] <= target_time:
            start = p
            break
    if not start: start = series[0]
    
    if start[1] == 0: return "stable"
    pct_change = (last[1] - start[1]) / start[1]
    
    if pct_change < -0.01: return "dropping"
    if pct_change > 0.01: return "rising"
    return "stable"

def auto_score(series, spec, weather_periods=None):
    if not series:
        if weather_periods:
            first = weather_periods[0]["detailedForecast"].lower()
            if "rain" in first and ("heavy" in first or "100%" in first):
                return 1.0, "🧪 Rising (Est)"
            if "rain" in first:
                return 2.5, "🧪 Rain (Est)"
            return 2.0, "🧪 Low/Stable (Est)"
        return 2.5, "🧪 Unknown"

    val = series[-1][1]
    trend = get_trend(series)
    low, high = parse_range(spec["T"])
    abs_low = spec.get("Low", 0)
    
    if val < abs_low:
        return 0.0, f"📡 Too Low ({val})"
    
    if val < low:
        if val >= (0.8 * low):
            arrow = "↑" if trend == "rising" else "↓" if trend == "dropping" else "↔"
            return 3.75, f"📡 {arrow} Near Ideal ({val})"
        if trend == "rising": return 3.0, f"📡 ↑ Rising ({val})"
        return 2.0, f"📡 Low ({val})"
        
    if low <= val <= high:
        if trend == "dropping": return 5.0, f"📡 ↓ Perfect Drop ({val})"
        return 4.5, f"📡 In Shape ({val})"
        
    if high < val <= (high * 1.4) and trend == "dropping":
        return 4.5, f"📡 ↓ Dropping In ({val})"
    elif high < val <= (high * 1.2):
        if trend == "rising": return 2.0, f"📡 ↑ Rising High ({val})"
        return 3.5, f"📡 Slightly High ({val})"
        
    if val > (high * 1.2):
        return 1.0, f"📡 Blown Out ({val})"
        
    return 2.5, f"📡 {val}"

def format_precip_text(txt: str) -> str:
    lower = txt.lower()
    m = re.search(r"(amounts? (of|between) .+? (possible|expected))", lower)
    if m:
        phrase = m.group(1).replace("amounts of", "").replace("amounts between", "").strip()
        phrase = re.sub(r'\s+', ' ', phrase).strip()
        if phrase: return f'💧 Precip: {phrase} possible'
    if "rain" in lower or "showers" in lower: return "💧 Precip: rain possible"
    if "snow" in lower: return "💧 Precip: snow possible"
    return "Precip: none/min"

# ============================================================
# 3. ROUTING & ITINERARY (UPDATED FOR COMPRESSED JSON & INJECTION)
# ============================================================

@st.cache_data
def load_routes():
    """Load pre-calculated routes with geometry from JSON or GZIP JSON.
       Also INJECTS missing routes manually if they don't exist.
    """
    data = {}
    
    # 1. Try Loading .json.gz (Compressed - Preferred for deploy)
    if os.path.exists('routes.json.gz'):
        try:
            with gzip.open('routes.json.gz', 'rt', encoding='utf-8') as f:
                data = json.load(f)
        except Exception as e:
            st.warning(f"Error loading routes.json.gz: {e}")

    # 2. Try Loading .json (Uncompressed - Dev fallback)
    elif os.path.exists('routes.json'):
        try:
            with open('routes.json', 'r') as f:
                data = json.load(f)
        except Exception as e:
            st.warning(f"Error loading routes.json: {e}")
    else:
        st.warning("No route database found (routes.json or routes.json.gz).")

    # 3. MANUAL INJECTION of Critical Missing Routes
    #    (Uses simple straight lines if complex geometry unavailable)
    
    if "Home|Delta, UT" not in data:
        data["Home|Delta, UT"] = {
            "miles": 524.8,  
            "hours": 9.72,
            "geometry": [[-105.879, 37.472], [-112.575, 39.352]] 
        }
    
    if "Pyramid|Delta, UT" not in data:
        data["Pyramid|Delta, UT"] = {
            "miles": 474.8, 
            "hours": 8.61, 
            "geometry": [[-119.60, 39.95], [-112.575, 39.352]]
        }

    return data

ROUTES_DB = load_routes()

def get_routing_info(loc1, loc2):
    """
    Get distance, time, and geometry between two hubs.
    Returns: (miles, hours, geometry_list)
    """
    if loc1 == loc2: return 0, 0.0, []
    
    # Fuzzy match logic for keys
    potential_keys = [
        f"{loc1}|{loc2}",
        f"{loc2}|{loc1}",
        f"{loc1}|{loc2}, UT", # Handle potential "Delta, UT" mismatch
        f"{loc2}, UT|{loc1}",
        f"{loc1}, UT|{loc2}",
        f"{loc2}|{loc1}, UT"
    ]
    
    for key in potential_keys:
        if key in ROUTES_DB:
            r = ROUTES_DB[key]
            geo = r.get('geometry', [])
            return r['miles'], r['hours'], geo

    # Fallback to Haversine if route missing in JSON
    if loc1 in NODE_COORDS and loc2 in NODE_COORDS:
        c1 = NODE_COORDS.get(loc1) or NODE_COORDS.get(f"{loc1}, UT")
        c2 = NODE_COORDS.get(loc2) or NODE_COORDS.get(f"{loc2}, UT")
        
        if c1 and c2:
            R = 3958.8
            a = math.sin(math.radians(c2['lat']-c1['lat'])/2)**2 + math.cos(math.radians(c1['lat'])) * math.cos(math.radians(c2['lat'])) * math.sin(math.radians(c2['lon']-c1['lon'])/2)**2
            c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
            dist = R * c * 1.35
            return int(dist), round(dist/50.0, 1), [[c1['lon'], c1['lat']], [c2['lon'], c2['lat']]]
        
    return 0, 0.0, []

def get_return_days_needed(loc):
    if loc == "Home": return 0
    _, h, _ = get_routing_info(loc, "Home")
    
    # Updated threshold: 27 hours allows covering more ground
    if h <= 12: return 1
    if h <= 27: return 2
    return 3

def generate_itinerary(start_date, trip_days, ratings, vetoes, mpg, gas_adj_total, start_location="Home"):
    rows = []
    curr_date = start_date
    curr_loc = start_location
    days_used = 0
    map_segments = [] # Store geometry for map
    
    FAMILIAR_WATERS = [
        "Eel (Main)", "SF Eel", "Van Duzen", "Redwood Creek", 
        "Smith River (CA)", "Chetco", "Umpqua (Main)", "N. Umpqua"
    ]
    
    # 1. PYRAMID BLOCK (With Delta Stop)
    if start_location in ["Home", "Pyramid", "Delta", "Delta, UT"]:
        if not vetoes.get("Pyramid") and ratings.get("Pyramid", 0) > 0.5:
            pyr_score = ratings["Pyramid"]
            pyr_days = 3 if pyr_score >= 3.5 else 2 if pyr_score >= 2.5 else 1
            
            # --- CUSTOM: Route Home -> Delta -> Pyramid ---
            if curr_loc == "Home":
                # Leg 1: Home -> Delta
                m, h, geo = get_routing_info("Home", "Delta, UT")
                cost = (m/mpg) * (BASE_GAS_PRICES.get("Home", 3.00) + gas_adj_total)
                rows.append([curr_date.strftime("%m/%d/%Y"), "Delta, UT", f"DRIVE: Home → Delta (Overnight)", m, h, cost])
                if geo: map_segments.append(geo)
                curr_loc = "Delta, UT"
                curr_date += timedelta(days=1)
                days_used += 1
                
                # Leg 2: Delta -> Pyramid
                m, h, geo = get_routing_info("Delta, UT", "Pyramid")
                cost = (m/mpg) * (BASE_GAS_PRICES.get("Delta", 3.69) + gas_adj_total)
                rows.append([curr_date.strftime("%m/%d/%Y"), "Pyramid", f"DRIVE: Delta → Pyramid", m, h, cost])
                if geo: map_segments.append(geo)
                curr_loc = "Pyramid"
                curr_date += timedelta(days=1)
                days_used += 1
            
            # If not starting at Home (e.g. debugging), standard move
            elif curr_loc != "Pyramid":
                 m, h, geo = get_routing_info(curr_loc, "Pyramid")
                 cost = (m/mpg) * (BASE_GAS_PRICES.get(curr_loc, 3.50) + gas_adj_total)
                 rows.append([curr_date.strftime("%m/%d/%Y"), "Pyramid", f"DRIVE: {curr_loc} → Pyramid", m, h, cost])
                 if geo: map_segments.append(geo)
                 curr_loc = "Pyramid"
                 curr_date += timedelta(days=1)
                 days_used += 1

            # Fish Pyramid
            for _ in range(pyr_days):
                if days_used + get_return_days_needed(curr_loc) >= trip_days: break
                rows.append([curr_date.strftime("%m/%d/%Y"), "Pyramid", "FISH: Pyramid Lake", 0, 0, 0])
                curr_date += timedelta(days=1)
                days_used += 1
            
            # Move to Eagle
            if days_used + get_return_days_needed("Eagle") < trip_days:
                m, h, geo = get_routing_info("Pyramid", "Eagle")
                cost = (m/mpg) * (BASE_GAS_PRICES.get("Pyramid", 4.00) + gas_adj_total)
                rows.append([curr_date.strftime("%m/%d/%Y"), "Eagle", "DRIVE: Pyramid → Eagle", m, h, cost])
                if geo: map_segments.append(geo)
                curr_loc = "Eagle"
                curr_date += timedelta(days=1)
                days_used += 1
                rows.append([(curr_date - timedelta(days=1)).strftime("%m/%d/%Y"), "Eagle", "FISH: Eagle Lake (PM)", 0, 0, 0])

    # 2. COASTAL LOOP
    regions = ["Northern California", "Southern Oregon Coast", "Central Oregon Coast", 
               "Northern Oregon Coast", "Washington Coast", "Olympic Peninsula"]
    
    for reg in regions:
        if vetoes.get(reg): continue
        
        candidates = []
        for r in RIVER_REGIONS[reg]:
            s = ratings.get(r["Name"], 0)
            sort_score = s
            if r["Name"] in FAMILIAR_WATERS: sort_score += 1.5 
            if sort_score >= 2.5: candidates.append((r, sort_score, s))
            
        candidates.sort(key=lambda x: x[1], reverse=True)
        if not candidates: continue
        
        target, boosted_score, real_score = candidates[0]
        hub = target["Hub"]
        
        return_days_from_hub = get_return_days_needed(hub)
        _, move_h, _ = get_routing_info(curr_loc, hub)
        move_days = 1 if move_h > 4 else 0 
        
        # --- STAY DURATION & CAPACITY LOGIC ---
        days_fish = 1
        if real_score >= 4.0:
            days_fish = 3 
            if target["Name"] in FAMILIAR_WATERS: days_fish = 4
        elif real_score >= 3.0:
            days_fish = 2
            if target["Name"] in FAMILIAR_WATERS: days_fish = 3
        
        # Justify Move: Force min 2 days if moving
        if curr_loc != hub:
            days_fish = max(2, days_fish)

        # Capacity Check
        if days_used + move_days + days_fish + return_days_from_hub > trip_days:
            continue
            
        # Execute Move
        if curr_loc != hub:
            m, h, geo = get_routing_info(curr_loc, hub)
            cost = (m/mpg) * (BASE_GAS_PRICES.get(curr_loc, 4.00) + gas_adj_total)
            if h > 4.0:
                rows.append([curr_date.strftime("%m/%d/%Y"), hub, f"DRIVE: {curr_loc} → {hub}", m, h, cost])
                curr_date += timedelta(days=1)
                days_used += 1
            else:
                rows.append([curr_date.strftime("%m/%d/%Y"), hub, f"DRIVE: {curr_loc} → {hub} (AM)", m, h, cost])
            if geo: map_segments.append(geo)
            curr_loc = hub
        
        # Fish
        for _ in range(days_fish):
            if days_used + return_days_from_hub >= trip_days: break
            rows.append([curr_date.strftime("%m/%d/%Y"), hub, f"FISH: {target['Name']} ({real_score:.1f})", 0, 0, 0])
            curr_date += timedelta(days=1)
            days_used += 1

    # 2b. SLACK FILLER (Prevents early home)
    return_days_current = get_return_days_needed(curr_loc)
    slack_days = trip_days - days_used - return_days_current
    if slack_days > 0 and curr_loc != "Home":
        for _ in range(slack_days):
             rows.append([curr_date.strftime("%m/%d/%Y"), curr_loc, "FISH: Extension (Time Remaining)", 0, 0, 0])
             curr_date += timedelta(days=1)
             days_used += 1

    # 3. RETURN
    if curr_loc != "Home":
        m, h, geo = get_routing_info(curr_loc, "Home")
        if geo: map_segments.append(geo)
        
        cost = (m/mpg) * (BASE_GAS_PRICES.get(curr_loc, 4.00) + gas_adj_total)
        return_days = get_return_days_needed(curr_loc)
        
        if return_days == 3:
            rows.append([curr_date.strftime("%m/%d/%Y"), "Transit", f"RETURN (Leg 1): {curr_loc} → Stop 1", m/3, h/3, cost/3])
            curr_date += timedelta(days=1)
            rows.append([curr_date.strftime("%m/%d/%Y"), "Transit", f"RETURN (Leg 2): Stop 1 → Stop 2", m/3, h/3, cost/3])
            curr_date += timedelta(days=1)
            rows.append([curr_date.strftime("%m/%d/%Y"), "Home", f"RETURN (Leg 3): Stop 2 → Home", m/3, h/3, cost/3])
        elif return_days == 2:
            rows.append([curr_date.strftime("%m/%d/%Y"), "Transit", f"RETURN (Leg 1): {curr_loc} → Midway", m/2, h/2, cost/2])
            curr_date += timedelta(days=1)
            rows.append([curr_date.strftime("%m/%d/%Y"), "Home", f"RETURN (Leg 2): Midway → Home", m/2, h/2, cost/2])
        else:
            rows.append([curr_date.strftime("%m/%d/%Y"), "Home", f"RETURN: {curr_loc} → Home", m, h, cost])

    return pd.DataFrame(rows, columns=["Date", "Location", "Activity", "Miles", "Hours", "Fuel Cost"]), map_segments

# ============================================================
# 4. RENDER FUNCTION (CALLED BY MAIN APP)
# ============================================================

def render_planner():
    st.title("Steelhead Navigator V10 🧭")

    with st.spinner("Fetching Live Data (Async Flows + Weather)..."):
        LIVE_DATA = get_live_data()
        LIVE_FLOWS = LIVE_DATA["flows"]
        LIVE_WEATHER = LIVE_DATA["weather"]

    with st.sidebar:
        st.header("🎛️ Mission Controls")
        with st.expander("📍 Live Navigator", expanded=True):
            loc_options = ["Home"] + sorted(list(NODE_COORDS.keys()))
            current_loc = st.selectbox("Where are you now?", loc_options, index=0)
        with st.expander("📅 Timeline", expanded=False):
            # START DATE: Later of 2026/01/01 or Today
            now = datetime.date.today()
            season_start = datetime.date(2026, 1, 1)
            start_default = max(now, season_start)
            
            start_d = st.date_input("Start Date", start_default, format="MM/DD/YYYY")
            end_d = st.date_input("End Date", start_default + timedelta(days=17), format="MM/DD/YYYY")
            
            # Calculate duration from dates
            trip_len = (end_d - start_d).days + 1
            
            st.info(f"Total Window: {trip_len} Days")

        user_ratings = {}
        vetoes = {}
        st.subheader("🌊 River Ratings")
        with st.expander("Pyramid Lake", expanded=False):
            user_ratings["Pyramid"] = st.slider("Manual Rating", 0.0, 5.0, 3.5, 0.25)

        for reg, rivers in RIVER_REGIONS.items():
            with st.expander(reg, expanded=False):
                vetoes[reg] = st.checkbox(f"Veto {reg}", value=False)
                if not vetoes[reg]:
                    for r in rivers:
                        series = LIVE_FLOWS.get(r["Name"])
                        hub = r.get("Hub", "")
                        w_key = "Forks" if "WA" in reg else "Brookings" if "South" in reg else "Eureka"
                        w_periods = LIVE_WEATHER.get(w_key, [])
                        auto, _ = auto_score(series, r, w_periods)
                        label = f"{r['Name']}"
                        if auto >= 4.0: label += " 🔥"
                        elif auto <= 1.0: label += " ⚠️"
                        user_ratings[r["Name"]] = st.slider(label, 0.0, 5.0, float(auto), 0.25, key=f"{r['Name']}_{r['ID']}")

        with st.expander("⚙️ Logistics", expanded=False):
            mpg = st.number_input("MPG", 5.0, 40.0, 23.0, step=0.1, format="%.1f")
            user_adj = st.slider("Gas +/-", -1.0, 1.0, 0.0, 0.1, format="%.1f")
            total_gas_adj = user_adj

        st.markdown("---")
        st.markdown("### 🔗 Quick Links")
        st.markdown("""
        * [NOAA River Forecast](https://www.cnrfc.noaa.gov/)
        * [Pyramid Fly Co](https://pyramidflyco.com/fishing-report/)
        * [The Fly Shop](https://www.theflyshop.com/stream-report)
        * [Ashland Fly Shop](https://www.ashlandflyshop.com/blogs/fishing-reports)
        * [Waters West](https://waterswest.com/fishing-report/)
        """)

    # ============================================================
    # 5. MAIN CONTENT
    # ============================================================

    df, map_segments = generate_itinerary(start_d, trip_len, user_ratings, vetoes, mpg, total_gas_adj, current_loc)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Days", trip_len)
    c2.metric("Fish Days", len(df[df["Activity"].str.contains("FISH")]))
    c3.metric("Est. Miles", f"{df['Miles'].sum():,.0f}")
    c4.metric("Est. Fuel", f"${df['Fuel Cost'].sum():,.0f}")

    st.divider()
    st.subheader("📅 Proposed Itinerary")

    def highlight_today(row):
        try:
            row_date = datetime.datetime.strptime(row['Date'], "%m/%d/%Y").date()
            if row_date == datetime.date.today():
                 return ['background-color: #dbeafe; color: #1e40af; font-weight: bold'] * len(row)
        except: pass
        return [''] * len(row)

    st.dataframe(
        df.style.format({"Miles": "{:.0f}", "Hours": "{:.1f}", "Fuel Cost": "${:.0f}"}).apply(highlight_today, axis=1),
        use_container_width=True,
        height=(len(df)+1)*35,
        hide_index=True
    )

    import pydeck as pdk
    st.subheader("🗺️ Route Map")
    
    st.markdown("""
    <style>
    .map-legend { font-size: 14px; margin-bottom: 10px; }
    .dot { height: 10px; width: 10px; border-radius: 50%; display: inline-block; margin-right: 5px; }
    </style>
    <div class="map-legend">
        <span class="dot" style="background-color: rgb(160, 32, 240);"></span>Nodes (Hubs) &nbsp;
        <span class="dot" style="background-color: rgb(0, 255, 0);"></span>Prime (>= 4.0) &nbsp;
        <span class="dot" style="background-color: rgb(255, 255, 0);"></span>Marginal (3.0-4.0) &nbsp;
        <span class="dot" style="background-color: rgb(255, 165, 0);"></span>Marginal (2.0-3.0) &nbsp;
        <span class="dot" style="background-color: rgb(255, 0, 0);"></span>Low/Poor (< 2.0) &nbsp;
        <span class="dot" style="background-color: rgb(200, 200, 200);"></span>Vetoed/No Data
    </div>
    """, unsafe_allow_html=True)

    map_points = []
    
    # --- Hub Points ---
    lats, lons = [], []
    for _, row in df.iterrows():
        loc = row["Location"]
        # Handle simple name mismatches or "Delta, UT"
        coord_loc = loc
        if loc not in NODE_COORDS and f"{loc}, UT" in NODE_COORDS:
            coord_loc = f"{loc}, UT"
            
        if coord_loc in NODE_COORDS:
            c = NODE_COORDS[coord_loc]
            lats.append(c['lat'])
            lons.append(c['lon'])
            map_points.append({
                "name": loc, 
                "lat": c['lat'], 
                "lon": c['lon'], 
                "type": "Hub", 
                "score": "N/A",
                "info": "Hub/Stop",
                "color": [160, 32, 240], # Purple
                "radius": 12000
            })

    # --- River Points ---
    # We iterate all regions to plot river statuses
    hub_counts = {}
    for reg, rivers in RIVER_REGIONS.items():
        is_vetoed = vetoes.get(reg, False)
        for r in rivers:
            hub = r["Hub"]
            if hub in NODE_COORDS:
                c_hub = NODE_COORDS[hub]
                
                # Simple spiral/offset algorithm to prevent stacking
                count = hub_counts.get(hub, 0)
                ring = (count // 6) + 1
                angle = (count % 6) * (60 * 3.14159 / 180)
                lat_off = c_hub['lat'] + (math.sin(angle) * 0.04 * ring)
                lon_off = c_hub['lon'] + (math.cos(angle) * 0.05 * ring)
                hub_counts[hub] = count + 1
                
                score = user_ratings.get(r["Name"], 0)
                
                if is_vetoed: col = [200, 200, 200]
                elif score >= 4.0: col = [0, 255, 0]
                elif score >= 3.0: col = [255, 255, 0]
                elif score >= 2.0: col = [255, 165, 0]
                else: col = [255, 0, 0]

                series = LIVE_FLOWS.get(r["Name"])
                if series and len(series) > 0:
                    val = series[-1][1]
                    unit = "ft" if "ft" in r.get("T", "") else "cfs"
                    flow_str = f"{val} {unit}"
                else:
                    flow_str = "N/A"

                map_points.append({
                    "name": r["Name"], 
                    "lat": lat_off, 
                    "lon": lon_off, 
                    "type": "River", 
                    "score": f"{score:.1f}",
                    "info": f"Flow: {flow_str}<br>Target: {r['T']}",
                    "color": col, 
                    "radius": 8000
                })

    # Render Actual Route Geometry from JSON
    l_route = pdk.Layer(
        "PathLayer",
        data=[{"path": segment} for segment in map_segments],
        get_path="path",
        get_color=[255, 140, 0],
        width_min_pixels=3,
        pickable=False
    )
    
    # Simplify tooltip HTML for points
    tooltip_html = """
        <div style="font-family: sans-serif; padding: 5px; background: rgba(0,0,0,0.8); color: white; border-radius: 4px;">
            <b>{name}</b><br>Type: {type}
        </div>
    """

    l_points = pdk.Layer(
        "ScatterplotLayer", 
        data=map_points, 
        get_position=["lon", "lat"], 
        get_color="color", 
        get_radius="radius",
        pickable=True,
        auto_highlight=True
    )

    # Dynamic View State
    if lats and lons:
        view = pdk.ViewState(latitude=sum(lats)/len(lats), longitude=sum(lons)/len(lons), zoom=5.2)
    else:
        view = pdk.ViewState(latitude=44.5, longitude=-122.0, zoom=5.2)
        
    st.pydeck_chart(pdk.Deck(layers=[l_route, l_points], initial_view_state=view, tooltip={"html": tooltip_html}))

    # --- Conditions ---
    st.divider()
    st.subheader("Conditions")

    with st.expander("🌤️ Weather Forecast", expanded=True):
        t1, t2 = st.tabs(["36-Hour Detail", "5-Day Outlook"])
        display_locs = ["Pyramid", "Eureka", "Crescent City", "Brookings", "Coos Bay", "Tillamook", "Forks"]
        
        with t1:
            cols = st.columns(3)
            for i, name in enumerate(display_locs):
                periods = LIVE_WEATHER.get(name, [])
                with cols[i % 3]:
                    st.markdown(f"**{name}**")
                    if periods:
                        for x in periods[:3]:
                            txt = x["detailedForecast"]
                            precip_clean = format_precip_text(txt)
                            st.caption(f"**{x['name']}**: {x['temperature']}°F. {x['shortForecast']}\n*Wind: {x.get('windSpeed')} | {precip_clean}*")
                    else: st.caption("No Data")
                        
        with t2:
            cols = st.columns(3)
            for i, name in enumerate(display_locs):
                periods = LIVE_WEATHER.get(name, [])
                with cols[i % 3]:
                    st.markdown(f"**{name}**")
                    if periods:
                        for x in periods[:10:2]:
                            txt = x["detailedForecast"]
                            precip_clean = format_precip_text(txt)
                            st.caption(f"**{x['name']}**: {x['temperature']}°F. {x['shortForecast']} {precip_clean}")
                    else: st.caption("No Data")