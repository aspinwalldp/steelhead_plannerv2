import streamlit as st
import pandas as pd
import datetime
import requests
from datetime import timedelta
import re
import math
import asyncio
import aiohttp

# --- CONFIGURATION ---
st.set_page_config(page_title="Steelhead Navigator V10", layout="wide", page_icon="🧭")

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
        {"Name": "Pistol", "ID": "14400000", "T": "300-1100 cfs", "Hub": "Gold Beach"}, # Proxy
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
    "Home": (37.472, -105.879), "Pyramid": (39.95, -119.60), "Eagle": (40.55, -120.80),
    "Pepperwood": (40.48, -124.03), "Maple Grove": (40.52, -123.90), "Hiouchi": (41.79, -124.07),
    "Eureka": (40.80, -124.16), "Brookings": (42.05, -124.27), "Gold Beach": (42.40, -124.42),
    "Port Orford": (42.74, -124.49), "Coos Bay": (43.36, -124.21), "Reedsport": (43.70, -124.09),
    "Newport": (44.63, -124.05), "Tillamook": (45.45, -123.84),
    "Steamboat": (43.34, -122.73), "Aberdeen": (46.97, -123.81),
    "Forks": (47.95, -124.38), "Bogachiel": (47.93, -124.45),
    "Hoh": (47.86, -124.25), "Queets": (47.53, -124.33),
}

BASE_GAS_PRICES = {
    "Home": 2.99, "Pyramid": 4.25, "Eagle": 4.10, "Pepperwood": 4.79, "Maple Grove": 4.79,
    "Hiouchi": 4.85, "Eureka": 4.79, "Brookings": 3.69, "Gold Beach": 3.89,
    "Port Orford": 3.99, "Coos Bay": 3.59, "Reedsport": 3.69, "Newport": 3.75, "Tillamook": 3.79,
    "Steamboat": 4.50, "Aberdeen": 4.19,
    "Forks": 4.39, "Bogachiel": 4.39, "Hoh": 4.45, "Queets": 4.50,
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
    """Fetch time series for trend analysis."""
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
    """Returns 'dropping', 'rising', or 'stable' based on last 6 hours."""
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
    if "between a tenth and a quarter" in lower or "between one tenth and one quarter" in lower:
        return '💧 Precip: 0.10–0.25" possible'
    if "between a quarter and half" in lower or "between a quarter and a half" in lower:
        return '💧 Precip: 0.25–0.50" possible'
    if "between half and three quarters" in lower or "between a half and three quarters" in lower:
        return '💧 Precip: 0.50–0.75" possible'
    if "between three quarters and one inch" in lower:
        return '💧 Precip: 0.75–1.00" possible'
    if "a quarter of an inch" in lower or "one quarter of an inch" in lower:
        return '💧 Precip: 0.25" possible'
    if "a tenth of an inch" in lower or "one tenth of an inch" in lower:
        return '💧 Precip: 0.10" possible'
    if "a half inch" in lower or "one half inch" in lower:
        return '💧 Precip: 0.50" possible'
    if "one inch possible" in lower or "around an inch" in lower:
        return '💧 Precip: ~1.00" possible'
    
    m = re.search(r"(amounts? (of|between) .+? (possible|expected))", lower)
    if m:
        phrase = m.group(1).replace("amounts of", "").replace("amounts between", "").strip()
        phrase = phrase.replace("a quarter", "0.25").replace("one quarter", "0.25")
        phrase = phrase.replace("a tenth", "0.10").replace("one tenth", "0.10")
        phrase = phrase.replace("a half", "0.5").replace("one half", "0.5")
        phrase = phrase.replace("three quarters", "0.75")
        phrase = phrase.replace("inches", '"').replace("inch", '"')
        phrase = phrase.replace("to", "–").replace("and", "–")
        phrase = phrase.replace("possible", "").replace("expected", "")
        phrase = re.sub(r'\s+', ' ', phrase).strip()
        if phrase:
            return f'💧 Precip: {phrase} possible'
            
    if "rain" in lower or "showers" in lower:
        return "💧 Precip: rain possible"
    if "snow" in lower:
        return "💧 Precip: snow possible"
    
    return "Precip: none/min"

# ============================================================
# 3. ROUTING & ITINERARY
# ============================================================

@st.cache_data
def load_distances():
    try:
        df = pd.read_csv("distances.csv")
    except FileNotFoundError:
        return pd.DataFrame(columns=["from", "to", "miles", "hours"])
    dist_map = {}
    for _, row in df.iterrows():
        key = f"{row['from']}|{row['to']}"
        dist_map[key] = (float(row['miles']), float(row['hours']))
        dist_map[f"{row['to']}|{row['from']}"] = (float(row['miles']), float(row['hours']))
    return dist_map

DISTANCE_MAP = load_distances()

def get_routing(loc1, loc2):
    if loc1 == loc2: return 0, 0.0
    if f"{loc1}|{loc2}" in DISTANCE_MAP: return DISTANCE_MAP[f"{loc1}|{loc2}"]
    if loc1 in NODE_COORDS and loc2 in NODE_COORDS:
        lat1, lon1 = NODE_COORDS[loc1]
        lat2, lon2 = NODE_COORDS[loc2]
        R = 3958.8
        a = math.sin(math.radians(lat2-lat1)/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(math.radians(lon2-lon1)/2)**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
        dist = R * c * 1.35
        return int(dist), round(dist/50.0, 1)
    return 0, 0.0

def get_return_days_needed(loc):
    """Calculates return days: 1 if <=12h, 2 if <=24h, 3 if >24h."""
    if loc == "Home": return 0
    _, h = get_routing(loc, "Home")
    if h <= 12: return 1
    if h <= 24: return 2
    return 3

def generate_itinerary(start_date, trip_days, ratings, vetoes, mpg, gas_adj_total, start_location="Home"):
    rows = []
    curr_date = start_date
    curr_loc = start_location
    days_used = 0
    
    # Define Familiar Waters for Logic Preference
    FAMILIAR_WATERS = [
        "Eel (Main)", "SF Eel", "Van Duzen", "Redwood Creek", 
        "Smith River (CA)", "Chetco", "Umpqua (Main)", "N. Umpqua"
    ]
    
    # 1. PYRAMID BLOCK (Skip if starting past Pyramid)
    if start_location in ["Home", "Pyramid"]:
        if not vetoes.get("Pyramid") and ratings.get("Pyramid", 0) > 0.5:
            pyr_score = ratings["Pyramid"]
            pyr_days = 3 if pyr_score >= 3.5 else 2 if pyr_score >= 2.5 else 1
            
            # Check if we can do Pyramid and still get back
            if curr_loc != "Pyramid":
                 m, h = get_routing(curr_loc, "Pyramid")
                 cost = (m/mpg) * (BASE_GAS_PRICES.get(curr_loc, 3.50) + gas_adj_total)
                 rows.append([curr_date.strftime("%m/%d/%Y"), "Pyramid", f"DRIVE: {curr_loc} → Pyramid", m, h, cost])
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
                m, h = get_routing("Pyramid", "Eagle")
                cost = (m/mpg) * (BASE_GAS_PRICES.get("Pyramid", 4.00) + gas_adj_total)
                rows.append([curr_date.strftime("%m/%d/%Y"), "Eagle", "DRIVE: Pyramid → Eagle", m, h, cost])
                curr_loc = "Eagle"
                curr_date += timedelta(days=1)
                days_used += 1
                rows.append([(curr_date - timedelta(days=1)).strftime("%m/%d/%Y"), "Eagle", "FISH: Eagle Lake (PM)", 0, 0, 0])

    # 2. COASTAL LOOP
    regions = ["Northern California", "Southern Oregon Coast", "Central Oregon Coast", 
               "Northern Oregon Coast", "Washington Coast", "Olympic Peninsula"]
    
    for reg in regions:
        # Standard Routing Logic
        if vetoes.get(reg): continue
        
        candidates = []
        for r in RIVER_REGIONS[reg]:
            s = ratings.get(r["Name"], 0)
            
            # --- FAMILIARITY LOGIC ---
            # Boost score for familiar waters for SELECTION purposes, but keep real score for duration logic
            sort_score = s
            if r["Name"] in FAMILIAR_WATERS:
                sort_score += 1.5 # Significant boost to prefer familiar waters
            
            if sort_score >= 2.5: candidates.append((r, sort_score, s))
            
        candidates.sort(key=lambda x: x[1], reverse=True)
        if not candidates: continue
        
        target, boosted_score, real_score = candidates[0]
        hub = target["Hub"]
        
        # Look ahead: If we move to this hub, how many days to return?
        return_days_from_hub = get_return_days_needed(hub)
        
        # Time to move there?
        _, move_h = get_routing(curr_loc, hub)
        move_days = 1 if move_h > 4 else 0 # Simple heuristic for move day consumption
        
        # --- STAY DURATION LOGIC (Calculated EARLY to check capacity) ---
        days_fish = 1
        
        if real_score >= 4.0:
            # Prime Conditions
            days_fish = 3 # Cap at 3 days normally
            if target["Name"] in FAMILIAR_WATERS:
                days_fish = 4 # Exception: Familiar + Prime = Stay 4
        elif real_score >= 3.0:
            # Good/Marginal Conditions
            days_fish = 2
            if target["Name"] in FAMILIAR_WATERS:
                days_fish = 3 # Stay 3 if familiar
        
        # OVERRIDE: Justify the move
        # If we are moving hubs, ensure we stay at least 2 days to justify the drive
        if curr_loc != hub:
            days_fish = max(2, days_fish)

        # Total days committed if we go: current used + moving + days fish + return
        if days_used + move_days + days_fish + return_days_from_hub > trip_days:
            # Not enough time to go here, stay long enough to justify it, and return
            continue
            
        # Execute Move
        if curr_loc != hub:
            m, h = get_routing(curr_loc, hub)
            cost = (m/mpg) * (BASE_GAS_PRICES.get(curr_loc, 4.00) + gas_adj_total)
            if h > 4.0:
                rows.append([curr_date.strftime("%m/%d/%Y"), hub, f"DRIVE: {curr_loc} → {hub}", m, h, cost])
                curr_date += timedelta(days=1)
                days_used += 1
            else:
                rows.append([curr_date.strftime("%m/%d/%Y"), hub, f"DRIVE: {curr_loc} → {hub} (AM)", m, h, cost])
            curr_loc = hub
        
        # Fish
        for _ in range(days_fish):
            if days_used + return_days_from_hub >= trip_days: break
            rows.append([curr_date.strftime("%m/%d/%Y"), hub, f"FISH: {target['Name']} ({real_score:.1f})", 0, 0, 0])
            curr_date += timedelta(days=1)
            days_used += 1

    # 2b. FILL REMAINING DAYS
    # If we finished the regions loop but the "Justify Move" logic forced skips that left us with slack time,
    # we should spend that time fishing the current location rather than going home early.
    return_days_current = get_return_days_needed(curr_loc)
    slack_days = trip_days - days_used - return_days_current
    
    if slack_days > 0 and curr_loc != "Home":
        for _ in range(slack_days):
             rows.append([curr_date.strftime("%m/%d/%Y"), curr_loc, "FISH: Extension (Time Remaining)", 0, 0, 0])
             curr_date += timedelta(days=1)
             days_used += 1

    # 3. RETURN TRIP (Dynamic Legs)
    if curr_loc != "Home":
        m, h = get_routing(curr_loc, "Home")
        cost = (m/mpg) * (BASE_GAS_PRICES.get(curr_loc, 4.00) + gas_adj_total)
        return_days = get_return_days_needed(curr_loc)
        
        if return_days == 3:
            # Leg 1
            rows.append([curr_date.strftime("%m/%d/%Y"), "Transit", f"RETURN (Leg 1): {curr_loc} → Stop 1", m/3, h/3, cost/3])
            curr_date += timedelta(days=1)
            # Leg 2
            rows.append([curr_date.strftime("%m/%d/%Y"), "Transit", f"RETURN (Leg 2): Stop 1 → Stop 2", m/3, h/3, cost/3])
            curr_date += timedelta(days=1)
            # Leg 3 (Arrival)
            rows.append([curr_date.strftime("%m/%d/%Y"), "Home", f"RETURN (Leg 3): Stop 2 → Home", m/3, h/3, cost/3])
        elif return_days == 2:
            # Leg 1
            rows.append([curr_date.strftime("%m/%d/%Y"), "Transit", f"RETURN (Leg 1): {curr_loc} → Midway", m/2, h/2, cost/2])
            curr_date += timedelta(days=1)
            # Leg 2 (Arrival)
            rows.append([curr_date.strftime("%m/%d/%Y"), "Home", f"RETURN (Leg 2): Midway → Home", m/2, h/2, cost/2])
        else:
            # Direct
            rows.append([curr_date.strftime("%m/%d/%Y"), "Home", f"RETURN: {curr_loc} → Home", m, h, cost])

    return pd.DataFrame(rows, columns=["Date", "Location", "Activity", "Miles", "Hours", "Fuel Cost"])

# ============================================================
# 4. SIDEBAR & UI
# ============================================================

st.title("Steelhead Navigator V10 🧭")

with st.spinner("Fetching Live Data (Async Flows + Weather)..."):
    LIVE_DATA = get_live_data()
    LIVE_FLOWS = LIVE_DATA["flows"]
    LIVE_WEATHER = LIVE_DATA["weather"]

with st.sidebar:
    st.header("🎛️ Mission Controls")
    
    # --- LIVE NAVIGATOR ---
    with st.expander("📍 Live Navigator", expanded=True):
        loc_options = ["Home"] + sorted(list(NODE_COORDS.keys()))
        loc_options = sorted(list(set(loc_options)), key=lambda x: (x != "Home", x))
        current_loc = st.selectbox("Where are you now?", loc_options, index=0)
        
    with st.expander("📅 Timeline", expanded=False):
        default_start = datetime.date.today() if current_loc != "Home" else datetime.date(2026, 1, 1)
        start_d = st.date_input("Start / Today", default_start, format="MM/DD/YYYY")
        end_d = st.date_input("End (Home Arrival)", datetime.date(2026, 1, 17), format="MM/DD/YYYY")
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
                    
                    auto, label_status = auto_score(series, r, w_periods)
                    
                    label = f"{r['Name']}"
                    if auto >= 4.0: label += " 🔥"
                    elif auto <= 1.0: label += " ⚠️"
                    
                    unique_key = f"{r['Name']}_{r['ID']}"
                    user_ratings[r["Name"]] = st.slider(label, 0.0, 5.0, float(auto), 0.25, key=unique_key)
                    st.caption(f"{label_status} | T: {r['T']}")

    with st.expander("⚙️ Logistics", expanded=False):
        # UPDATED: Enforce 1 decimal place using format="%.1f" and step=0.1
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

df = generate_itinerary(start_d, trip_len, user_ratings, vetoes, mpg, total_gas_adj, current_loc)

# --- Trip Totals ---
c1, c2, c3, c4 = st.columns(4)
total_miles = df["Miles"].sum()
total_fuel = df["Fuel Cost"].sum()
fish_days = len(df[df["Activity"].str.contains("FISH")])

c1.metric("Total Days", trip_len)
c2.metric("Fish Days", fish_days)
c3.metric("Est. Miles", f"{total_miles:,.0f}")
c4.metric("Est. Fuel", f"${total_fuel:,.0f}")

# --- Region Breakdown ---
st.caption("Days per Region")
df["Region"] = df["Location"].map(HUB_TO_REGION).fillna("Travel")
region_counts = df[df["Activity"].str.contains("FISH")].groupby("Region").size()

r_cols = st.columns(7)
regions_of_interest = ["Pyramid", "Northern California", "Southern Oregon Coast", "Central Oregon Coast", "Northern Oregon Coast", "Washington Coast", "Olympic Peninsula"]
short_names = {"Pyramid": "Pyramid", "Northern California": "NorCal", "Southern Oregon Coast": "S. Oregon", "Central Oregon Coast": "C. Oregon", "Olympic Peninsula": "OP", "Northern Oregon Coast": "N. Oregon", "Washington Coast": "WA Coast"}

for i, reg in enumerate(regions_of_interest):
    count = region_counts.get(reg, 0)
    r_cols[i % 7].metric(short_names.get(reg, reg), f"{count} days")

st.divider()

# --- Itinerary Table ---
st.subheader("📅 Proposed Itinerary")

# FUNCTION: Highlight Today's Row
def highlight_today(row):
    try:
        # Assuming Date column is string formatted %m/%d/%Y
        row_date = datetime.datetime.strptime(row['Date'], "%m/%d/%Y").date()
        if row_date == datetime.date.today():
             # Highlight Blue
             return ['background-color: #dbeafe; color: #1e40af; font-weight: bold'] * len(row)
    except:
        pass
    return [''] * len(row)

# APPLY: Highlight + Hide Index
st.dataframe(
    df.style.format({"Miles": "{:.0f}", "Hours": "{:.1f}", "Fuel Cost": "${:.0f}"}).apply(highlight_today, axis=1),
    use_container_width=True,
    height=(len(df)+1)*35,
    hide_index=True
)

# --- Map View ---
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
route_path = []

for _, row in df.iterrows():
    loc = row["Location"]
    if loc in NODE_COORDS:
        lat, lon = NODE_COORDS[loc]
        map_points.append({
            "name": loc, 
            "lat": lat, 
            "lon": lon, 
            "type": "Hub", 
            "score": "N/A",
            "info": "Hub/Stop",
            "color": [160, 32, 240], # Purple
            "radius": 12000 
        })
        route_path.append([lon, lat])

hub_counts = {}
for reg, rivers in RIVER_REGIONS.items():
    is_vetoed = vetoes.get(reg, False)
    for r in rivers:
        hub = r["Hub"]
        if hub in NODE_COORDS:
            lat, lon = NODE_COORDS[hub]
            count = hub_counts.get(hub, 0)
            ring = (count // 6) + 1
            angle = (count % 6) * (60 * 3.14159 / 180)
            lat_off = lat + (math.sin(angle) * 0.04 * ring)
            lon_off = lon + (math.cos(angle) * 0.05 * ring)
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

view = pdk.ViewState(latitude=44.5, longitude=-122.0, zoom=5.2)

l_route = pdk.Layer("PathLayer", data=[{"path": route_path}], get_path="path", get_color=[255, 140, 0], width_min_pixels=3, pickable=False)
l_points = pdk.Layer("ScatterplotLayer", data=map_points, get_position=["lon", "lat"], get_color="color", get_radius="radius", pickable=True, auto_highlight=True)

tooltip_html = """
    <div style="font-family: sans-serif; padding: 5px; background: rgba(0,0,0,0.8); color: white; border-radius: 4px;">
        <b>{name}</b><br>Type: {type}<br>Score: {score}<br>{info}
    </div>
"""

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