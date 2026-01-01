# -*- coding: utf-8 -*-
import streamlit as st
import datetime as dt
import pandas as pd
import asyncio
import aiohttp
import numpy as np

# Import map module safely
try:
    from map_v2 import render_coastal_map
except ImportError:
    def render_coastal_map(data, filters):
        st.warning("Map module not found. Please ensure map_v2.py is present.")

# ============================================================
# 1. REGION SPECS (FULL LIST + FAMILIARITY)
# ============================================================

def load_coastal_region_specs():
    # 1. NORTHERN CALIFORNIA
    COASTAL_NORCAL = [
        {"Name": "Smith (Jed)", "lat": 41.90, "lon": -124.13, "Gauges": [{"ID": "11532500", "P": "00060", "Source": "USGS"}], "T": "1500-7500 cfs", "Low": 600, "Type": "Flashy", "N": "The Holy Grail.", "NOAA_zone": "CAC01", "NWS_ID": "JEDC1", "Familiar": True},
        {"Name": "Eel (Main)", "lat": 40.49, "lon": -124.10, "Gauges": [{"ID": "11477000", "P": "00060", "Source": "USGS"}], "T": "2000-15000 cfs", "Low": 1000, "Type": "Large", "N": "Mainstem swing.", "NOAA_zone": "CAC02", "NWS_ID": "SCOC1", "Familiar": True},
        {"Name": "S.F. Eel", "lat": 40.23, "lon": -123.79, "Gauges": [{"ID": "11476500", "P": "00060", "Source": "USGS"}], "T": "800-4000 cfs", "Low": 300, "Type": "Medium", "N": "Miranda gauge.", "NOAA_zone": "CAC02", "NWS_ID": "MRNC1", "Familiar": True},
        {"Name": "Van Duzen", "lat": 40.48, "lon": -124.13, "Gauges": [{"ID": "11478500", "P": "00060", "Source": "USGS"}], "T": "500-2500 cfs", "Low": 250, "Type": "Flashy", "N": "Bridgeville gauge.", "NOAA_zone": "CAC02", "NWS_ID": "BRGC1", "Familiar": True},
        {"Name": "Redwood Cr.", "lat": 41.29, "lon": -124.09, "Gauges": [{"ID": "11482500", "P": "00060", "Source": "USGS"}], "T": "400-1500 cfs", "Low": 300, "Type": "Flashy", "N": "Orick gauge.", "NOAA_zone": "CAC02", "NWS_ID": "ORIC1", "Familiar": True},
        {"Name": "Mad River", "lat": 40.89, "lon": -124.03, "Gauges": [{"ID": "11481000", "P": "00060", "Source": "USGS"}], "T": "600-3500 cfs", "Low": 300, "Type": "Flashy", "N": "Hatchery run.", "NOAA_zone": "CAC02", "NWS_ID": "ARCC1"},
        {"Name": "Navarro", "lat": 39.15, "lon": -123.67, "Gauges": [{"ID": "11468000", "P": "00060", "Source": "USGS"}, {"ID": "NAV", "P": "20", "Source": "CDEC"}], "T": "350-1500 cfs", "Low": 250, "Type": "Sedimentary", "N": "Road flood index.", "NOAA_zone": "CAC03", "NWS_ID": "NVRC1"},
        {"Name": "Gualala", "lat": 38.76, "lon": -123.53, "Gauges": [{"ID": "11467510", "P": "00060", "Source": "USGS"}], "T": "250-1200 cfs", "Low": 200, "Type": "Sedimentary", "N": "Bar-dependent.", "NOAA_zone": "CAC03", "NWS_ID": "GRLC1"},
        {"Name": "Garcia", "lat": 38.91, "lon": -123.70, "Gauges": [{"ID": "11467600", "P": "00060", "Source": "USGS"}], "T": "250-1100 cfs", "Low": 200, "Type": "Flashy", "N": "Compliance point.", "NOAA_zone": "CAC03", "NWS_ID": "GRCC1"},
        {"Name": "Mattole", "lat": 40.28, "lon": -124.35, "Gauges": [{"ID": "11468900", "P": "00060", "Source": "USGS"}], "T": "500-3000 cfs", "Low": 200, "Type": "Flashy", "N": "Ettersburg gauge.", "NOAA_zone": "CAC02", "NWS_ID": "MATC1"},
    ]

    # 2. SOUTHERN OREGON COAST
    COASTAL_SOUTH_OR = [
        {"Name": "Chetco", "lat": 42.08, "lon": -124.23, "Gauges": [{"ID": "14400000", "P": "00060", "Source": "USGS"}], "T": "1500-5000 cfs", "Low": 800, "Type": "Flashy", "N": "Fast dropper.", "NOAA_zone": "ORC01", "NWS_ID": "CHTO3", "Familiar": True},
        {"Name": "Elk River", "lat": 42.74, "lon": -124.45, "Gauges": [{"ID": "14327250", "P": "00065", "Source": "USGS"}], "T": "4.0-6.0 ft", "Low": 3.5, "Type": "Flashy", "N": "Fish factory.", "NOAA_zone": "ORC02", "NWS_ID": "ELKO3"},
        {"Name": "Sixes", "lat": 42.82, "lon": -124.46, "Gauges": [{"ID": "14327150", "P": "00065", "Source": "USGS"}], "T": "5.0-9.0 ft", "Low": 4.0, "Type": "Flashy", "N": "Muddy banks.", "NOAA_zone": "ORC02"},
        {"Name": "Winchuck", "lat": 42.02, "lon": -124.22, "Gauges": [{"ID": "14400200", "P": "00060", "Source": "USGS"}], "T": "250-900 cfs", "Low": 200, "Type": "Flashy", "N": "Tiny, brushy.", "NOAA_zone": "ORC01"},
        {"Name": "Pistol", "lat": 42.27, "lon": -124.39, "Gauges": [{"ID": "14400000", "P": "00060", "Source": "USGS", "Is_Proxy": True, "Name_Proxy": "Chetco"}], "T": "300-1100 cfs", "Low": 250, "Type": "Flashy", "N": "Proxy: Chetco.", "NOAA_zone": "ORC01"},
        {"Name": "Illinois", "lat": 42.44, "lon": -124.08, "Gauges": [{"ID": "14377100", "P": "00060", "Source": "USGS"}], "T": "1000-4000 cfs", "Low": 800, "Type": "Mixed", "N": "Kerby gauge.", "NOAA_zone": "ORC01", "NWS_ID": "KRBO3"},
        {"Name": "Rogue (Agness)", "lat": 42.55, "lon": -124.06, "Gauges": [{"ID": "14372300", "P": "00060", "Source": "USGS"}], "T": "2000-8000 cfs", "Low": 1500, "Type": "Medium", "N": "Canyon water.", "NOAA_zone": "ORC01", "NWS_ID": "AGNO3"},
        {"Name": "Floras/New", "lat": 42.92, "lon": -124.45, "Gauges": [{"ID": "14327137", "P": "00060", "Source": "USGS"}], "T": "300-1100 cfs", "Low": 200, "Type": "Flashy", "N": "Langlois gauge.", "NOAA_zone": "ORC02"},
    ]

    # 3. CENTRAL OREGON COAST
    COASTAL_CENTRAL_OR = [
        {"Name": "Umpqua (Main)", "lat": 43.63, "lon": -123.63, "Gauges": [{"ID": "14321000", "P": "00060", "Source": "USGS"}], "T": "3000-12000 cfs", "Low": 2000, "Type": "Large", "N": "Elkton gauge.", "NOAA_zone": "ORC04", "NWS_ID": "ELKO3", "Familiar": True},
        {"Name": "N. Umpqua", "lat": 43.30, "lon": -123.10, "Gauges": [{"ID": "14319500", "P": "00060", "Source": "USGS"}], "T": "1200-4000 cfs", "Low": 900, "Type": "Mixed", "N": "Winchester gauge.", "NOAA_zone": "ORC04", "NWS_ID": "WINO3", "Familiar": True},
        {"Name": "Coquille (S.F.)", "lat": 43.05, "lon": -124.16, "Gauges": [{"ID": "14325000", "P": "00065", "Source": "USGS"}], "T": "4.0-9.0 ft", "Low": 2.0, "Type": "Medium", "N": "Powers gauge.", "NOAA_zone": "ORC03", "NWS_ID": "POWO3"},
        {"Name": "Coquille (N.F.)", "lat": 43.15, "lon": -124.11, "Gauges": [{"ID": "14325000", "P": "00060", "Source": "USGS"}], "T": "600-2200 cfs", "Low": 450, "Type": "Mixed", "N": "Smaller/clearer.", "NOAA_zone": "ORC03"},
        {"Name": "Coquille (Main)", "lat": 43.11, "lon": -124.40, "Gauges": [{"ID": "14326500", "P": "00060", "Source": "USGS"}], "T": "2500-8500 cfs", "Low": 1600, "Type": "Mixed", "N": "Coquille gauge.", "NOAA_zone": "ORC03", "NWS_ID": "COQO3"},
        {"Name": "Coos/Millicoma", "lat": 43.40, "lon": -124.08, "Gauges": [{"ID": "14325000", "P": "00060", "Source": "USGS"}], "T": "700-2600 cfs", "Low": 500, "Type": "Mixed", "N": "Allegany.", "NOAA_zone": "ORC03"},
        {"Name": "Tenmile", "lat": 43.59, "lon": -124.20, "Gauges": [{"ID": "NO_GAUGE", "Source": "None"}], "T": "250-900 cfs", "Low": 200, "Type": "Flashy", "N": "Monitor lake levels.", "NOAA_zone": "ORC03"},
    ]

    # 4. NORTHERN OREGON COAST
    COASTAL_NORTH_OR = [
        {"Name": "Siuslaw", "lat": 44.06, "lon": -123.95, "Gauges": [{"ID": "14307620", "P": "00065", "Source": "USGS"}], "T": "4.0-10.0 ft", "Low": 3.0, "Type": "Medium", "N": "Mapleton gauge.", "NOAA_zone": "ORC05", "NWS_ID": "MPLO3"},
        {"Name": "Alsea", "lat": 44.38, "lon": -123.83, "Gauges": [{"ID": "14306500", "P": "00065", "Source": "USGS"}], "T": "3.0-9.0 ft", "Low": 2.0, "Type": "Flashy", "N": "Tidewater gauge.", "NOAA_zone": "ORC05", "NWS_ID": "TIDO3"},
        {"Name": "Siletz", "lat": 44.71, "lon": -123.89, "Gauges": [{"ID": "14305500", "P": "00065", "Source": "USGS"}], "T": "3.5-8.0 ft", "Low": 2.5, "Type": "Medium", "N": "Siletz gauge.", "NOAA_zone": "ORC05", "NWS_ID": "SILO3"},
        {"Name": "Nestucca", "lat": 45.24, "lon": -123.88, "Gauges": [{"ID": "14303600", "P": "00065", "Source": "USGS"}], "T": "3.5-7.5 ft", "Low": 2.5, "Type": "Flashy", "N": "Beaver gauge.", "NOAA_zone": "ORC06", "NWS_ID": "BEVO3"},
        {"Name": "Trask", "lat": 45.43, "lon": -123.77, "Gauges": [{"ID": "14302480", "P": "00065", "Source": "USGS"}], "T": "4.0-8.0 ft", "Low": 3.0, "Type": "Flashy", "N": "Drops quick.", "NOAA_zone": "ORC07"},
        {"Name": "Wilson", "lat": 45.48, "lon": -123.74, "Gauges": [{"ID": "14301500", "P": "00065", "Source": "USGS"}], "T": "3.5-7.5 ft", "Low": 2.5, "Type": "Flashy", "N": "Tillamook gauge.", "NOAA_zone": "ORC07", "NWS_ID": "TLMO3"},
        {"Name": "Nehalem", "lat": 45.71, "lon": -123.75, "Gauges": [{"ID": "14301000", "P": "00065", "Source": "USGS"}], "T": "3.5-9.0 ft", "Low": 2.5, "Type": "Large", "N": "Foss gauge.", "NOAA_zone": "ORC08", "NWS_ID": "FOSO3"},
        {"Name": "N.F. Nehalem", "lat": 45.78, "lon": -123.80, "Gauges": [{"ID": "14299000", "P": "00060", "Source": "USGS"}], "T": "400-1500 cfs", "Low": 300, "Type": "Flashy", "N": "Hatchery river.", "NOAA_zone": "ORC08"},
        {"Name": "Necanicum", "lat": 45.98, "lon": -123.91, "Gauges": [{"ID": "14297000", "P": "00060", "Source": "USGS"}], "T": "300-1200 cfs", "Low": 250, "Type": "Flashy", "N": "Seaside.", "NOAA_zone": "ORC08"},
    ]

    # 5. WASHINGTON COAST
    COASTAL_WA_COAST = [
        {"Name": "Willapa", "lat": 46.65, "lon": -123.72, "Gauges": [{"ID": "12010000", "P": "00060", "Source": "USGS"}], "T": "800-2600 cfs", "Low": 600, "Type": "Mixed", "N": "Tidal bay system.", "NOAA_zone": "WAC01", "NWS_ID": "WLLW1"},
        {"Name": "Satsop", "lat": 47.00, "lon": -123.49, "Gauges": [{"ID": "12035000", "P": "00060", "Source": "USGS"}], "T": "1500-7000 cfs", "Low": 600, "Type": "Medium", "N": "Chehalis trib.", "NOAA_zone": "WAC02", "NWS_ID": "SATW1"},
        {"Name": "Wynoochee", "lat": 47.07, "lon": -123.63, "Gauges": [{"ID": "12037400", "P": "00060", "Source": "USGS"}], "T": "1000-5000 cfs", "Low": 400, "Type": "Medium", "N": "Dam controlled.", "NOAA_zone": "WAC02", "NWS_ID": "MNTW1"},
        {"Name": "Humptulips", "lat": 47.23, "lon": -124.03, "Gauges": [{"ID": "12039005", "P": "00060", "Source": "USGS"}], "T": "1500-6000 cfs", "Low": 600, "Type": "Flashy", "N": "Hatchery powerhouse.", "NOAA_zone": "WAC02", "NWS_ID": "HMPW1"},
    ]

    # 6. OLYMPIC PENINSULA
    COASTAL_OP = [
        {"Name": "Quinault", "lat": 47.47, "lon": -123.86, "Gauges": [{"ID": "12039500", "P": "00060", "Source": "USGS"}], "T": "2000-10000 cfs", "Low": 1200, "Type": "Glacial", "N": "Rainforest giant.", "NOAA_zone": "WAC04", "NWS_ID": "QNTW1"},
        {"Name": "Queets", "lat": 47.53, "lon": -124.31, "Gauges": [{"ID": "12040500", "P": "00060", "Source": "USGS"}], "T": "2000-8000 cfs", "Low": 1000, "Type": "Glacial", "N": "Remote.", "NOAA_zone": "WAC04", "NWS_ID": "QUEW1"},
        {"Name": "Hoh", "lat": 47.81, "lon": -124.25, "Gauges": [{"ID": "12041200", "P": "00060", "Source": "USGS"}], "T": "1500-6000 cfs", "Low": 800, "Type": "Glacial", "N": "Milky green.", "NOAA_zone": "WAC04", "NWS_ID": "HOHW1"},
        {"Name": "Bogachiel", "lat": 47.93, "lon": -124.40, "Gauges": [{"ID": "12043015", "P": "00060", "Source": "USGS"}], "T": "1000-6000 cfs", "Low": 500, "Type": "Flashy", "N": "La Push.", "NOAA_zone": "WAC03", "NWS_ID": "LPUW1"},
        {"Name": "Calawah", "lat": 47.95, "lon": -124.37, "Gauges": [{"ID": "12043000", "P": "00060", "Source": "USGS"}], "T": "1000-4000 cfs", "Low": 500, "Type": "Flashy", "N": "Steep/Fast.", "NOAA_zone": "WAC03", "NWS_ID": "FKSW1"},
        {"Name": "Sol Duc", "lat": 47.98, "lon": -124.50, "Gauges": [{"ID": "12041500", "P": "00060", "Source": "USGS"}], "T": "600-2400 cfs", "Low": 350, "Type": "Mixed", "N": "Green glacial.", "NOAA_zone": "WAC03", "NWS_ID": "SOLW1"},
    ]

    return {
        "Northern California": COASTAL_NORCAL,
        "Southern Oregon Coast": COASTAL_SOUTH_OR,
        "Central Oregon Coast": COASTAL_CENTRAL_OR,
        "Northern Oregon Coast": COASTAL_NORTH_OR,
        "Washington Coast": COASTAL_WA_COAST,
        "Olympic Peninsula": COASTAL_OP,
    }

# ============================================================
# 2. ASYNC DATA FETCHING (MULTI-SOURCE + NOAA FORECASTS)
# ============================================================

async def coastal_fetch_usgs_async(session, site_id, param):
    """Async USGS fetcher."""
    try:
        url = "https://waterservices.usgs.gov/nwis/iv/"
        params = {
            "format": "json",
            "sites": site_id,
            "parameterCd": param,
            "period": "P3D", 
            "siteStatus": "all"
        }
        async with session.get(url, params=params, timeout=8) as response:
            if response.status != 200: return {"value": []}
            data = await response.json()
            if "value" not in data or not data["value"]["timeSeries"]:
                return {"value": []}
            return {"value": data["value"]["timeSeries"][0]["values"][0]["value"]}
    except Exception:
        return {"value": []}

async def coastal_fetch_cdec_async(session, station_id, sensor_id="20"):
    """Async CDEC fetcher (California)."""
    try:
        url = "https://cdec.water.ca.gov/dynamicapp/req/CSVDataServlet"
        params = {
            "Stations": station_id,
            "Sensor": sensor_id,
            "dur": "E",
            "Start": (dt.datetime.now() - dt.timedelta(days=3)).strftime("%Y-%m-%d"),
            "End": dt.datetime.now().strftime("%Y-%m-%d")
        }
        async with session.get(url, params=params, timeout=8) as response:
            if response.status != 200: return []
            text = await response.text()
            lines = text.strip().split("\n")
            series = []
            for line in lines:
                parts = line.split(",")
                if len(parts) < 6: continue
                try:
                    date_str = parts[4] + " " + parts[5]
                    val_str = parts[6]
                    try:
                        ts = dt.datetime.strptime(date_str, "%Y%m%d %H%M")
                    except:
                        ts = dt.datetime.strptime(date_str, "%m/%d/%Y %H:%M")
                    if val_str.replace('.','',1).isdigit():
                        val = float(val_str)
                        series.append((ts, val))
                except:
                    continue
            return series
    except Exception:
        return []

async def coastal_fetch_noaa_eta_async(session, spec):
    """Async precipitation probability fetcher."""
    zone = spec.get("NOAA_zone")
    if not zone: return None
    try:
        if "CAC" in zone: office, gx, gy = "EKA", 50, 160
        elif "ORC" in zone: office, gx, gy = "PQR", 110, 80
        elif "WAC" in zone: office, gx, gy = "SEW", 140, 80
        else: return None

        url = f"https://api.weather.gov/gridpoints/{office}/{gx},{gy}/forecast/hourly"
        headers = {"User-Agent": "CoastalSteelheadDashboard/2.0"}
        async with session.get(url, headers=headers, timeout=5) as response:
            if response.status != 200: return None
            r = await response.json()
            periods = r.get("properties", {}).get("periods", [])
            for i, p in enumerate(periods):
                pop = p.get("probabilityOfPrecipitation", {}).get("value", 0)
                if pop and pop >= 50: return i
        return None
    except: return None


# ============================================================
# PATCH 1 + PATCH 2 — 36‑hour forecast extraction + enhanced NWS fetcher
# ============================================================

def coastal_extract_36hr_forecast(forecasts):
    """Return list of (timestamp, value) for next 36 hours."""
    out = []
    now = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)
    cutoff = now + dt.timedelta(hours=36)

    for pt in forecasts:
        try:
            ts = dt.datetime.fromisoformat(pt["validTime"].replace("Z", "+00:00"))
            if now <= ts <= cutoff:
                out.append((ts, pt.get("primary", None)))
        except:
            continue

    return out


async def coastal_fetch_nws_forecast(session, nws_id):
    """Fetches river forecast (stage/flow) from NWPS API, returning peak + next 36 hrs."""
    if not nws_id: return None
    try:
        url = f"https://api.water.noaa.gov/nwps/v1/gauges/{nws_id}"
        headers = {"User-Agent": "CoastalSteelheadDashboard/2.0"}
        async with session.get(url, headers=headers, timeout=6) as response:
            if response.status != 200: return None
            data = await response.json()
            forecasts = data.get("hydrograph", {}).get("forecast", [])
            if not forecasts: return None

            max_val, max_time = -1, None
            for pt in forecasts:
                val = pt.get("primary", 0)
                if val > max_val:
                    max_val, max_time = val, pt.get("validTime", "")

            peak_str = None
            if max_val > -1 and max_time:
                try:
                    dt_obj = dt.datetime.fromisoformat(max_time.replace("Z", "+00:00"))
                    peak_str = f"Peak: {max_val:,.0f} @ {dt_obj.strftime('%a %I%p')}"
                except:
                    peak_str = f"Peak: {max_val:,.0f}"

            forecast_36hr = coastal_extract_36hr_forecast(forecasts)
            return {"peak": peak_str, "next36": forecast_36hr}
    except: return None

async def coastal_fetch_nws_forecast_full(session, nws_id):
    """Fetches full NWS hydrograph for 96h prediction."""
    if not nws_id: return None
    try:
        url = f"https://api.water.noaa.gov/nwps/v1/gauges/{nws_id}"
        async with session.get(url, headers={"User-Agent": "Dashboard/2.0"}, timeout=6) as resp:
            if resp.status != 200: return None
            data = await resp.json()
            return data.get("hydrograph", {}).get("forecast", [])
    except: return None

# ============================================================
# 3. HYDROLOGY & PREDICTIVE LOGIC
# ============================================================

def coastal_compute_trend(series):
    if len(series) < 2: return "↔", None, "↔ stable"
    end_time = series[-1][0]
    cutoff = end_time - dt.timedelta(hours=12)
    recent = [s for s in series if s[0] >= cutoff]
    if len(recent) < 2: recent = series
    start_val, end_val = recent[0][1], recent[-1][1]
    if start_val <= 0: return "↔", None, "↔ stable"
    pct = (end_val - start_val) / start_val * 100.0
    if pct > 5: return "↑", pct, "↑ rising"
    elif pct < -5: return "↓", pct, "↓ dropping"
    return "↔", pct, "↔ stable"

def coastal_make_sparkline_html(series, num_points=24):
    if not series: return ""
    vals = [v for (_, v) in series]
    if len(vals) > num_points:
        idx = np.linspace(0, len(vals)-1, num_points).astype(int)
        vals = [vals[i] for i in idx]
    if not vals: return ""
    min_v, max_v = min(vals), max(vals)
    norm = [(v - min_v)/(max_v - min_v) if max_v > min_v else 0.5 for v in vals]
    chars = "▁▂▃▄▅▆▇█"
    peak_idx = vals.index(max(vals))
    html = []
    for i, n in enumerate(norm):
        char = chars[int(n * (len(chars)-1))]
        # Modified colors: Rising=Orange, Dropping=Green, Peak=Red
        color = "#FF9800" if (i > 0 and vals[i] > vals[i-1]) else "#66BB6A" if (i > 0 and vals[i] < vals[i-1]) else "#9E9E9E"
        if i == peak_idx: 
            html.append(f"<span style='color:#EF5350; font-weight:bold;'>{char}</span>") # Red Peak
        else: 
            html.append(f"<span style='color:{color}'>{char}</span>")
    html.append("<span style='color:#000000; font-weight:bold;'>●</span>")
    return "".join(html)

def coastal_time_since_peak(series):
    if not series: return None
    vals = [v for (_,v) in series]
    peak_time = series[vals.index(max(vals))][0]
    return (series[-1][0] - peak_time).total_seconds() / 3600.0

def coastal_recession_rate(series):
    if len(series) < 2: return 0.0
    recent = [v for v in series if v[0] >= series[-1][0] - dt.timedelta(hours=24)]
    if len(recent) < 2: recent = series[-2:]
    t0, v0 = recent[0]; t1, v1 = recent[-1]
    hours = (t1 - t0).total_seconds() / 3600.0
    return (v1 - v0) / hours if hours > 0 else 0.0

def coastal_get_condition(val, spec, trend, hours):
    if val is None:
        if "↑" in trend: return "blown out", "#FFCDD2"
        if "↓" in trend:
            if hours is None: return "no data", "#E0E0E0"
            if hours < 12: return "slightly high", "#FFCC80"
            if hours < 36: return "in shape", "#C8E6C9"
            return "low", "#FFEB3B"
        return "no data", "#E0E0E0"
    t_str = spec.get("T", "")
    try:
        parts = t_str.lower().replace("cfs", "").replace("ft", "").split("-")
        lo, hi = float(parts[0]), float(parts[1])
    except: return "unknown", "#FFFFFF"
    if spec.get("Low") and val < spec.get("Low"): return "too low", "#E0E0E0"
    if val < lo: return "low", "#FFEB3B"
    if val > hi: return "blown out" if val > hi*1.2 else "slightly high", "#FFCDD2" if val > hi*1.2 else "#FFCC80"
    return "in shape", "#C8E6C9"

def coastal_score(val, spec, trend, series=None):
    # Added 'series' as optional parameter to fix TypeError
    if val is None: return 0.5
    try:
        lo, hi = [float(x) for x in spec["T"].lower().replace("cfs","").replace("ft","").split("-")]
        mid = (lo+hi)/2
    except: return 1.0
    
    flow_score = 1.0 + max(0.0, 1.0 - abs(val - mid)/(hi-lo)) if lo <= val <= hi else 0.5
    trend_score = 0.0 if "↑" in trend else 1.5 if "↓" in trend and lo <= val <= hi else 0.5
    
    # Handle optional series for lag calculation
    hours = coastal_time_since_peak(series) if series else None
    tsp_score = 1.0 if hours and hours > 48 else 0.3 if hours and hours > 24 else 0.0
    
    familiar_bonus = 0.5 if spec.get("Familiar") else 0.0 # Tie-breaker weight
    return max(0.0, min(5.0, flow_score + trend_score + tsp_score + familiar_bonus))

def predict_future_state(forecast_points, lead_hours):
    if not forecast_points: return None, "stable"
    target_time = dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=lead_hours)
    points = sorted(forecast_points, key=lambda x: x.get("validTime", ""))
    closest_val, trend = None, "stable"
    for i, pt in enumerate(points):
        pt_time = dt.datetime.fromisoformat(pt["validTime"].replace("Z", "+00:00"))
        if pt_time >= target_time:
            closest_val = pt.get("primary")
            if i > 0:
                prev = points[i-1].get("primary")
                if closest_val > prev * 1.05: trend = "rising"
                elif closest_val < prev * 0.95: trend = "dropping"
            break
    return closest_val, trend

def coastal_storm_cycle(trend_text, hours_since_peak):
    if "↑" in trend_text: return ("Rising", "🌧️", "#FFCDD2")
    if hours_since_peak is None: return ("Unknown", "❔", "#E0E0E0")
    if hours_since_peak < 6: return ("Peak", "🌊", "#EF9A9A")
    elif hours_since_peak < 12: return ("Early Drop", "🌈", "#FFE082")
    elif hours_since_peak < 36: return ("Prime Drop", "🔥", "#C8E6C9")
    elif hours_since_peak < 72: return ("Post‑Storm", "🌤️", "#FFF59D")
    else: return ("Low/Clear", "💧", "#BBDEFB")

# ============================================================
# 4. ORCHESTRATION (THE ASYNC LOOP)
# ============================================================

async def process_single_river(session, region, spec):
    gauges = spec.get("Gauges", [])
    result = {
        "spec": spec, "region": region, "last_val": None, "series": [],
        "source": "none", "icon": "🚫", "timestamp": None,
        "storm_eta": None, "is_modeled": False, "is_proxy": False,
        "nws_peak": None, "forecast_36hr": None, "nws_raw": [], 
        "scores": {"now": 0.0, "48h": 0.0, "96h": 0.0}
    }

    for g in gauges:
        if g.get("ID") == "NO_GAUGE": continue
        source, series = g.get("Source", "USGS"), []
        is_proxy_gauge = g.get("Is_Proxy", False)

        if source == "USGS":
            usgs_data = await coastal_fetch_usgs_async(session, g["ID"], g["P"])
            if usgs_data["value"]:
                for v in usgs_data["value"]:
                    try:
                        ts = dt.datetime.strptime(v["dateTime"][:19], "%Y-%m-%dT%H:%M:%S")
                        series.append((ts, float(v["value"])))
                    except: continue
        elif source == "CDEC":
            series = await coastal_fetch_cdec_async(session, g["ID"])

        if series:
            series.sort(key=lambda x: x[0])
            result.update({
                "last_val": series[-1][1], "series": series, "source": source,
                "icon": "📡", "timestamp": series[-1][0], "gauge_used": g
            })
            if is_proxy_gauge:
                result["is_proxy"] = True
                result["source"] = f"Proxy ({g.get('Name_Proxy', 'Neighbor')})"
            break

    eta_hours = await coastal_fetch_noaa_eta_async(session, spec)

    if spec.get("NWS_ID"):
        nws_fc = await coastal_fetch_nws_forecast(session, spec["NWS_ID"])
        if nws_fc:
            result["nws_peak"] = nws_fc.get("peak")
            result["forecast_36hr"] = nws_fc.get("next36")
        
        # New: Fetch Full Raw Forecast for 96h Prediction
        result["nws_raw"] = await coastal_fetch_nws_forecast_full(session, spec.get("NWS_ID"))

    arrow, pct, trend_text = coastal_compute_trend(result["series"])
    hours = coastal_time_since_peak(result["series"])
    
    # Calculate Scores (Fix applied here: Pass series explicitly)
    result["scores"]["now"] = coastal_score(result["last_val"], spec, trend_text, result["series"])
    for lead in [48, 96]:
        f_val, f_trend = predict_future_state(result["nws_raw"], lead)
        # Future scores don't use 'series' history, so we pass None implicitly or explicitly
        result["scores"][f"{lead}h"] = coastal_score(f_val, spec, f_trend)

    if result["last_val"] is None:
        result.update({
            "is_modeled": True, "source": "NOAA Forecast", "icon": "🧪", "timestamp": dt.datetime.now()
        })
        if eta_hours is not None and eta_hours < 24:
            cond_text, cond_color, trend_text = "likely high", "#FFCC80", "↑ rising?"
            storm_cycle, hydro_insight = ("Rising", "🌧️", "#FFCDD2"), f"🌧️ Storm in {eta_hours}h • Likely rising"
        else:
            cond_text, cond_color, trend_text = "likely low", "#FFEB3B", "↔ stable"
            storm_cycle, hydro_insight = ("Low/Clear", "💧", "#BBDEFB"), "💧 No rain forecast • Modeled"
        spark = "<span style='color:#999; font-size:0.8em;'>-- Modeled --</span>"
    else:
        cond_text, cond_color = coastal_get_condition(result["last_val"], spec, trend_text, hours)
        storm_cycle = coastal_storm_cycle(trend_text, hours)
        hydro_insight = f"{storm_cycle[1]} {storm_cycle[0]} • ETA: {eta_hours if eta_hours else '--'}h"
        spark = coastal_make_sparkline_html(result["series"])

    result.update({
        "arrow": arrow, "pct_change": pct, "trend_text": trend_text, "spark": spark,
        "cond_text": cond_text, "cond_color": cond_color,
        "storm_cycle": storm_cycle, "hydro_insight": hydro_insight,
        "time_str": result["timestamp"].strftime("%m/%d %H:%M") if result["timestamp"] else "Modeled"
    })

    return result

async def fetch_all_data():
    specs = load_coastal_region_specs()
    tasks = []
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
        for region, rivers in specs.items():
            for r in rivers: tasks.append(process_single_river(session, region, r))
        flat_results = await asyncio.gather(*tasks)
    grouped = {r: [] for r in specs.keys()}
    for res in flat_results: grouped[res["region"]].append(res)
    return grouped

# Renamed to force cache invalidation
@st.cache_data(ttl=900, show_spinner=False)
def get_dashboard_data_v3():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    return loop.run_until_complete(fetch_all_data())

# ============================================================
# 5. UI RENDERERS
# ============================================================

def render_filters():
    with st.sidebar:
        st.header("Filters")
        if st.button("🔄 Refresh Data"):
            get_dashboard_data_v3.clear()
            st.rerun()
        st.divider()
        regions = list(load_coastal_region_specs().keys())
        sel_regions = []
        st.caption("Regions")
        for r in regions:
            if st.checkbox(r, value=True, key=f"filter_{r}"):
                sel_regions.append(r)
        st.caption("Conditions")
        conds = ["in shape", "low", "slightly high", "blown out", "too low", "no data", "likely low", "likely high", "likely in shape"]
        sel_conds = []
        for c in conds:
            if st.checkbox(c.title(), value=True, key=f"cond_{c}"):
                sel_conds.append(c)
    return sel_regions, sel_conds

def coastal_render_region_summary(coastal_data):
    st.subheader("Region Summary")
    regions = list(coastal_data.items())
    cols = st.columns(3)
    for i, (region_name, entries) in enumerate(regions):
        with cols[i % 3]:
            total = len(entries)
            measured = sum(1 for e in entries if not e.get("is_modeled", False))
            estimated = total - measured
            rising = sum(1 for e in entries if "Rising" in e.get("storm_cycle", ("Unknown",))[0])
            peaking = sum(1 for e in entries if "Peak" in e.get("storm_cycle", ("Unknown",))[0])
            dropping = sum(1 for e in entries if any(x in e.get("storm_cycle", ("Unknown",))[0] for x in ["Drop", "Post"]))
            in_window = sum(1 for e in entries if e["cond_text"] in ["in shape", "likely in shape"])
            pct_window = (in_window / total * 100) if total > 0 else 0
            
            if pct_window >= 40:
                badge = "🔥 Hot"; badge_bg = "#FEE2E2"; badge_col = "#991B1B"
            elif pct_window >= 15:
                badge = "🟡 Mixed"; badge_bg = "#FEF3C7"; badge_col = "#92400E"
            elif sum(1 for e in entries if e["cond_text"] == "blown out") > (total * 0.4):
                badge = "🟥 Blown"; badge_bg = "#FEE2E2"; badge_col = "#B91C1C"
            else:
                badge = "❄️ Cold"; badge_bg = "#EFF6FF"; badge_col = "#1E40AF"
                
            tile_html = (
                f"<div style='background-color:#FFFFFF; padding:12px; border-radius:8px; margin-bottom:12px; border:1px solid #E5E7EB; box-shadow: 0 1px 2px rgba(0,0,0,0.05);'>"
                f"  <div style='display:flex; justify-content:space-between; align-items:center; margin-bottom:8px;'>"
                f"      <span style='font-weight:700; font-size:1rem; color:#111827;'>{region_name}</span>"
                f"      <span style='font-size:0.75rem; background-color:{badge_bg}; color:{badge_col}; padding:2px 8px; border-radius:12px; font-weight:600;'>{badge}</span>"
                f"  </div>"
                f"  <div style='font-size:0.8rem; color:#4B5563; line-height:1.5;'>"
                f"      <div style='display:flex; justify-content:space-between;'>"
                f"          <span>📡 <b>{measured}</b> Meas.</span>"
                f"          <span>🧪 <b>{estimated}</b> Est.</span>"
                f"      </div>"
                f"      <div style='margin-top:4px; border-top:1px solid #F3F4F6; padding-top:4px;'>"
                f"          <span title='Rising'>🌧️ {rising}</span> &nbsp;" 
                f"          <span title='Peaking'>🌊 {peaking}</span> &nbsp;" 
                f"          <span title='Dropping/Post'>📉 {dropping}</span>"
                f"      </div>"
                f"      <div style='margin-top:4px; font-weight:500; color:#059669;'>"
                f"          🎯 {in_window} Rivers Fishable ({pct_window:.0f}%)"
                f"      </div>"
                f"  </div>"
                f"</div>"
            )
            st.markdown(tile_html, unsafe_allow_html=True)

def render_top3_predictive(data, horizon="now"):
    label_map = {"now": "Right Now", "48h": "in 48 Hours", "96h": "in 96 Hours"}
    st.subheader(f"🔥 Top Recommendations ({label_map[horizon]})")
    # Flatten data for sorting
    flat_data = [item for sublist in data.values() for item in sublist]
    # Rank by score first, then familiarity
    sorted_items = sorted(flat_data, key=lambda x: (x["scores"][horizon], x["spec"].get("Familiar", False)), reverse=True)
    top3 = sorted_items[:3]
    cols = st.columns(3)
    for idx, item in enumerate(top3):
        with cols[idx]:
            fam_tag = "⭐ FAMILIAR" if item["spec"].get("Familiar") else ""
            st.markdown(f"<div style='font-size:0.7rem; font-weight:700; color:#333;'>RANK #{idx+1} • SCORE {item['scores'][horizon]:.1f} {fam_tag}</div>", unsafe_allow_html=True)
            coastal_tile(item)
    st.divider()

def coastal_get_tile_text_color_from_bg(bg):
    bg = bg.lstrip("#")
    if len(bg) != 6: return "#000000"
    r, g, b = int(bg[0:2], 16), int(bg[2:4], 16), int(bg[4:6], 16)
    return "#000000" if (0.299*r + 0.587*g + 0.114*b) > 160 else "#FFFFFF"

def coastal_tile(item):
    spec, val, bg = item["spec"], item["last_val"], item["cond_color"]
    unit = "ft" if item.get("gauge_used", {}).get("P") == "00065" else "cfs"
    
    gauge = item.get("gauge_used", {})
    source = item.get("source", "USGS").split(" ")[0]
    gid = gauge.get("ID", "")
    
    if source == "USGS" and gid != "NO_GAUGE":
        url = f"https://waterdata.usgs.gov/monitoring-location/{gid}"
    elif source == "CDEC":
        url = f"https://cdec.water.ca.gov/dynamicapp/QueryF?s={gid}"
    else:
        url = "#"

    # MODELED COLOR LOGIC (Muted palette)
    if item.get("is_modeled"):
        val_str, font_style = "Est.", "font-style:italic;"
        if bg == "#C8E6C9": bg = "#DAE8DC" # Muted Green
        elif bg == "#FFEB3B": bg = "#EFEAC5" # Muted Yellow
        elif bg == "#FFCDD2": bg = "#E6DBCE" # Muted Red
        elif bg == "#FFCC80": bg = "#F1E5D5" # Muted Orange
    else:
        val_str, font_style = f"{val:,.0f}" if val is not None else "ERR", ""
    
    fg = coastal_get_tile_text_color_from_bg(bg)
    
    # Forecast display block
    fcst_html = ""
    if item.get("nws_peak"):
        fcst_html += (
            f"<div style='font-size:0.75rem; color:{fg}; opacity:0.95; "
            f"margin-top:3px; font-weight:700; border-top: 1px dashed {fg}88; padding-top:2px;'>"
            f"🔮 {item['nws_peak']}</div>"
        )
    if item.get("forecast_36hr"):
        mini = ", ".join(f"{ts.strftime('%a %H:%M')}: {v:,.0f}" for ts, v in item["forecast_36hr"] if v is not None)
        fcst_html += (
            f"<div style='font-size:0.70rem; color:{fg}; opacity:0.85; margin-top:2px;'>"
            f"📈 Next 36 hrs: {mini}</div>"
        )

    style = f"background-color:{bg}; color:{fg}; {font_style} padding:8px; border-radius:8px; margin-bottom:10px; font-size:0.85rem; border:1px solid rgba(0,0,0,0.1); line-height:1.4;"
    
    html = (
        f'<div style="{style}">'
        f'  <div style="display:flex; justify-content:space-between; align-items:center;">'
        f'    <a href="{url}" target="_blank" style="color:{fg}; text-decoration:none; font-weight:800; border-bottom:1px dotted {fg};">{spec["Name"]} {item["icon"]}</a>'
        f'    <span style="font-weight:800;">{val_str} <span style="font-size:0.75em; font-weight:normal;">{unit}</span></span>'
        f'  </div>'
        f'  <div style="font-weight:600; opacity:0.9;">{item["cond_text"].title()} • {spec["T"]}</div>'
        f'  {fcst_html}'
        f'  <div style="font-size:0.75rem; margin-top:4px;">'
        f'    {item["spark"]} {item["trend_text"]}<br>'
        f'    {item["hydro_insight"]}<br>'
        f'    <div style="margin-top:4px; font-size:0.7rem; opacity:0.8;"><i>{spec.get("N", "")}</i></div>'
        f'  </div>'
        f'</div>'
    )
    st.markdown(html, unsafe_allow_html=True)

def render_coastal_dashboard():
    st.set_page_config(page_title="Coastal Dashboard", layout="wide")
    st.title("🌊 Coastal Conditions Dashboard")
    
    # Use v3 cache to force reload of colors
    data = get_dashboard_data_v3()
    sel_regions, sel_conds = render_filters()
    
    t1, t2, t3, t4 = st.tabs(["Current", "48h Forecast", "96h Forecast", "Map View"])
    
    # Common Render Logic
    def render_list_view(horizon):
        coastal_render_region_summary(data)
        render_top3_predictive(data, horizon)
        
        # New Legend
        st.markdown(
            """
            <div style="font-size:0.8em; margin-bottom:10px; opacity:0.8;">
            <b>Sparkline Key:</b> 
            <span style="color:#FF9800;">■</span> Rising &nbsp; 
            <span style="color:#66BB6A;">■</span> Dropping &nbsp; 
            <span style="color:#EF5350;">■</span> Peak
            </div>
            """, 
            unsafe_allow_html=True
        )
        
        st.subheader("📍 Regions")
        for region, entries in data.items():
            if region not in sel_regions: continue
            visible = [e for e in entries if e["cond_text"] in sel_conds]
            if not visible: continue
            with st.expander(region, expanded=True):
                cols = st.columns(3)
                for i, item in enumerate(visible):
                    with cols[i % 3]: coastal_tile(item)

    with t1: render_list_view("now")
    with t2: render_list_view("48h")
    with t3: render_list_view("96h")
    with t4:
        filters_dict = {"regions": sel_regions, "status": sel_conds}
        render_coastal_map(data, filters_dict)

if __name__ == "__main__":
    render_coastal_dashboard()