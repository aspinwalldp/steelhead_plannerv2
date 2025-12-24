# -*- coding: utf-8 -*-
import streamlit as st
import datetime as dt
import pandas as pd
import asyncio
import aiohttp
import numpy as np

# Import map module
try:
    from map_v2 import render_coastal_map
except ImportError:
    def render_coastal_map(data, filters):
        st.warning("Map module not found. Please ensure map_v2.py is present.")

# ============================================================
# 1. REGION SPECS (UPDATED PER HYDROLOGIC TELEMETRY REPORT)
# ============================================================

def load_coastal_region_specs():
    # 1. NORTHERN CALIFORNIA
    COASTAL_NORCAL = [
        {
            "Name": "Gualala", "lat": 38.76, "lon": -123.53, 
            "Gauges": [
                {"ID": "11467510", "P": "00060", "Source": "USGS"}, # Primary: SF Gualala (Index per report)
                {"ID": "11467500", "P": "00060", "Source": "USGS"}  # Secondary: Mainstem (often bar-affected)
            ], 
            "T": "250-1200 cfs", "Low": 200, "Type": "Sedimentary", "N": "Bar-dependent. Using S.Fork index.", "NOAA_zone": "CAC03"
        },
        {
            "Name": "Garcia", "lat": 38.91, "lon": -123.70, 
            "Gauges": [
                {"ID": "11467600", "P": "00060", "Source": "USGS"}, # Primary: Eureka Hill (Compliance point)
                {"ID": "GRC", "P": "20", "Source": "CDEC"}         # Secondary: CDEC Mirror
            ], 
            "T": "250-1100 cfs", "Low": 200, "Type": "Flashy", "N": "Compliance point. Highly reliable.", "NOAA_zone": "CAC03"
        },
        {
            "Name": "Navarro", "lat": 39.15, "lon": -123.67, 
            "Gauges": [
                {"ID": "11468000", "P": "00060", "Source": "USGS"}, # Primary: Near Navarro
                {"ID": "NAV", "P": "20", "Source": "CDEC"}         # Secondary: CDEC Mirror
            ], 
            "T": "350-1500 cfs", "Low": 250, "Type": "Sedimentary", "N": "Gold standard gauge. Road flood index.", "NOAA_zone": "CAC03"
        },
        {
            "Name": "Mattole", "lat": 40.28, "lon": -124.35, 
            "Gauges": [
                {"ID": "11468900", "P": "00060", "Source": "USGS"}, # Primary: Ettersburg (Headwaters/Response)
                {"ID": "11469000", "P": "00060", "Source": "USGS"}, # Secondary: Petrolia (Mouth/Cumulative)
                {"ID": "MAT", "P": "20", "Source": "CDEC"}         # Tertiary: CDEC
            ], 
            "T": "500-3000 cfs", "Low": 200, "Type": "Flashy", "N": "Using Ettersburg (Headwaters).", "NOAA_zone": "CAC02"
        },
        {
            "Name": "S.F. Eel (Miranda)", "lat": 40.23, "lon": -123.79, 
            "Gauges": [{"ID": "11476500", "P": "00060", "Source": "USGS"}], 
            "T": "800-4000 cfs", "Low": 300, "Type": "Medium", "N": "Redwoods run. Clears slowly.", "NOAA_zone": "CAC02"
        },
        {
            "Name": "Eel (Mainstem)", "lat": 40.49, "lon": -124.10, 
            "Gauges": [{"ID": "11477000", "P": "00060", "Source": "USGS"}], 
            "T": "2000-15000 cfs", "Low": 1000, "Type": "Large", "N": "The main stem. Big water swinging.", "NOAA_zone": "CAC02"
        },
        {
            "Name": "Van Duzen", "lat": 40.48, "lon": -124.13, 
            "Gauges": [{"ID": "11478500", "P": "00060", "Source": "USGS"}], 
            "T": "500-2500 cfs", "Low": 250, "Type": "Flashy", "N": "Flashy tributary. Clears fast.", "NOAA_zone": "CAC02"
        },
        {
            "Name": "Mad River", "lat": 40.89, "lon": -124.03, 
            "Gauges": [{"ID": "11481000", "P": "00060", "Source": "USGS"}], 
            "T": "600-3500 cfs", "Low": 300, "Type": "Flashy", "N": "Hatchery run. Can be crowded.", "NOAA_zone": "CAC02"
        },
        {
            "Name": "Redwood Creek", "lat": 41.29, "lon": -124.09, 
            "Gauges": [{"ID": "11482500", "P": "00060", "Source": "USGS"}], 
            "T": "400-1500 cfs", "Low": 300, "Type": "Flashy", "N": "Sand-choked mouth; very storm-sensitive.", "NOAA_zone": "CAC02"
        },
        {
            "Name": "M.F. Smith", "lat": 41.84, "lon": -123.94, 
            "Gauges": [{"ID": "11532650", "P": "00060", "Source": "USGS"}], # Primary: Gasquet
            "T": "800-3000 cfs", "Low": 400, "Type": "Flashy", "N": "Gasquet gauge. Technical wading.", "NOAA_zone": "CAC01"
        },
        {
            "Name": "Smith River (CA)", "lat": 41.90, "lon": -124.13, 
            "Gauges": [{"ID": "11532500", "P": "00060", "Source": "USGS"}], 
            "T": "1500-7500 cfs", "Low": 600, "Type": "Flashy", "N": "Holy Grail. Tidewater to Craigs.", "NOAA_zone": "CAC01"
        },
    ]

    # 2. SOUTHERN OREGON COAST
    COASTAL_SOUTH_OR = [
        {
            "Name": "Winchuck", "lat": 42.02, "lon": -124.22, 
            "Gauges": [
                {"ID": "14400200", "P": "00060", "Source": "USGS"}, # Primary: Near Brookings
                {"ID": "14400200", "Source": "OWRD"} # Secondary: OWRD
            ],
            "T": "250-900 cfs", "Low": 200, "Type": "Flashy", "N": "Tiny, brushy. Intermittent federal funding.", "NOAA_zone": "ORC01"
        },
        {
            "Name": "Chetco", "lat": 42.08, "lon": -124.23, 
            "Gauges": [{"ID": "14400000", "P": "00060", "Source": "USGS"}], 
            "T": "1500-5000 cfs", "Low": 800, "Type": "Flashy", "N": "Similar to Smith. Drops fast.", "NOAA_zone": "ORC01"
        },
        {
            "Name": "Pistol", "lat": 42.27, "lon": -124.39, 
            "Gauges": [{"ID": "14400000", "P": "00060", "Source": "USGS", "Is_Proxy": True, "Name_Proxy": "Chetco"}], 
            "T": "300-1100 cfs", "Low": 250, "Type": "Flashy", "N": "UNGAUGED. Proxy: Chetco Trends.", "NOAA_zone": "ORC01"
        },
        {
            "Name": "Illinois", "lat": 42.44, "lon": -124.08, 
            "Gauges": [{"ID": "14377100", "P": "00060", "Source": "USGS"}], # Primary: Kerby
            "T": "1000-4000 cfs", "Low": 800, "Type": "Mixed", "N": "Kerby gauge (NWS Forecast Point).", "NOAA_zone": "ORC01"
        },
        {
            "Name": "Rogue (Agness)", "lat": 42.55, "lon": -124.06, 
            "Gauges": [{"ID": "14372300", "P": "00060", "Source": "USGS"}], 
            "T": "2000-8000 cfs", "Low": 1500, "Type": "Medium", "N": "Classic canyon water. Jet boat access.", "NOAA_zone": "ORC01"
        },
        {
            "Name": "Elk River", "lat": 42.74, "lon": -124.45, 
            "Gauges": [
                {"ID": "14327250", "P": "00065", "Source": "USGS"},
                {"ID": "14327250", "Source": "OWRD"}
            ], 
            "T": "4.0-6.0 ft", "Low": 3.5, "Type": "Flashy", "N": "Fish factory. Small water.", "NOAA_zone": "ORC02"
        },
        {
            "Name": "Sixes", "lat": 42.82, "lon": -124.46, 
            "Gauges": [{"ID": "14327150", "P": "00065", "Source": "USGS"}], 
            "T": "5.0-9.0 ft", "Low": 4.0, "Type": "Flashy", "N": "Sibling to the Elk. Muddy banks.", "NOAA_zone": "ORC02"
        },
        {
            "Name": "Floras/New River", "lat": 42.92, "lon": -124.45, 
            "Gauges": [{"ID": "14327137", "P": "00060", "Source": "USGS"}], # Primary: Langlois
            "T": "300-1100 cfs", "Low": 200, "Type": "Flashy", "N": "Langlois gauge. Represents New River.", "NOAA_zone": "ORC02"
        },
    ]

    # 3. CENTRAL OREGON COAST
    COASTAL_CENTRAL_OR = [
        {"Name": "Coquille (S.F.)", "lat": 43.05, "lon": -124.16, "Gauges": [{"ID": "14325000", "P": "00065", "Source": "USGS"}], "T": "4.0-9.0 ft", "Low": 2.0, "Type": "Medium", "N": "Softer flows. Good drifting.", "NOAA_zone": "ORC03"},
        {"Name": "Coquille (N.F.)", "lat": 43.15, "lon": -124.11, "Gauges": [{"ID": "14325000", "P": "00060", "Source": "USGS"}], "T": "600-2200 cfs", "Low": 450, "Type": "Mixed", "N": "Smaller/clearer than SF Coquille.", "NOAA_zone": "ORC03"},
        {"Name": "Coquille (Main)", "lat": 43.11, "lon": -124.40, "Gauges": [{"ID": "14326500", "P": "00060", "Source": "USGS"}], "T": "2500-8500 cfs", "Low": 1600, "Type": "Mixed", "N": "Tidal. Data often stage only.", "NOAA_zone": "ORC03"},
        {"Name": "Coos/Millicoma", "lat": 43.40, "lon": -124.08, "Gauges": [{"ID": "14325000", "P": "00060", "Source": "USGS"}], "T": "700-2600 cfs", "Low": 500, "Type": "Mixed", "N": "Millicoma forks feed Coos tidal.", "NOAA_zone": "ORC03"},
        # Tenmile: Ungauged/Lake Level. No direct proxy suitable for cfs.
        {"Name": "Tenmile", "lat": 43.59, "lon": -124.20, "Gauges": [{"ID": "NO_GAUGE", "Source": "None"}], "T": "250-900 cfs", "Low": 200, "Type": "Flashy", "N": "UNGAUGED. Monitor lake levels locally.", "NOAA_zone": "ORC03"},
        {"Name": "Umpqua (Mainstem)", "lat": 43.63, "lon": -123.63, "Gauges": [{"ID": "14321000", "P": "00060", "Source": "USGS"}], "T": "3000-12000 cfs", "Low": 2000, "Type": "Large", "N": "Big emerald water. Winter steelhead mecca.", "NOAA_zone": "ORC04"},
        {"Name": "North Umpqua", "lat": 43.30, "lon": -123.10, "Gauges": [{"ID": "14319500", "P": "00060", "Source": "USGS"}], "T": "1200-4000 cfs", "Low": 900, "Type": "Mixed", "N": "Colder, regulated; clarity holds.", "NOAA_zone": "ORC04"},
    ]

    # 4. NORTHERN OREGON COAST
    COASTAL_NORTH_OR = [
        {"Name": "Siuslaw", "lat": 44.06, "lon": -123.95, "Gauges": [{"ID": "14307620", "P": "00065", "Source": "USGS"}], "T": "4.0-10.0 ft", "Low": 3.0, "Type": "Medium", "N": "Coastal range drain. Tea colored.", "NOAA_zone": "ORC05"},
        {"Name": "Alsea", "lat": 44.38, "lon": -123.83, "Gauges": [{"ID": "14306500", "P": "00065", "Source": "USGS"}], "T": "3.0-9.0 ft", "Low": 2.0, "Type": "Flashy", "N": "Popular drift. Hatchery programs.", "NOAA_zone": "ORC05"},
        {"Name": "Siletz", "lat": 44.71, "lon": -123.89, "Gauges": [{"ID": "14305500", "P": "00065", "Source": "USGS"}], "T": "3.5-8.0 ft", "Low": 2.5, "Type": "Medium", "N": "Beautiful gorge. Native runs.", "NOAA_zone": "ORC05"},
        {"Name": "Salmon (OR)", "lat": 45.02, "lon": -123.90, "Gauges": [{"ID": "14305500", "P": "00060", "Source": "USGS"}], "T": "400-1400 cfs", "Low": 300, "Type": "Flashy", "N": "Smaller neighbor to Siletz.", "NOAA_zone": "ORC05"},
        
        # Little Nestucca: Proxy Nestucca
        {
            "Name": "Little Nestucca", "lat": 45.13, "lon": -123.94, 
            "Gauges": [{"ID": "14303600", "P": "00065", "Source": "USGS", "Is_Proxy": True, "Name_Proxy": "Nestucca"}], 
            "T": "300-1200 cfs", "Low": 250, "Type": "Flashy", "N": "UNGAUGED. Proxy: Nestucca Trends.", "NOAA_zone": "ORC06"
        },
        {"Name": "Nestucca", "lat": 45.24, "lon": -123.88, "Gauges": [{"ID": "14303600", "P": "00065", "Source": "USGS"}], "T": "3.5-7.5 ft", "Low": 2.5, "Type": "Flashy", "N": "Broodstock river. Busy but productive.", "NOAA_zone": "ORC06"},
        
        # Three Rivers: Proxy Nestucca
        {
            "Name": "Three Rivers", "lat": 45.22, "lon": -123.88, 
            "Gauges": [{"ID": "14303600", "P": "00065", "Source": "USGS", "Is_Proxy": True, "Name_Proxy": "Nestucca"}], 
            "T": "200-800 cfs", "Low": 180, "Type": "Flashy", "N": "UNGAUGED. Proxy: Nestucca Trends.", "NOAA_zone": "ORC06"
        },
        {"Name": "Trask", "lat": 45.43, "lon": -123.77, "Gauges": [{"ID": "14302480", "P": "00065", "Source": "USGS"}], "T": "4.0-8.0 ft", "Low": 3.0, "Type": "Flashy", "N": "Tillamook bay feeder. Drops quick.", "NOAA_zone": "ORC07"},
        {"Name": "Wilson", "lat": 45.48, "lon": -123.74, "Gauges": [{"ID": "14301500", "P": "00065", "Source": "USGS"}], "T": "3.5-7.5 ft", "Low": 2.5, "Type": "Flashy", "N": "Portland's backyard. Good access.", "NOAA_zone": "ORC07"},
        
        # Kilchis: Proxy Miami
        {
            "Name": "Kilchis", "lat": 45.54, "lon": -123.87, 
            "Gauges": [{"ID": "14301300", "P": "00060", "Source": "USGS", "Is_Proxy": True, "Name_Proxy": "Miami"}], 
            "T": "500-1800 cfs", "Low": 350, "Type": "Flashy", "N": "UNGAUGED. Proxy: Miami Trends.", "NOAA_zone": "ORC07"
        },
        {"Name": "Miami", "lat": 45.58, "lon": -123.89, "Gauges": [{"ID": "14301300", "P": "00060", "Source": "USGS"}], "T": "250-900 cfs", "Low": 200, "Type": "Flashy", "N": "Smallest Tillamook creek.", "NOAA_zone": "ORC07"},
        {"Name": "Nehalem", "lat": 45.71, "lon": -123.75, "Gauges": [{"ID": "14301000", "P": "00065", "Source": "USGS"}], "T": "3.5-9.0 ft", "Low": 2.5, "Type": "Large", "N": "Long river. Wild fish stronghold.", "NOAA_zone": "ORC08"},
        {"Name": "N.F. Nehalem", "lat": 45.78, "lon": -123.80, "Gauges": [{"ID": "14299000", "P": "00060", "Source": "USGS"}], "T": "400-1500 cfs", "Low": 300, "Type": "Flashy", "N": "Small hatchery river.", "NOAA_zone": "ORC08"},
        {"Name": "Necanicum", "lat": 45.98, "lon": -123.91, "Gauges": [{"ID": "14297000", "P": "00060", "Source": "USGS"}], "T": "300-1200 cfs", "Low": 250, "Type": "Flashy", "N": "Coastal creek near Seaside.", "NOAA_zone": "ORC08"},
    ]

    # 5. WASHINGTON COAST
    COASTAL_WA_COAST = [
        {"Name": "Willapa", "lat": 46.65, "lon": -123.72, "Gauges": [{"ID": "12010000", "P": "00060", "Source": "USGS"}], "T": "800-2600 cfs", "Low": 600, "Type": "Mixed", "N": "Tidal bay system.", "NOAA_zone": "WAC01"},
        {"Name": "North River", "lat": 46.81, "lon": -123.84, "Gauges": [{"ID": "12017000", "P": "00060", "Source": "USGS"}], "T": "400-1500 cfs", "Low": 300, "Type": "Mixed", "N": "Small coastal river S of Grays Harbor.", "NOAA_zone": "WAC01"},
        {"Name": "Johns River", "lat": 46.89, "lon": -124.01, "Gauges": [{"ID": "12017500", "P": "00060", "Source": "USGS"}], "T": "200-800 cfs", "Low": 180, "Type": "Flashy", "N": "Small coastal creek into South Bay.", "NOAA_zone": "WAC02"},
        {"Name": "Satsop", "lat": 47.00, "lon": -123.49, "Gauges": [{"ID": "12035000", "P": "00060", "Source": "USGS"}], "T": "1500-7000 cfs", "Low": 600, "Type": "Medium", "N": "Chehalis trib. Broad gravel bars.", "NOAA_zone": "WAC02"},
        {"Name": "Wynoochee", "lat": 47.07, "lon": -123.63, "Gauges": [{"ID": "12037400", "P": "00060", "Source": "USGS"}], "T": "1000-5000 cfs", "Low": 400, "Type": "Medium", "N": "Dam controlled. Consistent flows.", "NOAA_zone": "WAC02"},
        {"Name": "Wishkah", "lat": 47.08, "lon": -123.80, "Gauges": [{"ID": "12038000", "P": "00060", "Source": "USGS"}], "T": "400-1400 cfs", "Low": 300, "Type": "Flashy", "N": "Smaller Grays Harbor trib.", "NOAA_zone": "WAC02"},
        {"Name": "Hoquiam", "lat": 47.11, "lon": -123.86, "Gauges": [{"ID": "12038660", "P": "00060", "Source": "USGS"}], "T": "300-1200 cfs", "Low": 250, "Type": "Flashy", "N": "East Fork Hoquiam (Freshwater index).", "NOAA_zone": "WAC02"},
        {"Name": "Humptulips", "lat": 47.23, "lon": -124.03, "Gauges": [{"ID": "12039005", "P": "00060", "Source": "USGS"}], "T": "1500-6000 cfs", "Low": 600, "Type": "Flashy", "N": "Hatchery powerhouse. Plunking low down.", "NOAA_zone": "WAC02"},
    ]

    # 6. OLYMPIC PENINSULA
    COASTAL_OP = [
        {"Name": "Quinault", "lat": 47.47, "lon": -123.86, "Gauges": [{"ID": "12039500", "P": "00060", "Source": "USGS"}], "T": "2000-10000 cfs", "Low": 1200, "Type": "Glacial", "N": "Rainforest giant. Tribal guides mostly.", "NOAA_zone": "WAC04"},
        {"Name": "Queets", "lat": 47.53, "lon": -124.31, "Gauges": [{"ID": "12040500", "P": "00060", "Source": "USGS"}], "T": "2000-8000 cfs", "Low": 1000, "Type": "Glacial", "N": "Remote. The wild west.", "NOAA_zone": "WAC04"},
        # Clearwater: Proxy Queets (Combined flow)
        {
            "Name": "Clearwater", "lat": 47.58, "lon": -124.28, 
            "Gauges": [{"ID": "12040500", "P": "00060", "Source": "USGS", "Is_Proxy": True, "Name_Proxy": "Queets"}], 
            "T": "400-1500 cfs", "Low": 300, "Type": "Flashy", "N": "UNGAUGED. Proxy: Queets (Downstream).", "NOAA_zone": "WAC04"
        },
        {"Name": "Hoh", "lat": 47.81, "lon": -124.25, "Gauges": [{"ID": "12041200", "P": "00060", "Source": "USGS"}], "T": "1500-6000 cfs", "Low": 800, "Type": "Glacial", "N": "Milky green. Steelhead capital.", "NOAA_zone": "WAC04"},
        {"Name": "Bogachiel", "lat": 47.93, "lon": -124.40, "Gauges": [{"ID": "12043015", "P": "00060", "Source": "USGS"}], "T": "1000-6000 cfs", "Low": 500, "Type": "Flashy", "N": "Gauge Near La Push (Better coverage).", "NOAA_zone": "WAC03"},
        {"Name": "Calawah", "lat": 47.95, "lon": -124.37, "Gauges": [{"ID": "12043000", "P": "00060", "Source": "USGS"}], "T": "1000-4000 cfs", "Low": 500, "Type": "Flashy", "N": "Steep and fast. The 'Calawash'.", "NOAA_zone": "WAC03"},
        {"Name": "Sol Duc", "lat": 47.98, "lon": -124.50, "Gauges": [{"ID": "12041500", "P": "00060", "Source": "USGS"}], "T": "600-2400 cfs", "Low": 350, "Type": "Mixed", "N": "Green glacial flavor; swing structure.", "NOAA_zone": "WAC03"},
        {"Name": "Dickey", "lat": 47.94, "lon": -124.60, "Gauges": [{"ID": "12042800", "P": "00060", "Source": "USGS"}], "T": "300-1100 cfs", "Low": 220, "Type": "Flashy", "N": "Small OP trib; tight windows.", "NOAA_zone": "WAC03"},
        # Quillayute: Estuary. Proxy: Bogachiel Trend
        {
            "Name": "Quillayute", "lat": 47.91, "lon": -124.62, 
            "Gauges": [{"ID": "12043015", "P": "00060", "Source": "USGS", "Is_Proxy": True, "Name_Proxy": "Bogachiel"}], 
            "T": "1500-5500 cfs", "Low": 1000, "Type": "Mixed", "N": "Estuary. Using Bogachiel Trend.", "NOAA_zone": "WAC03"
        },
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
# 2. ASYNC DATA FETCHING (MULTI-SOURCE)
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
    """
    Async CDEC fetcher (California).
    Common Sensors: 20=Flow (cfs), 1=Stage (ft), 6=Stage (ft)
    """
    try:
        # CDEC CSV service: duration='E' (Event/Hourly)
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
            
            # Simple CSV Parsing
            lines = text.strip().split("\n")
            series = []
            for line in lines:
                parts = line.split(",")
                if len(parts) < 6: continue
                # Format: STATION_ID, DUR, SENSOR, TYPE, DATE, TIME, VALUE, ...
                try:
                    # Date/Time is usually in col 4 and 5 (index 4, 5) or combined
                    date_str = parts[4] + " " + parts[5]
                    val_str = parts[6]
                    try:
                        ts = dt.datetime.strptime(date_str, "%Y%m%d %H%M")
                    except:
                        ts = dt.datetime.strptime(date_str, "%m/%d/%Y %H:%M")
                    
                    # Handle "BRT" or invalid values
                    if val_str.replace('.','',1).isdigit():
                        val = float(val_str)
                        series.append((ts, val))
                except:
                    continue
            return series
    except Exception:
        return []

async def coastal_fetch_owrd_async(session, station_id):
    """
    Async OWRD fetcher (Oregon).
    Fetches tab-separated values from OWRD Near Real Time tool.
    """
    try:
        url = "https://apps.wrd.state.or.us/apps/sw/hydro_near_real_time/hydro_download.aspx"
        # OWRD uses format: station_nbr, start_date, end_date, format
        start = (dt.datetime.now() - dt.timedelta(days=3)).strftime("%m/%d/%Y")
        end = dt.datetime.now().strftime("%m/%d/%Y")
        params = {
            "station_nbr": station_id,
            "start_date": start,
            "end_date": end,
            "format": "html" # HTML table is often more stable than their "tsv" download which triggers file events
        }
        # Note: OWRD is tricky. We'll try a simpler scraping approach if this fails, 
        # but the /hydro_download endpoint is the cleanest if it works. 
        # For this example, we will assume a successful HTML/TSV return or fail gracefully.
        
        # Simpler approach: Use the USGS mirror for OWRD if available, but if not:
        # We will return empty for now as OWRD scraping is complex without a dedicated API key.
        # This is a placeholder for where you'd put the specific OWRD scraping logic.
        return [] 
    except:
        return []

async def coastal_fetch_wadoe_async(session, station_id):
    """
    Async WADOE fetcher (Washington Dept of Ecology).
    """
    try:
        # WADOE API: https://data.wa.gov/resource/...
        # Placeholder for robust implementation.
        return []
    except:
        return []

async def coastal_fetch_noaa_eta_async(session, spec):
    """Async equivalent of coastal_fetch_noaa_eta."""
    zone = spec.get("NOAA_zone")
    if not zone: return None

    try:
        # GRID POINT MAPPING
        if "CAC" in zone: office, gx, gy = "EKA", 50, 160
        elif "ORC" in zone: office, gx, gy = "PQR", 110, 80
        elif "WAC" in zone: office, gx, gy = "SEW", 140, 80
        else: return None

        url = f"https://api.weather.gov/gridpoints/{office}/{gx},{gy}/forecast/hourly"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        async with session.get(url, headers=headers, timeout=5) as response:
            if response.status != 200: return None
            r = await response.json()
            periods = r.get("properties", {}).get("periods", [])
            for i, p in enumerate(periods):
                pop = p.get("probabilityOfPrecipitation", {}).get("value", 0)
                if pop and pop >= 50:
                    return i
        return None
    except:
        return None

# ============================================================
# 3. HYDROLOGY LOGIC
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
        color = "#9E9E9E"
        if i > 0:
            if vals[i] > vals[i-1]: color = "#42A5F5"
            elif vals[i] < vals[i-1]: color = "#66BB6A"
        
        if i == peak_idx: html.append(f"<span style='color:#FFD700; font-weight:bold;'>{char}</span>")
        else: html.append(f"<span style='color:{color}'>{char}</span>")
    
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
    t0, v0 = recent[0]
    t1, v1 = recent[-1]
    hours = (t1 - t0).total_seconds() / 3600.0
    return (v1 - v0) / hours if hours > 0 else 0.0

def coastal_basin_lag_modifier(spec, hours):
    t = spec.get("Type", "").lower()
    if hours is None: return 0.0
    if t == "flashy": return -0.2 if hours < 12 else 0.3 if hours >= 24 else 0.0
    if t == "mixed": return -0.3 if hours < 24 else 0.1 if hours >= 48 else -0.1
    return 0.0

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

def coastal_score(val, spec, trend, series):
    if val is None: return 0.5
    try:
        lo, hi = [float(x) for x in spec["T"].lower().replace("cfs","").replace("ft","").split("-")]
        mid = (lo+hi)/2
    except: return 1.0
    
    flow_score = 1.0 + max(0.0, 1.0 - abs(val - mid)/(hi-lo)) if lo <= val <= hi else 0.5
    trend_score = 0.0 if "↑" in trend else 1.5 if "↓" in trend and lo <= val <= hi else 0.5
    
    slope = coastal_recession_rate(series)
    pct_hr = (slope/val)*100 if val else 0
    rec_score = 0.5 if pct_hr < -5 else 0.3 if pct_hr < -1 else 0.1 if pct_hr < 0 else 0.0
    
    hours = coastal_time_since_peak(series)
    lag_score = coastal_basin_lag_modifier(spec, hours)
    tsp_score = 1.0 if hours and hours > 48 else 0.3 if hours and hours > 24 else 0.0
    
    return max(0.0, min(5.0, flow_score + trend_score + rec_score + lag_score + tsp_score))

def coastal_storm_window(hours_since_peak):
    if hours_since_peak is None: return "Window —"
    if hours_since_peak < 12: return "Window Closed"
    elif hours_since_peak < 36: return "Window OPEN"
    elif hours_since_peak < 60: return "Window Closing"
    else: return "Window Low/Clear"

def coastal_basin_lag_label(spec):
    t = spec.get("Type", "").lower()
    if t == "flashy": return "Lag 6–12h"
    if t == "mixed": return "Lag 12–24h"
    if t in ["sedimentary", "glacial"]: return "Lag 24–48h"
    return "Lag —"

def coastal_format_storm_eta(hours):
    if hours is None: return "Storm ETA —"
    if hours == 0: return "Storm ETA now"
    if hours == 1: return "Storm ETA 1h"
    return f"Storm ETA {hours}h"

def coastal_trend_strength(series, current_val):
    if len(series) < 2 or current_val is None or current_val == 0: return "stable"
    slope_cfs = coastal_recession_rate(series)
    pct_per_hour = (slope_cfs / current_val) * 100.0
    if pct_per_hour > 5.0: return "strong rise"
    if pct_per_hour > 1.0: return "mild rise"
    if pct_per_hour < -5.0: return "strong drop"
    if pct_per_hour < -1.0: return "mild drop"
    return "stable"

def coastal_trend_strength_text(trend_strength, storm_cycle_label):
    ts = trend_strength.lower()
    cycle = storm_cycle_label
    if cycle == "Rising":
        if ts == "stable": return "Slow rise beginning"
        if "mild" in ts: return "Steady rise underway"
        if "strong" in ts: return "Sharp rise — storm pulse incoming"
        return "Rise detected"
    if cycle in ["Early Drop", "Prime Drop", "Post‑Storm", "Peak"]:
        if ts == "stable": return "Drop beginning — early recession"
        if "mild" in ts: return "Clean recession — shaping up"
        if "strong" in ts: return "Fast drop — prime window opening"
        return "Recession ongoing"
    if cycle == "Low/Clear": return "Low and clear conditions"
    return trend_strength 

def coastal_storm_cycle(trend_text, hours_since_peak):
    if "↑" in trend_text: return ("Rising", "🌧️", "#FFCDD2")
    if hours_since_peak is None: return ("Unknown", "❔", "#E0E0E0")
    if hours_since_peak < 6: return ("Peak", "🌊", "#EF9A9A")
    elif hours_since_peak < 12: return ("Early Drop", "🌈", "#FFE082")
    elif hours_since_peak < 36: return ("Prime Drop", "🔥", "#C8E6C9")
    elif hours_since_peak < 72: return ("Post‑Storm", "🌤️", "#FFF59D")
    else: return ("Low/Clear", "💧", "#BBDEFB")

def coastal_hydro_insight(storm_cycle, trend_strength, window, lag_label, storm_eta):
    label, emoji, _ = storm_cycle
    trend_text = coastal_trend_strength_text(trend_strength, label)
    return f"{emoji} {trend_text} • {window} • {lag_label} • {storm_eta}"

# ============================================================
# 4. ORCHESTRATION (THE ASYNC LOOP)
# ============================================================

async def process_single_river(session, region, spec):
    gauges = spec.get("Gauges", [])
    result = {
        "spec": spec, "region": region, "last_val": None, "series": [], 
        "source": "none", "confidence": "none", "icon": "🚫", "timestamp": None,
        "storm_eta": None, "is_modeled": False, "is_proxy": False
    }
    
    # 1. Fetch Hydrology (Multi-Source Dispatch)
    for g in gauges:
        # Check if dummy NO_GAUGE
        if g.get("ID") == "NO_GAUGE":
            continue

        source = g.get("Source", "USGS")
        series = []
        
        # Check if this gauge is flagged as a Proxy
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
            
        elif source == "OWRD":
            series = await coastal_fetch_owrd_async(session, g["ID"])
            
        elif source == "WADOE":
            series = await coastal_fetch_wadoe_async(session, g["ID"])
            
        if series:
            series.sort(key=lambda x: x[0])
            result["last_val"] = series[-1][1]
            result["series"] = series
            result["source"] = source
            result["confidence"] = "low" if is_proxy_gauge else "high"
            result["icon"] = "📡"
            result["timestamp"] = series[-1][0]
            result["gauge_used"] = g
            
            # Important: Mark if we are using a proxy so we can style it differently
            if is_proxy_gauge:
                result["is_proxy"] = True
                result["source"] = f"Proxy ({g.get('Name_Proxy', 'Neighbor')})"
                
            break # Found valid data, stop searching sources
    
    # 2. Fetch NOAA ETA
    eta_hours = await coastal_fetch_noaa_eta_async(session, spec)
    
    # 3. Compute Logic (Initial)
    arrow, pct, trend_text = coastal_compute_trend(result["series"])
    hours = coastal_time_since_peak(result["series"])
    
    # --- FALLBACK: PREDICTED HYDROLOGY FOR MISSING RIVERS ---
    if result["last_val"] is None:
        result["is_modeled"] = True
        result["source"] = "NOAA Forecast"
        result["confidence"] = "low"
        result["icon"] = "🧪"
        result["timestamp"] = dt.datetime.now()
        
        # Heuristic: If storm is close (<24h), assume rising/high. If far (>72h), assume low.
        if eta_hours is not None and eta_hours < 24:
            cond_text = "likely high"
            cond_color = "#FFCC80" # Orange
            trend_text = "↑ rising?"
            storm_cycle = ("Rising", "🌧️", "#FFCDD2")
            hydro_insight = f"🌧️ Storm in {eta_hours}h • Likely rising"
        elif eta_hours is None or eta_hours > 72:
            cond_text = "likely low"
            cond_color = "#FFEB3B" # Yellow
            trend_text = "↔ stable"
            storm_cycle = ("Low/Clear", "💧", "#BBDEFB")
            hydro_insight = "💧 No rain forecast • Likely low"
        else:
            cond_text = "likely in shape"
            cond_color = "#C8E6C9" # Green
            trend_text = "↓ dropping?"
            storm_cycle = ("Prime Drop", "🔥", "#C8E6C9")
            hydro_insight = "🌤️ Storm window open • Modeled"
            
        score = 2.5
        result["spark"] = "<span style='color:#999; font-size:0.8em;'>-- Modeled --</span>"
        
    else:
        # Standard Measured Logic
        cond_text, cond_color = coastal_get_condition(result["last_val"], spec, trend_text, hours)
        score = coastal_score(result["last_val"], spec, trend_text, result["series"])
        
        # V1 Insights Logic
        storm_cycle = coastal_storm_cycle(trend_text, hours)
        trend_strength = coastal_trend_strength(result["series"], result["last_val"])
        lag_label = coastal_basin_lag_label(spec)
        window = coastal_storm_window(hours)
        storm_eta = coastal_format_storm_eta(eta_hours)
        
        hydro_insight = coastal_hydro_insight(storm_cycle, trend_strength, window, lag_label, storm_eta)
    
    # Populate result
    result.update({
        "arrow": arrow, "pct_change": pct, "trend_text": trend_text,
        "spark": result.get("spark") if result.get("spark") else coastal_make_sparkline_html(result["series"]),
        "cond_text": cond_text, "cond_color": cond_color, "score": score,
        "storm_cycle": storm_cycle, 
        "hydro_insight": hydro_insight,
        "time_str": result["timestamp"].strftime("%m/%d %H:%M") if result["timestamp"] else "Modeled"
    })
    
    return result

async def fetch_all_data():
    specs = load_coastal_region_specs()
    tasks = []
    
    # Disable SSL for speed on Windows/Mac
    connector = aiohttp.TCPConnector(ssl=False)
    
    async with aiohttp.ClientSession(connector=connector) as session:
        for region, rivers in specs.items():
            for r in rivers:
                tasks.append(process_single_river(session, region, r))
        
        flat_results = await asyncio.gather(*tasks)
        
    # Re-group by region
    grouped = {r: [] for r in specs.keys()}
    for res in flat_results:
        grouped[res["region"]].append(res)
        
    return grouped

@st.cache_data(ttl=900, show_spinner=False)
def get_dashboard_data():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    return loop.run_until_complete(fetch_all_data())

# ============================================================
# 5. UI RENDERERS (EXACT V1 STYLE)
# ============================================================

def coastal_get_tile_text_color_from_bg(bg):
    bg = bg.lstrip("#")
    if len(bg) != 6: return "#000000"
    r, g, b = int(bg[0:2], 16), int(bg[2:4], 16), int(bg[4:6], 16)
    return "#000000" if (0.299*r + 0.587*g + 0.114*b) > 160 else "#FFFFFF"

def coastal_tile(item):
    """Renders the specific V1 HTML tile."""
    spec = item["spec"]
    name = spec["Name"]
    val = item["last_val"]
    
    # Improved visualization for predicted/modeled data
    if item.get("is_modeled"):
        val_str = "Est."
        
        # Use specific desaturated colors for estimates
        c_text = item["cond_text"]
        if "in shape" in c_text:
            bg = "#DAE8DC" # Grey-green
        elif "low" in c_text:
            bg = "#EFEAC5" # Grey-yellow
        else:
            bg = "#E6DBCE" # Grey-orange for high
            
        font_style = "font-style:italic;"
        
    elif item.get("is_proxy"):
        # Distinct style for PROXY data (Calculated from neighbor)
        val_str = f"Est. ({val:,.0f})" if val is not None else "Est."
        bg = "#E6DBCE" # Use Grey-orange to denote 'Attention needed'
        font_style = "font-style:italic;"
        
    else:
        val_str = f"{val:,.0f}" if val is not None else "ERR"
        bg = item["cond_color"]
        font_style = ""
    
    # Fix: Ensure unit is displayed
    gauge = item.get("gauge_used") or {}
    param = gauge.get("P", "")
    unit = "ft" if param == "00065" else "cfs"
    
    range_str = spec["T"]
    status_str = item["cond_text"].title()
    # bg is set above
    fg = coastal_get_tile_text_color_from_bg(bg)
    spark = item["spark"]
    trend = item["trend_text"]
    meta = f"{item['source']} • {item['time_str']}"
    insight = item["hydro_insight"]
    icon = item["icon"]
    
    style = f"background-color:{bg}; color:{fg}; {font_style} padding:6px 8px; border-radius:5px; margin-bottom:4px; font-size:0.80rem; line-height:1.35;"
    
    html = f"""
    <div style="{style}">
        <div style="display:flex; justify-content:flex-start; align-items:center;">
            <span style="font-weight:700; font-size:0.9rem; margin-right: 10px;">{name} {icon}</span>
            <span style="font-weight:700; font-size:0.9rem;">{val_str} <span style="font-size:0.7em; font-weight:normal;">{unit}</span></span>
        </div>
        <div style="opacity:0.9;">{range_str} • {status_str}</div>
        <div style="display:flex; justify-content:space-between; margin-top:2px;">
            <span>{spark} {trend}</span>
        </div>
        <div style="font-size:0.70rem; opacity:0.8; margin-top:4px;">
            {meta}<br>
            {insight}<br>
            <i>{spec.get('N', '')}</i>
        </div>
    </div>
    """
    st.markdown(html, unsafe_allow_html=True)

def render_filters():
    with st.sidebar:
        st.header("Filters")
        if st.button("🔄 Refresh Data"):
            get_dashboard_data.clear()
            st.rerun()
        st.divider()
        
        # Regions - Short names
        regions = list(load_coastal_region_specs().keys())
        sel_regions = []
        st.caption("Regions")
        for r in regions:
            # Shorten label for display
            label = r.replace("Oregon", "OR").replace("Washington", "WA").replace("Coast", "Cst")
            if st.checkbox(label, value=True, key=r):
                sel_regions.append(r)
                
        # Conditions
        st.caption("Conditions")
        # Add "no data" to default conditions so missing rivers show up
        conds = ["in shape", "low", "slightly high", "blown out", "too low", "no data", "likely low", "likely high", "likely in shape"]
        sel_conds = []
        for c in conds:
            if st.checkbox(c.title(), value=True, key=c):
                sel_conds.append(c)
                
    return sel_regions, sel_conds

def coastal_render_region_summary(coastal_data):
    st.subheader("Region Summary")
    regions = list(coastal_data.items())
    cols = st.columns(3)

    for i, (region_name, entries) in enumerate(regions):
        with cols[i % 3]:
            # Fix: Ensure logic handles all entries
            total = len(entries)
            # Count measured if source is not modeled
            measured = sum(1 for e in entries if not e.get("is_modeled", False))
            estimated = total - measured
            
            # Safe access to storm_cycle tuple
            rising = sum(1 for e in entries if "Rising" in e.get("storm_cycle", ("Unknown",))[0])
            peaking = sum(1 for e in entries if "Peak" in e.get("storm_cycle", ("Unknown",))[0])
            dropping = sum(1 for e in entries if any(x in e.get("storm_cycle", ("Unknown",))[0] for x in ["Drop", "Post"]))
            
            # Correct In Shape counting
            in_window = sum(1 for e in entries if e["cond_text"] in ["in shape", "likely in shape"])
            
            pct_window = (in_window / total * 100) if total > 0 else 0
            if pct_window >= 40:
                badge = "🔥 Hot"
                badge_bg = "#FEE2E2"
                badge_col = "#991B1B"
            elif pct_window >= 15:
                badge = "🟡 Mixed"
                badge_bg = "#FEF3C7"
                badge_col = "#92400E"
            elif sum(1 for e in entries if e["cond_text"] == "blown out") > (total * 0.4):
                badge = "🟥 Blown"
                badge_bg = "#FEE2E2"
                badge_col = "#B91C1C"
            else:
                badge = "❄️ Cold"
                badge_bg = "#EFF6FF"
                badge_col = "#1E40AF"

            tile_html = f"""
            <div style='background-color:#FFFFFF; padding:12px; border-radius:8px; margin-bottom:12px; border:1px solid #E5E7EB; box-shadow: 0 1px 2px rgba(0,0,0,0.05);'>
                <div style='display:flex; justify-content:space-between; align-items:center; margin-bottom:8px;'>
                    <span style='font-weight:700; font-size:1rem; color:#111827;'>{region_name}</span>
                    <span style='font-size:0.75rem; background-color:{badge_bg}; color:{badge_col}; padding:2px 8px; border-radius:12px; font-weight:600;'>{badge}</span>
                </div>
                <div style='font-size:0.8rem; color:#4B5563; line-height:1.5;'>
                    <div style='display:flex; justify-content:space-between;'>
                        <span>📡 <b>{measured}</b> Meas.</span>
                        <span>🧪 <b>{estimated}</b> Est.</span>
                    </div>
                    <div style='margin-top:4px; border-top:1px solid #F3F4F6; padding-top:4px;'>
                        <span title='Rising'>🌧️ {rising}</span> &nbsp; 
                        <span title='Peaking'>🌊 {peaking}</span> &nbsp; 
                        <span title='Dropping/Post'>📉 {dropping}</span>
                    </div>
                    <div style='margin-top:4px; font-weight:500; color:#059669;'>
                        🎯 {in_window} Rivers Fishable ({pct_window:.0f}%)
                    </div>
                </div>
            </div>
            """
            st.markdown(tile_html, unsafe_allow_html=True)

def render_top3(data):
    candidates = []
    for reg, items in data.items():
        for i in items:
            if i["cond_text"] == "in shape": candidates.append((10, i))
            elif i["cond_text"] == "low": candidates.append((5, i))
            
    candidates.sort(key=lambda x: x[0], reverse=True)
    top3 = [x[1] for x in candidates[:3]]
    
    if top3:
        st.subheader("🔥 Top 3 Right Now")
        cols = st.columns(3)
        for idx, item in enumerate(top3):
            with cols[idx]:
                coastal_tile(item)
        st.divider()

def render_coastal_dashboard():
    st.set_page_config(page_title="Coastal Dashboard", layout="wide")
    st.title("🌊 Coastal Conditions Dashboard")
    
    with st.spinner("Fetching Data (Async)..."):
        data = get_dashboard_data()
    
    sel_regions, sel_conds = render_filters()
    
    # ----------------------------------------------------
    # NEW: TABBED INTERFACE
    # ----------------------------------------------------
    tab1, tab2 = st.tabs(["📝 List View", "🗺️ Map View"])
    
    with tab1:
        coastal_render_region_summary(data)
        render_top3(data)
        
        st.subheader("📍 Regions")
        for region, entries in data.items():
            if region not in sel_regions: continue
            
            visible = [e for e in entries if e["cond_text"] in sel_conds]
            if not visible: continue
            
            with st.expander(region, expanded=True):
                cols = st.columns(3)
                for i, item in enumerate(visible):
                    with cols[i % 3]:
                        coastal_tile(item)
                        
    with tab2:
        # Pass data and filters to the map module
        filters_dict = {"regions": sel_regions, "status": sel_conds}
        render_coastal_map(data, filters_dict)

if __name__ == "__main__":
    render_coastal_dashboard()