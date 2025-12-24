import streamlit as st

st.set_page_config(layout="wide", page_title="Expedition App V2")

# Import Modules
# Using try-except blocks to allow safe fallbacks if files are missing during dev
try:
    from dashboard_v2 import render_coastal_dashboard
except ImportError as e:
    st.error(f"Error importing dashboard_v2: {e}")
    def render_coastal_dashboard(): st.warning("Dashboard V2 unavailable")

try:
    from planner_v2 import render_planner
except ImportError as e:
    st.error(f"Error importing planner_v2: {e}")
    def render_planner(): st.warning("Planner V2 unavailable")

# --- NAVIGATION ---
if "page" not in st.session_state:
    st.session_state.page = "home"

def render_home():
    st.title("🧭 Expedition App v2.0")
    st.caption("Enhanced with Async I/O & NOAA Forecasting")
    st.markdown("---")
    
    c1, c2 = st.columns(2)
    with c1:
        st.info("### 🌊 Coastal Dashboard\nView real-time USGS flows and NOAA forecasts.")
        if st.button("Open Dashboard"):
            st.session_state.page = "dashboard"
            st.rerun()
            
    with c2:
        st.success("### 🗺️ Trip Planner\nPlan expeditions based on predictive hydrology.")
        if st.button("Open Planner"):
            st.session_state.page = "planner"
            st.rerun()

# --- MAIN ROUTING ---
if st.session_state.page == "home":
    render_home()
elif st.session_state.page == "dashboard":
    if st.button("← Home"):
        st.session_state.page = "home"
        st.rerun()
    render_coastal_dashboard()
elif st.session_state.page == "planner":
    if st.button("← Home"):
        st.session_state.page = "home"
        st.rerun()
    render_planner()