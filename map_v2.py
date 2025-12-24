import streamlit as st
import pydeck as pdk
import pandas as pd

def render_coastal_map(data_dict, filters):
    """
    Renders map using the unified data structure from dashboard_v2.
    """
    all_rows = []
    
    # Diagnostics counters
    total_rivers = 0
    skipped_region = 0
    skipped_status = 0
    
    # Track which statuses are being skipped to help debug
    skipped_status_types = {}
    
    # Flatten the region dictionary into a list for the map
    for region, entries in data_dict.items():
        total_rivers += len(entries)
        
        # Respect Region Filters
        # Note: filters["regions"] comes from dashboard_v2 sidebar
        if region not in filters["regions"]:
            skipped_region += len(entries)
            continue
            
        for e in entries:
            # Respect Status Filters
            # e["cond_text"] is the status in dashboard_v2 (e.g., "in shape")
            status_raw = e.get("cond_text", "no data")
            
            # Check if this status is selected in filters
            if status_raw not in filters["status"]:
                skipped_status += 1
                skipped_status_types[status_raw] = skipped_status_types.get(status_raw, 0) + 1
                continue
                
            spec = e["spec"]
            
            # Skip if no coordinates (shouldn't happen with updated specs)
            if "lat" not in spec or "lon" not in spec:
                continue

            # ------------------------------------------------
            # COLOR LOGIC (Mapped to dashboard_v2 statuses)
            # ------------------------------------------------
            stat = status_raw.lower()
            
            # RGBA Colors
            if "in shape" in stat: 
                color = [0, 255, 0, 200]       # Green
                radius = 6000
                display_status = "Prime / In Shape"
            elif "slightly high" in stat or "likely high" in stat: 
                color = [220, 220, 0, 200]     # Yellow/Gold
                radius = 4500
                display_status = "Slightly High"
            elif "low" in stat: 
                color = [255, 140, 0, 200]     # Orange
                radius = 4500
                display_status = "Low"
            elif "blown out" in stat: 
                color = [200, 0, 0, 200]       # Red
                radius = 3000
                display_status = "Blown Out"
            elif "too low" in stat: 
                color = [50, 50, 200, 150]     # Blue
                radius = 2000
                display_status = "Too Low"
            else: 
                color = [150, 150, 150, 150]   # Grey
                radius = 2000
                display_status = status_raw.title()
            
            # ------------------------------------------------
            # TOOLTIP CONTENT
            # ------------------------------------------------
            trend_arrow = "➡️"
            trend_txt = e.get("trend_text", "")
            if "↑" in trend_txt: trend_arrow = "↗️"
            if "↓" in trend_txt: trend_arrow = "↘️"
            
            # Formatting the Value
            val = e.get('last_val')
            if e.get("is_modeled"):
                val_str = "Est."
            else:
                val_str = f"{val:.0f}" if val is not None else "?"
            
            # Unit lookup
            unit = "cfs"
            if "ft" in spec.get("T", ""): unit = "ft"

            tooltip_html = f"""
                <div style='font-family: sans-serif; padding: 4px;'>
                    <b style='font-size: 1.1em;'>{spec['Name']}</b><br/>
                    <span style='color: #ccc;'>{region}</span><hr style='margin: 4px 0; border-color: #555;'/>
                    <b>Flow:</b> {val_str} {unit}<br/>
                    <b>Status:</b> {display_status}<br/>
                    <b>Trend:</b> {trend_txt} {trend_arrow}<br/>
                    <i style='font-size: 0.9em; color: #aaa;'>Ideal: {spec['T']}</i>
                </div>
            """
            
            all_rows.append({
                "name": spec["Name"],
                "lat": spec["lat"],
                "lon": spec["lon"],
                "color": color,
                "radius": radius,
                "tooltip": tooltip_html
            })

    # ------------------------------------------------
    # EMPTY STATE HANDLING
    # ------------------------------------------------
    if not all_rows:
        if total_rivers == 0:
            st.error("⚠️ No data loaded.")
        else:
            st.info(f"ℹ️ No rivers match the current filters.")
            
        return

    df = pd.DataFrame(all_rows)

    # ------------------------------------------------
    # PYDECK LAYER CONFIG
    # ------------------------------------------------
    layer = pdk.Layer(
        "ScatterplotLayer",
        data=df,
        get_position=["lon", "lat"],
        get_color="color",
        get_radius="radius",
        pickable=True,
        opacity=0.8,
        stroked=True,
        filled=True,
        radius_scale=1,
        radius_min_pixels=5,
        radius_max_pixels=25,
        get_line_color=[0, 0, 0],
        get_line_width=100,
    )

    # Center map based on data
    if not df.empty:
        lat_center = df["lat"].mean()
        lon_center = df["lon"].mean()
        zoom = 5.5
    else:
        lat_center = 45.0
        lon_center = -123.0
        zoom = 5

    view_state = pdk.ViewState(
        latitude=lat_center,
        longitude=lon_center,
        zoom=zoom,
        pitch=0,
    )

    # ------------------------------------------------
    # RENDER DECK
    # ------------------------------------------------
    st.pydeck_chart(pdk.Deck(
        map_style=pdk.map_styles.CARTO_LIGHT,
        initial_view_state=view_state,
        layers=[layer],
        tooltip={
            "html": "{tooltip}",
            "style": {
                "backgroundColor": "#1e1e1e",
                "color": "#ffffff",
                "borderRadius": "5px",
                "border": "1px solid #333",
                "zIndex": "1000"
            }
        }
    ))