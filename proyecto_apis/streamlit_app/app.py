import streamlit as st
from pymongo import MongoClient
import pandas as pd
import plotly.express as px
from datetime import datetime

# 🎨 Tema tectónico-volcánico
def set_tectonic_theme():
    st.set_page_config(page_title="Chile Geo Hazards Dashboard", page_icon="🌋", layout="wide")
    st.markdown("""
    <style>
    .main {
        background-color: #0d0d0d;
        color: #ff7043;
    }
    h1, h2, h3, label, div, p, span {
        color: #ff7043 !important;
    }
    .stButton>button {
        background-color: #b71c1c;
        color: white;
    }
    .css-1d391kg { background-color: #1a1a1a; }
    </style>
    """, unsafe_allow_html=True)

set_tectonic_theme()

# 🔌 MongoDB
mongo_uri = "mongodb+srv://diegoloria38:zTXN0YAqVR9zov9Q@bigdataubuntu.psjwglz.mongodb.net/"
client = MongoClient(mongo_uri)
db = client["project_api"]
collection = db["proyecto_de_apis"]

st.title("🌋 Mapa de Peligros Naturales en Chile (2010–2020)")

# 📦 Obtener documento
data_doc = collection.find_one()

# 🌍 TERREMOTOS
df_eq = pd.DataFrame(data_doc.get("earthquakes", [])) if data_doc else pd.DataFrame()
if not df_eq.empty and {"latitude", "longitude"}.issubset(df_eq.columns):
    df_eq["lat"] = df_eq["latitude"]
    df_eq["lon"] = df_eq["longitude"]
    df_eq["type"] = "Terremoto"
    df_eq["color"] = "red"
    df_eq["valor"] = (
        "Magnitud: " + df_eq["magnitude"].astype(str) +
        " | Profundidad: " + df_eq["depth"].astype(str) + " km"
    )
    df_eq = df_eq[["lat", "lon", "type", "color", "valor"]]
else:
    df_eq = pd.DataFrame(columns=["lat", "lon", "type", "color", "valor"])

# 🌊 TSUNAMIS
df_ts = pd.DataFrame(data_doc.get("tsunamis", [])) if data_doc else pd.DataFrame()
if not df_ts.empty and {"latitude", "longitude"}.issubset(df_ts.columns):
    df_ts["lat"] = df_ts["latitude"]
    df_ts["lon"] = df_ts["longitude"]
    df_ts["type"] = "Tsunami"
    df_ts["color"] = "blue"
    df_ts["valor"] = (
        "Muertes: " + df_ts["deaths"].astype(str) +
        " | Lugar: " + df_ts["location"].astype(str)
    )
    df_ts = df_ts[["lat", "lon", "type", "color", "valor"]]
else:
    df_ts = pd.DataFrame(columns=["lat", "lon", "type", "color", "valor"])

# 🌋 VOLCANES (desde el mismo documento)
volc_data = data_doc.get("volcanoes", []) if data_doc else []
df_volc = pd.DataFrame(volc_data)

if not df_volc.empty and {"latitude", "longitude", "name", "year"}.issubset(df_volc.columns):
    df_volc = df_volc[df_volc["latitude"].notna() & df_volc["longitude"].notna()]
    df_volc["lat"] = df_volc["latitude"].astype(float)
    df_volc["lon"] = df_volc["longitude"].astype(float)
    df_volc["type"] = "Volcán"
    df_volc["color"] = "orange"
    df_volc["valor"] = (
        df_volc["name"].astype(str) +
        " (" + df_volc["year"].astype(str) + ")" +
        " | VEI: " + df_volc["vei"].astype(str) +
        " | Muertes: " + df_volc["fatalities"].fillna(0).astype(str)
    )
    df_volc = df_volc[["lat", "lon", "type", "color", "valor"]]
else:
    df_volc = pd.DataFrame(columns=["lat", "lon", "type", "color", "valor"])

# 🔗 Combinar todo
df_all = pd.concat([df_eq, df_ts, df_volc], ignore_index=True)

# 🗺️ Mostrar mapa
if not df_all.empty:
    fig = px.scatter_mapbox(
        df_all,
        lat="lat",
        lon="lon",
        color="type",
        hover_name="valor",
        color_discrete_map={
            "Terremoto": "red",
            "Tsunami": "blue",
            "Volcán": "orange"
        },
        zoom=4,
        mapbox_style="carto-darkmatter",
        title="Eventos Naturales en Chile"
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    st.warning("⚠️ No hay datos disponibles para mostrar en el mapa.")

# 📊 Mostrar tabla interactiva si quieres
with st.expander("📄 Ver tabla detallada de eventos"):
    st.dataframe(df_all, use_container_width=True)

# 📅 Footer
st.markdown("---")
st.markdown(f"<div style='text-align: center; color: #ff7043; font-size: 0.85em;'>Última actualización: {datetime.now().strftime('%Y-%m-%d')}</div>", unsafe_allow_html=True)
