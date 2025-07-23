from datetime import datetime
from utils.api_helpers import fetch_api_data
import math

def web_mercator_to_lonlat(x, y):
    """
    Convierte coordenadas Web Mercator (EPSG:3857) a longitud y latitud (EPSG:4326)
    """
    R = 6378137.0  # Radio de la tierra en metros (WGS84)
    lon = (x / R) * (180 / math.pi)
    lat = (math.pi / 2 - 2 * math.atan(math.exp(-y / R))) * (180 / math.pi)
    return lon, lat

def extract_volcanoes(ti):
    url = "https://gis.ngdc.noaa.gov/arcgis/rest/services/web_mercator/hazards/MapServer/6/query"
    params = {
        "where": "COUNTRY='Chile'",  # <-- QUITAMOS el filtro de años
        "outFields": "NAME,COUNTRY,YEAR,VEI,FATALITIES,DAMAGE_MILLIONS_DOLLARS,DEATHS",
        "f": "json"
    }
    data = fetch_api_data(url, params=params)
    ti.xcom_push(key="raw_data", value=data)

def transform_volcanoes(ti):
    raw_data = ti.xcom_pull(key="raw_data", task_ids="extract_volcanoes")
    if not raw_data or "features" not in raw_data:
        ti.xcom_push(key="transformed_data", value=[])
        return

    transformed_list = []

    for feature in raw_data["features"]:
        attrs = feature.get("attributes", {})
        geom = feature.get("geometry", {})

        def safe_int(value):
            try:
                return int(value)
            except (TypeError, ValueError):
                return None

        def safe_deaths(value):
            if value is None:
                return 0
            try:
                return int(value)
            except (TypeError, ValueError):
                return 0

        lon, lat = None, None
        if "x" in geom and "y" in geom:
            lon, lat = web_mercator_to_lonlat(geom["x"], geom["y"])

        transformed_list.append({
            "name": attrs.get("NAME"),
            "country": attrs.get("COUNTRY"),
            "year": safe_int(attrs.get("YEAR")),
            "vei": safe_int(attrs.get("VEI")),
            "fatalities": safe_deaths(attrs.get("FATALITIES")),
            "damage_millions_dollars": attrs.get("DAMAGE_MILLIONS_DOLLARS"),
            "deaths": safe_deaths(attrs.get("DEATHS")),
            "longitude": lon,
            "latitude": lat
        })

    ti.xcom_push(key="transformed_data", value=transformed_list)

