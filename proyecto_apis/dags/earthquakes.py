from datetime import datetime
from utils.api_helpers import fetch_api_data

def extract_earthquakes(ti):
    url = "https://earthquake.usgs.gov/fdsnws/event/1/query"
    params = {
        "format": "geojson",
        "starttime": "2010-01-01",
        "endtime": "2020-12-31",
        "minmagnitude": 6,
        "minlatitude": -56,
        "maxlatitude": -17,
        "minlongitude": -75,
        "maxlongitude": -66
    }
    data = fetch_api_data(url, params=params)
    ti.xcom_push(key="raw_data", value=data)

def transform_earthquakes(ti):
    raw_data = ti.xcom_pull(key="raw_data", task_ids="extract_earthquakes")
    if not raw_data or "features" not in raw_data:
        ti.xcom_push(key="transformed_data", value=[])
        return

    transformed_list = []
    for feature in raw_data["features"]:
        props = feature.get("properties", {})
        geometry = feature.get("geometry", {})
        transformed_list.append({
            "id": feature.get("id"),
            "place": props.get("place"),
            "time": datetime.utcfromtimestamp(props.get("time", 0)/1000).isoformat() if props.get("time") else None,
            "magnitude": props.get("mag"),
            "longitude": geometry.get("coordinates", [None, None])[0],
            "latitude": geometry.get("coordinates", [None, None])[1],
            "depth": geometry.get("coordinates", [None, None, None])[2] if len(geometry.get("coordinates", [])) > 2 else None,
            "tsunami": props.get("tsunami", 0)
        })

    ti.xcom_push(key="transformed_data", value=transformed_list)
