from datetime import datetime
from utils.api_helpers import fetch_api_data

def extract_tsunamis(ti):
    url = "https://www.ngdc.noaa.gov/hazel/hazard-service/api/v1/tsunamis/events"
    params = {
        "causeCode": 1,
        "country": "Chile",
        "itemsPerPage": 200,
        "page": 1
    }
    data = fetch_api_data(url, params=params)
    ti.xcom_push(key="raw_data", value=data)

def transform_tsunamis(ti):
    raw_data = ti.xcom_pull(key="raw_data", task_ids="extract_tsunamis")
    if not raw_data or "items" not in raw_data:
        ti.xcom_push(key="transformed_data", value=[])
        return

    transformed_list = []
    for event in raw_data["items"]:
        # Asegurarse de que el evento tenga coordenadas
        lat = event.get("latitude")
        lon = event.get("longitude")
        if lat is None or lon is None:
            continue  # omitir eventos sin ubicación

        # Intentar construir una fecha legible
        year = event.get("year")
        month = event.get("month", 1)
        day = event.get("day", 1)
        try:
            date = datetime(year, month, day).isoformat()
        except:
            date = None

        transformed_list.append({
            "id": event.get("id"),
            "date": date,
            "location": event.get("locationName") or event.get("location"),
            "country": event.get("country"),
            "latitude": lat,
            "longitude": lon,
            "maxHeight": event.get("maxWaterHeight"),
            "deaths": event.get("deathsTotal") or event.get("deaths"),
            "damageMillionsDollars": event.get("damageMillionsDollars") or event.get("damageMillionsDollarsTotal"),
            "type": "tsunami"
        })

    ti.xcom_push(key="transformed_data", value=transformed_list)
