import requests
from datetime import datetime

def fetch_api_data(url, params=None, headers=None):
    try:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()  # Lanza error si status no es 200
        return response.json()
    except Exception as e:
        return {
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }
