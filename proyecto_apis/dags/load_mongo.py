import pymongo
from datetime import datetime

def load_to_mongo(ti):
    # Conexión a MongoDB Atlas
    client = pymongo.MongoClient("mongodb+srv://diegoloria38:zTXN0YAqVR9zov9Q@bigdataubuntu.psjwglz.mongodb.net/")
    db = client["project_api"]
    collection = db["proyecto_de_apis"]

    # Obtener datos transformados desde XCom
    earthquakes = ti.xcom_pull(key="transformed_data", task_ids="transform_earthquakes")
    tsunamis = ti.xcom_pull(key="transformed_data", task_ids="transform_tsunamis")
    volcanoes = ti.xcom_pull(key="transformed_data", task_ids="transform_volcanoes")

    # Armar documento para insertar o reemplazar
    document = {
        "timestamp": datetime.utcnow().isoformat(),
        "earthquakes": earthquakes,
        "tsunamis": tsunamis,
        "volcanoes": volcanoes
    }

    # Reemplazar el documento con filtro fijo (type: "hazards_data")
    result = collection.replace_one(
        {"type": "hazards_data"},                # filtro para documento único
        {**document, "type": "hazards_data"},   # documento a insertar/reemplazar, agregando campo "type"
        upsert=True                             # inserta si no existe
    )

    # Guardar en XCom el id del documento insertado o nota si solo fue actualización
    inserted_id = result.upserted_id if result.upserted_id is not None else "existing_document_updated"
    ti.xcom_push(key="inserted_doc_id", value=str(inserted_id))

