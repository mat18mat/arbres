from pymongo import MongoClient

MONGO_HOST = "localhost"
MONGO_PORT = 27017
MONGO_USER = "root"
MONGO_PASSWORD = "rootpassword"
MONGO_DB = "arbres_db"
MONGO_COLLECTION = "arbres"


def get_collection():
    uri = (
        f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}"
        f"@{MONGO_HOST}:{MONGO_PORT}/"
        f"?authSource=admin"
    )
    client = MongoClient(uri)
    db = client[MONGO_DB]
    return db[MONGO_COLLECTION]
