from datetime import date
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
import os

load_dotenv()

mongodb_uri = os.getenv("MONGODB_URI")

uri = mongodb_uri

# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))

# Database creation
db = client["option_chain_data"]

# Create collection
today = date.today().strftime("%d-%m-%Y")
collection = db[f"oc_data_{today}"]