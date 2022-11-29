import enum
import json
import os

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# read db_config from env variables
DB_CONFIG = {
   "user": os.getenv("DB_USER"),
   "password": os.getenv("DB_PASSWORD"),
   "host": os.getenv("DB_HOST"),
   "port": os.getenv("DB_PORT"),
   "database": os.getenv("DB_NAME"),
   "table": os.getenv("DB_TABLE")
}
