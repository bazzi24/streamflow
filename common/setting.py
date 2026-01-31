import os 
import json
import secrets
from datetime import date
import logging
from common.config_utils import decrypt_database_config

DATABASE_TYPE = os.getenv("DB_TYPE", "mysql")
DATABASE = decrypt_database_config(name=DATABASE_TYPE)