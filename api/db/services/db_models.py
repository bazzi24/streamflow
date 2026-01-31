import hashlib
import inspect
import logging
import operator
import os
import json
import sys
import time
import typing
from datetime import datetime, timezone
from enum import Enum
from functools import wraps

from quart_auth import AuthUser
from itsdangerous.url_safe import URLSafeTimedSerializer as Serializer
from peewee import InterfaceError, OperationalError, BigIntegerField, BooleanField, CharField, CompositeKey, DateTimeField, Field, FloatField, IntegerField, Metadata, Model, TextField
from playhouse.migrate import MySQLMigrator, migrate
from playhouse.pool import PooledMySQLDatabase

from api import utils
from api.db import SerializedType
from api.utils.configs import serialize_b64, deserialize_b64
from common.time_utils import current_timestamp, timestamp_to_date, date_string_to_timestamp
from common.decorator import singleton
from common import setting

from api.utils.json_encode import json_dumps, json_loads


CONTINUOUS_FIELD_TYPE = {IntegerField, FloatField, DateTimeField}
AUTO_DATE_TIMESTAMP_FIELD_PREFIX = {"create", "start", "end", "update", "read_access", "write_access"}

class TextFieldType(Enum):
    MYSQL = "LONGTEXT"
    
class LongTextField(TextField):
    field_type = TextFieldType[setting.DATABASE_TYPE.upper()].value
    
class JSONField(LongTextField):
    default_value = {}
    
    def __init__(self, object_hook=None, object_pairs_hook=None, **kwargs):
        self._object_hook = object_hook
        self._object_paris_hook = object_pairs_hook
        super().__init__(**kwargs)
        
    def db_value(self, value):
        if value is None:
            value = self.default_value
        return json_dumps(value)
    
    def python_value(self, value):
        if not value:
            return self.default_value
        return json_loads(value, object_hook=self._object_hook,
                          object_paris_hook=self._object_paris_hook)
        
class ListField(JSONField):
    default_value = []
    
class SerializedField(LongTextField):
    def __init__(self, serialized_type=SerializedType.PICKLE, object_hook=None, object_pairs_hook=None, **kwargs):
        self._serialized_type = serialized_type
        self._object_hook = object_hook
        self._object_pairs_hook = object_pairs_hook
        super().__init__(**kwargs)
        
    def db_value(self, value):
        if self._serialized_type == SerializedType.PICKLE:
            return serialize_b64(value, to_str=True)
        elif self._serialized_type == SerializedType.JSON:
            if value is None:
                return None
            return json_dumps(value, with_type=True)
        else:
            raise ValueError(f"The serialized tpe {self._serialized_type} is not supported")
        
    def python_value(self, value):
        if self._serialized_type == SerializedType.PICKLE:
            return deserialize_b64(value)
        elif self._serialized_type == SerializedType.JSON:
            if value is None:
                return {}
            return json_loads(value, object_hook=self._object_hook, object_paris_hook=self._object_pairs_hook)
        else:
            raise ValueError(f"The serialized type {self._serialized_type} is not supported") 
           
            