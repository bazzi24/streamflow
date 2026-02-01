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
from peewee import *

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
           
def is_continuouse_field(cls: typing.Type) -> bool:
    if cls in CONTINUOUS_FIELD_TYPE:
        return True
    for p in cls.__base__:
        if p in CONTINUOUS_FIELD_TYPE:
            return True
        elif p is not Field and p is not object:
            if is_continuouse_field(p):
                return True
    else:
        return False

def auto_date_timestamp_field():
    return {f"f_{f}_time" for f in AUTO_DATE_TIMESTAMP_FIELD_PREFIX}

def auto_date_timestamp_db_field():
    return {f"f_{f}_time" for f in AUTO_DATE_TIMESTAMP_FIELD_PREFIX}

def remove_field_name_prefix(field_name):
    return field_name[2:] if field_name.startswith("f_") else field_name 

class BaseModel(Model):
    create_time = BigIntegerField(null=True, index=True)
    create_date = DateTimeField(null=True, index=True)
    update_time = BigIntegerField(null=True, index=True)
    update_date = DateTimeField(null=True, index=True)
    
    def to_dict(self):
        return self.__dict__["__data__"]
    
    def to_json(self):
        return self.to_dict()
    
    def to_human_model_dict(self, only_primary_with: list = None):
        model_dict = self.__dict__["__data__"]
        if not only_primary_with:
            return {remove_field_name_prefix(k): v for k, v in model_dict.items()}
        
        human_model_dict = {}
        for k in self.__meta.primary_key.field_names:
            human_model_dict[remove_field_name_prefix(k)] = model_dict[k]
        for k in only_primary_with:
            human_model_dict[k] = model_dict[f"f_{k}"]
        return human_model_dict
    
    @property
    def meta(self) -> Metadata:
        return self._meta 
    
    @classmethod
    def get_primary_keys_name(cls):
        return cls._meta.primary_key.field_names if isinstance(cls._meta.primary_key, CompositeKey) else [cls._meta.primary_key.name]
    
    @classmethod
    def getter_by(cls, attr):
        return operator.attrgetter(attr)(cls)
    
    @classmethod
    def query(cls, reverse=None, order_by=None, **kwargs):
        filter = []
        for f_n, f_v in kwargs.items():
            attr_name = "%s" % f_n
            if not hasattr(cls, attr_name) or f_v is None:
                continue
            if type(f_v) in {list, set}:
                f_v = list(f_v)
                if is_continuouse_field(type(getattr(cls, attr_name))):
                    if len(f_v) == 2:
                        for i, v in enumerate(f_v):
                            if isinstance(v, str) and f_n in auto_date_timestamp_field():
                                f_v[i] = date_string_to_timestamp(v)
                        lt_value = f_v[0]
                        gt_value = f_v[1]
                        if lt_value is not None and gt_value is not None:
                            filter.append(cls.getter_by(attr_name).between(lt_value, gt_value))
                        elif lt_value is not None:
                            filter.append(operator.attrgetter(attr_name)(cls) >= lt_value)
                        elif gt_value is not None:
                            filter.append(operator.attrgetter(attr_name)(cls) <= gt_value)
                else:
                    filter.append(operator.attrgetter(attr_name)(cls) << f_v)
            else:
                filter.append(operator.attrgetter(attr_name)(cls) == f_v)
        if filter:
            query_records = cls.select().where(*filter)
            if reverse is not None:
                if not order_by or not hasattr(cls, f"{order_by}"):
                    order_by = "create_time"
                if reverse is True:
                    query_records = query_records.order_by(cls.getter_by(f"{order_by}").desc())
                elif reverse is False:
                    query_records = query_records.order_by(cls.getter_by(f"{order_by}").asc())
            return [query_records for query_record in query_records]
        else:
            return []    
        
    @classmethod
    def insert(cls, __data=None, **insert):
        if isinstance(__data, dict) and __data:
            __data[cls._meta.combined["create_time"]] = current_timestamp()
        if insert:
            insert["create_time"] = current_timestamp()
        return super().insert(__data, **insert)
    
    @classmethod
    def _normalize_data(cls, data, kwargs):
        normalized = super()._normalize_data(data, kwargs)
        if not normalized:
            return {}
        normalized[cls._meta.combined["update_time"]] = current_timestamp()
        
        for f_n in AUTO_DATE_TIMESTAMP_FIELD_PREFIX:
            if {f"{f_n}_time", f"{f_n}_date"}.issubset(cls._meta.combined.keys()) and cls._meta.combined[f"{f_n}_time"] in normalized[cls._meta.combined[f"{f_n}_time"]] is not None:
                normalized[cls._meta.combined[f"{f_n}_date"]] = timestamp_to_date(normalized[cls._meta.combined[f"{f_n}_time"]])
                
        return normalized
    
class JsonSerializedField(SerializedField):
    def __init__(self, object_hook=utils.from_dict_hook, object_pairs_hook=None, **kwargs):
        super(JsonSerializedField, self).__init__(serialized_type=SerializedType.JSON, object_hook=object_hook, object_pairs_hook=object_pairs_hook, **kwargs)
        
class RetryingPoolMySQLDatabase(PooledMySQLDatabase):
    def __init__(self, *args, **kwargs):
        self.max_retries = kwargs.pop("max_retries", 5)
        self.retry_delay = kwargs.pop("retry_delay", 1)
        super().__init__(*args, **kwargs)
        
    def execute_sql(self, sql, params=None, commit=True):
        for attempt in range(self.max_retries + 1):
            try:
                return super().execute_sql(sql, params, commit)
            except (OperationalError, IndentationError) as e:
                error_codes = [2013, 2006]
                error_message = ['', 'Lost connection']
                should_retry = (
                    (hasattr(e, 'args') and e.args and e.args[0] in error_codes) or
                    (str(e) in error_message) or
                    (hasattr(e, '__class__') and e.__class__.__name__ == 'InterfaceError')
                )
                
                if should_retry and attempt < self.max_retries:
                    logging.warning(
                        f"Database connection issue (attempt {attempt+1}/{self.max_retries}): {e}"
                    )
                    self._handle_connection_loss()
                    time.sleep(self.retry_delay * (2 ** attempt))
                else:
                    logging.error(f"Database execution failure: {e}")
                    raise
        return None
    
    def _handle_connection_loss(self):
        try:
            self.close()
        except Exception:
            pass
        try:
            self.connect()
        except Exception as e:
            logging.error(f"Failed to reconnect: {e}")
            time.sleep(0.2)
            try:
                self.connect()
            except Exception as e2:
                logging.error(f"Fail to reconnect on second attempt: {e2}")
                raise
        
    def begin(self):
        for attempt in range(self.max_retries + 1):
            try: 
                return super().begin()
            except (OperationalError, InterfaceError) as e:
                error_codes = [2013, 2006]
                error_messages = ['', "Lost connection"]
                should_retry = (
                    (hasattr(e, 'args') and e.args and e.args[0] in error_codes) or
                    (str(e) in error_messages) or
                    (hasattr(e, '__class__') and e.__class__.__name__ == 'InterfaceError')
                )

                if should_retry and attempt < self.max_retries:
                    logging.warning(
                        f"Lost connection during transaction (attempt {attempt+1}/{self.max_retries})"
                    )
                    self._handle_connection_loss()
                    time.sleep(self.retry_delay * (2 ** attempt))
                else:
                    raise
        return None
    
    
class PooledDatabase(Enum):
    MYSQL = RetryingPoolMySQLDatabase
    
@singleton
class BaseDatabase:
    def __init__(self):
        database_config = setting.DATABASE.copy()
        db_name = database_config.pop("name")
        
        pool_config = {
            'max_retries': 5,
            'retry_delay': 1
        }
        
        database_config.update(pool_config)
        self.database_connection = PooledDatabase[setting.DATABASE_TYPE.upper()].value(
            db_name, **database_config
        )
        
        logging.info("init database on cluster mode successfully")
        
def with_retry(max_retries=3, retry_delay=1.0):
    """Decorator: Add retry mechanism to database operations

    Args:
        max_retries (int): maximum number of retries
        retry_delay (float): initial retry delay (seconds), will increase exponentially

    Returns:
        decorated function
    """
    
    def decorator(func):
        @wraps
        def wrapper(*args, **kwargs):
            for retry in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    self_obj = args[0] if args else None
                    func_name = func.__name__
                    lock_name = getattr(self_obj, "lock_name", "unknown") if self_obj else "unknown"
                    
                    if retry < max_retries - 1:
                        current_delay = retry_delay * (2**retry)
                        logging.warning(f"{func_name} {lock_name} failed: {str(e)}, retrying ({retry + 1}/{max_retries})")
                        time.sleep(current_delay)
                    else:
                        logging.error(f"{func_name} {lock_name} failed after all attempt: {str(e)}")
                        
            if last_exception:
                raise last_exception
            return False
        return wrapper
    return decorator


class MySQLDatabaseLock:
    def __init__(self, lock_name, timeout=10, db=None):
        self.lock_name = lock_name
        self.timeout = int(timeout)
        self.db = db if db else DB
    
    @with_retry(max_retries=3, retry_delay=1.0)
    def lock(self):
        cursor = self.db.execute_sql("SELECT GET_LOCK(%s, %s)", (self.lock_name), (self.timeout))
        ret = cursor.fetchone()
        if ret[0] == 0:
            raise Exception(f"Acquire mysql lock {self.lock_name} timeout")
        elif ret[0] == 1:
            return True
        else:
            raise Exception(f"failed to acquire lock {self.lock_name}")
        
    @with_retry
    def unlock(self):
        cursor = self.db.execute_sql("SELECT RELEASE_LOCK(%s)", (self.lock_name))
        ret = cursor.fetchone()
        if ret[0] == 0:
            raise Exception(f"MySQL lock {self.lock_name} was not established by this thread")
        elif ret[0] == 1:
            return True
        else:
            raise Exception(f"MySQL lock {self.lock_name} does not exists")
        
    def __enter__(self):
        if isinstance(self.db, PooledMySQLDatabase):
            self.lock()
        return self
    
    def __exit__(self, exec_type, exec_val, exec_tb):
        if isinstance(self.db, PooledMySQLDatabase):
            self.unlock()
            
    def __call__(self, func):
        @wraps(func)
        def magic(*args, **kwargs):
            with self:
                return func(*args, **kwargs)
        return magic
        
class DatabaseLock(Enum):
    MYSQL = MySQLDatabaseLock
        
        
DB = BaseDatabase().database_connection
DB.lock = DatabaseLock[setting.DATABASE_TYPE.upper()].value

def close_connection():
    try:
        if DB:
            DB.close_stale(age=30)
    except Exception as e:
        logging.exception(e)
        
class DatabaseModel(BaseDatabase):
    class Meta:
        database = DB
        

@DB.connection_context()
@DB.lock("init_database_tables", 60)
def init_database_tables(alter_fields=[]):
    members = inspect.getmembers(sys.modules[__name__], inspect.isclass)
    table_objs = []
    create_failed_list = []
    for name, obj in members:
        if obj != DatabaseModel and issubclass(obj, DatabaseModel):
            table_objs.append(obj)

            if not obj.table_exists():
                logging.debug(f"start create table {obj.__name__}")
                try:
                    obj.create_table(safe=True)
                    logging.debug(f"create table success: {obj.__name__}")
                except Exception as e:
                    logging.exception(e)
                    create_failed_list.append(obj.__name__)
            else:
                logging.debug(f"table {obj.__name__} already exists, skip creation.")

    if create_failed_list:
        logging.error(f"create tables failed: {create_failed_list}")
        raise Exception(f"create tables failed: {create_failed_list}")
    migrate_db()
    
class User(DatabaseModel, AuthUser):
    idUser = CharField(max_length=32, primary_key=True)
    nickname = CharField(max_length=100, null=False, help_text="nicky name", index=True)
    password = CharField(max_length=255, null=False, help_text="password", index=True)
    email = CharField(max_length=255, null=False, help_text="email", index=True)
    last_login_time = DateTimeField(null=True, index=True)
    isAuthenticated = CharField(max_length=1, null=False, default="1", index=True)
    isActive = CharField(max_length=1, null=False, default="1", index=True)
    isSuperuser = BooleanField(null=True, help_text="is root", default=False, index=True)
    
    def __str__(self):
        return self.email
    
    class Meta:
        db_table = "user"
        
class Market(DatabaseModel):
    idMarket = AutoField(primary_key=True)
    marketName = CharField(max_length=255, null=False, help_text="market name", index=True)
    
    class Meta:
        db_table = "market"
        
class Sector(DatabaseModel):
    idSector = AutoField(primary_key = True)
    sectorName = CharField(max_length=255, null=False, help_text="sector name", index=True)
    
    class Meta:
        db_table = "sector"
        
class Corporation(DatabaseModel):
    idSymbol = CharField(max_length=32, null=False, index=True)
    
    idSector = ForeignKeyField(
        Sector,
        backref="corporation",
        on_delete="CASCADE",
        related_name="idSector"
    )
    
    idMarket = ForeignKeyField(
        Market,
        backref="corporation",
        related_name="idMarket",
    )
    
    symbolName = CharField(max_length=255, null=False, help_text="symbol name", index=True)
    symbolEnName = CharField(max_length=255, null=True, help_text="symbol english name", index=True)

    class Meta:
        db_table = "corporation"
        
class CorporationDetail(DatabaseModel):
    idCorpDetail = AutoField(primary_key=True)
    idSymbol = CharField(max_length=32, null=False, help_text="id Symbol", index=True)
    address = CharField(max_length=255, null=True, help_text="addres corporation", index=True)
    webURL = TextField(null=True, help_text="URL Web Corp", index=True)
    stockType = CharField(max_length=100, null=True, help_text="Type stock", index=True)
    listingDate = DateField(null=True, index=True)
    delistingDate = DateField(null=True, index=True)
    
    class Meta:
        db_table = "corporationDetail"
        
class CEO(DatabaseModel):
    idCEO = AutoField(primary_key=True)
    
    idSymbol = ForeignKeyField(
        Corporation,
        backref="idSYmbol",
        related_name="idSymbol"
    )
    
    ceoName = CharField(max_length=255, null=False, help_text="Name CEO", index=True)
    ceoVolume = DoubleField(default=0, index=False)
    ceoPercent = DoubleField(default=0, index=False)
    
    class Meta:
        db_table = "ceo"
        
class IndexList(DatabaseModel):
    idINdexList = AutoField(primary_key=True)
    
    idMarket = ForeignKeyField(
        Market,
        backref="idMarket",
        related_name="idSymbol",
    )
    
    indexName = CharField(max_length=100, null=False, help_text="index name", index=True)
    indexEnName = CharField(max_length=100, null=False, help_text="index english name", index=True)    

    class Meta:
        db_table = "indexlist"
        

    

        
def migrate_db():
    logging.disable(logging.ERROR)
                    
                    
                