import os 
import copy
import logging
import importlib
from common.constants import SERVICE_CONF


def conf_realpath(conf_name):
    conf_path = f"conf/{conf_name}"

def read_config(conf_name=SERVICE_CONF):
    local_config = {}
    local_path = conf_realpath(f"local.{conf_name}")
    
    
    

CONFIGS = read_config

def get_base_config(key, default=None):
    if key is None:
        return None
    if default is None:
        default = os.environ.get(key.upper())
    return CONFIGS.get(key, default)

def decrypt_database_password(password):
    encrypt_password = get_base_config("encrypt_password", False)
    encrypt_module = get_base_config("encrypt_module", False)
    private_key = get_base_config("private_key", None)

    if not password or not encrypt_password:
        return password

    if not private_key:
        raise ValueError("No private key")

    module_fun = encrypt_module.split("#")
    pwdecrypt_fun = getattr(
        importlib.import_module(
            module_fun[0]),
        module_fun[1])

    return pwdecrypt_fun(private_key, password)


def decrypt_database_config(database=None, passwd_key="password", name="database"):
    if not database:
        database = get_base_config(name, {})
        
    database[passwd_key] = decrypt_database_password(database[passwd_key])
    return database
        
    