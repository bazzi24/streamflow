"""Unit tests for SQLAlchemy URL reconstruction from JDBC config values."""

import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "api_service"))

from src.config import _build_sqlalchemy_url


def test_build_sqlalchemy_url_preserves_path_and_query():
    jdbc_url = "jdbc:mysql://mysql:3306/data?charset=utf8mb4"
    result = _build_sqlalchemy_url(jdbc_url, "streamflow_app", "secret123")
    assert result == "mysql+pymysql://streamflow_app:secret123@mysql:3306/data?charset=utf8mb4"


def test_build_sqlalchemy_url_replaces_embedded_credentials():
    jdbc_url = "jdbc:mysql://root:oldpass@mysql:3306/warehouse?charset=utf8mb4"
    result = _build_sqlalchemy_url(jdbc_url, "app_user", "newpass")
    assert result == "mysql+pymysql://app_user:newpass@mysql:3306/warehouse?charset=utf8mb4"


def test_build_sqlalchemy_url_encodes_special_characters():
    jdbc_url = "jdbc:mysql://mysql:3306/api?charset=utf8mb4"
    result = _build_sqlalchemy_url(jdbc_url, "user@name", "pa:ss/w?rd")
    assert result == "mysql+pymysql://user%40name:pa%3Ass%2Fw%3Frd@mysql:3306/api?charset=utf8mb4"
