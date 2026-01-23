import os
import time
import logging
from typing import Optional, Dict, Any, Generator, List
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv

load_dotenv()

config = {
    "auth_type": os.getenv("auth_type"),
    "consumerID": os.getenv("consumerID"),
    "consumerSecret": os.getenv("consumerSecret"),
    "url": os.getenv("url"),
    "stream_url": os.getenv("stream_url")
}

API_BASE = config.url.rstrip('/') + '/api/v2/Market'

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class FCDataClient:
    def __init__(self):
        self.consumer_id = config.consumerID
        self.consumer_secret = config.consumerSecret
        self.auth_type = config.auth_type
        self._session = requests.Session()
        self._token = None
        self.authenticate()
    
    def authenticate(self):
        url = f"{API_BASE}/AccessToken"
        payload = {"consumerID": self.consumer_id, "consumerSecret": self.consumer_secret}
        r = self._session.post(url, json=payload, timeout=15)
        r.raise_for_status()
        j = r.json()
        token = j.get("data", {}).get("accessToken")
        if not token:
            raise RuntimeError(f"Access token not found in response: {j}")
        self._token = token
        self._session.headers.update({"Authorization": f"Bearer {self._token}"})
        logger.info("Authenticated. Token obtained.")
        
    def _request(self, method: str, path: str, retry_on_401=True, **kwargs):
        url = f"{API_BASE}/{path}"
        r = self._session.request(method, url, timeout=20, **kwargs)
        if r.status_code == 401 and retry_on_401:
            # Token might be expired, re-authenticate and retry once
            logger.info("401 received, refreshing token and retrying...")
            self.authenticate()
            r = self._session.request(method, url, timeout=20, **kwargs)
        # handle 429 Retry-After (request retry handles some cases, but check explicitly)
        if r.status_code == 429:
            ra = r.headers.get("Retry-After")
            delay = int(ra) if ra and ra.isdigit() else 5
            logger.warning(f"429 received, refreshing after {delay} seconds...")
            time.sleep(delay)
            r = self._session.request(method, url, timeout=20, **kwargs)
        r.raise_for_status()
        return r.json()

    def get_securities(self, path: str, params: Dict[str, Any], page_field="PageIndex", size_field="PageSize", data_path="data", list_field="dataList") -> Generator[Dict, None, None]:
        page = 1
        params = params.copy()
        params[size_field] = params.get(size_field, 100)
        while True:
            params[page_field] = page
            resp = self._request("GET", path, params=params)
            # drill to data List
            data_obj = resp
            for key in data_path and list_field:
                if isinstance(data_obj, dict) and key in data_obj:
                    data_obj = data_obj[key]
                if not data_obj:
                    break
                if isinstance(data_obj, list):
                    for item in data_obj:
                        yield item
                else:
                    # try dataList inside
                    rows = data_obj.get("dataList") if isinstance(data_obj, dict) else []
                    if not rows:
                        break
                    for item in rows:
                        yield item
                if isinstance(data_obj, list) and len(data_obj) < params[size_field]:
                    break
                page += 1