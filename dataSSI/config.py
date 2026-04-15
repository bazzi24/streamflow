import os
from dotenv import load_dotenv

load_dotenv()


class _Config:
    def __init__(self):
        self.url            = os.getenv("url", "https://fc-data.ssi.com.vn/")
        self.stream_url     = os.getenv("stream_url", "https://fc-datahub.ssi.com.vn/")
        self.consumerID     = os.getenv("consumerID", "")
        self.consumerSecret = os.getenv("consumerSecret", "")
        self.auth_type      = os.getenv("auth_type", "Bearer")


# Singleton config instance — imported by producers and other modules
config = _Config()
