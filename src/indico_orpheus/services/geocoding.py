from __future__ import annotations

import pandas as pd
from geopy.geocoders import GoogleV3


class GeocoderService:
    def __init__(self, api_key: str):
        self.geolocator = GoogleV3(api_key=api_key)

    def geocode_address(self, address: str) -> pd.Series:
        try:
            location = self.geolocator.geocode(address)
            if location:
                return pd.Series([location.latitude, location.longitude, location.address])
            return pd.Series([None, None, None])
        except Exception:
            return pd.Series([None, None, None])