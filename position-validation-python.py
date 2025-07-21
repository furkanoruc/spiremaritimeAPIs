import requests
import geopandas as gpd
from urllib.parse import quote

auth_key = "YOUR_AUTH_KEY_HERE"

# Build and encode the CQL filter string
raw_cql = (
    "INTERSECTS(position, POLYGON((0 -110, 20 -110, 20 -85, 0 -85, 0 -110))) "
    "AND rfgl_latest_anomaly_event IN ('OUT OF FOOTPRINT', '181/91')"
)
encoded_cql = quote(raw_cql)

# Base WFS endpoint
url = "https://services.exactearth.com/gws/ows"

# Build full URL manually
full_url = (
    f"{url}?service=WFS&version=1.1.0&request=GetFeature"
    f"&outputformat=json&sortBy=ts_pos_utc+D&typenames=exactAIS:RFGL-TARGET"
    f"&cql_filter={encoded_cql}&authKey={auth_key}"
)

# Request
response = requests.get(full_url)

# Debug
print(f"Status code: {response.status_code}")
print("Content preview:\n", response.text[:500])

# If OK, read into GeoDataFrame
if response.status_code == 200 and "application/json" in response.headers.get("Content-Type", ""):
    gdf = gpd.read_file(full_url)
    print(gdf.head())
else:
    print("Failed to load GeoJSON.")
