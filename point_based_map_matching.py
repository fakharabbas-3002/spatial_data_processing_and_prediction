from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import pandas as pd
import osmnx as ox
import geopandas as gpd
from tqdm import tqdm
import time



# Returns the closest osm location for a given gps point
# Inputs: lat, Lon
# Outputs: location (osm_id, ...)
def get_osm_location(lat,lon, min_delay_seconds=3):
    geolocator = Nominatim(user_agent="application")
    reverse = RateLimiter(geolocator.reverse, min_delay_seconds=min_delay_seconds)
    location = reverse((lat,lon), language='en', exactly_one=True)
    return location.raw

# Searches closes edge on OSMNX Graph for given gps point
# Inputs: G, lat, lon
# Output: edge (e.g.: (2351758730, 2351758618, 0) )
def get_edge_on_roadnetwork(G, lat, lon):
    edge = ox.distance.nearest_edges(G, X=lon, Y=lat)
    return edge


