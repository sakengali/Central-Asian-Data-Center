import pandas as pd
from math import sin, cos, sqrt, atan2, radians


def get_nearest_city(lat: float, lon: float, country: str) -> str:
    city_df = pd.read_csv(f'cities/{country.lower()}.csv')
    min_distance = float('inf')
    nearest_city = 'Unknown'
    
    for index, row in city_df.iterrows():
        R = 6373.0  # Radius of the Earth in kilometers
        
        lat1 = radians(lat)
        lon1 = radians(lon)
        lat2 = radians(float(row['lat']))
        lon2 = radians(float(row['lng']))

        dlon = lon2 - lon1
        dlat = lat2 - lat1

        a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
        c = 2 * atan2(sqrt(a), sqrt(1 - a))

        distance = R * c

        if distance < min_distance:
            min_distance = distance
            nearest_city = row['city']
    
    return nearest_city

