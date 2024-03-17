import pandas as pd
from geopy.geocoders import Nominatim
from geopy.geocoders import ArcGIS
import time
import re
from playsound import playsound
import requests
import traceback

def getLocationDetails(lat,long):
    geolocator = Nominatim(user_agent="geoapiExercises")
    location = geolocator.reverse((lat,long))
    address=location.raw['address']
    city = address.get('city','')
    state =address.get('state','')
    postcode=address.get('postcode','')
    return city,state,postcode


def reverseGoogle(lat,lng):
    # Google Geocoding API endpoint
    url = 'https://maps.googleapis.com/maps/api/geocode/json'

    # Your API key (optional, but recommended)
    api_key = ''

    # Parameters for the API request
    params = {
        'latlng': f'{lat},{lng}',
        'key': api_key
    }

    # Make the API request
    response = requests.get(url, params=params)

    # Parse the JSON response
    data = response.json()
    # print(data)
    # Print the formatted address
    if data['status'] == 'OK':
       return data['results'][0]['formatted_address']
    else:
        return 'Error:', data['status']

file_name="./Batches/BATCH_8_REM.xlsx"

df = pd.read_excel(file_name,engine='openpyxl')

if 'Processed' not in df.columns:
    df['Processed']=""
if 'map' not in df.columns:
    df['map']=""

def dms_to_dd(dms):
    """Converts coordinates from degrees, minutes, and seconds to decimal degrees."""
    try:
        parts = dms.replace('°', '-').replace("'", '-').replace('"', '-').split('-')
        degrees = float(parts[0])  # Extract the degrees part
        minutes = float(parts[1]) if len(parts) > 1 else 0  # Extract the minutes part if available
        seconds = float(parts[2]) if len(parts) > 2 else 0  # Extract the seconds part if available
        dd = degrees + minutes / 60.0 + seconds / 3600.0
        return dd
    except Exception as e:
        return 0


max_retries=50
retry_delay=20
for _ in range(max_retries):
    try:
        for index,row in df.iterrows():
            #print(index)
            if ( (not pd.isna(row['Lat']) or  not pd.isna(row['Long']) ) and ( pd.isna(row['Processed']) or df.at[index,'Processed'] =="" ))  :
                print(index)
                print(df.at[index,'Sr No'])
                
                #nom=ArcGIS()
                #df.at[index,'Processed'] = nom.reverse((str(row['Lat']),str(row['Lon'])))
                # getLoc = Nominatim(user_agent="GetLoc")
                # locname=getLoc.reverse((str(row['Lat']),str(row['Long'])))
                # df.at[index,'Processed'] =locname
                
                if("°" in str(row['Lat']).strip()):
                    latit=dms_to_dd(str(row['Lat']).strip())
                    longit=dms_to_dd(str(row['Long']).strip())
                    df.at[index,'map']=str(latit)+","+str(longit)
                    reverseData=reverseGoogle(str(latit),str(longit))
                    df.at[index,'Processed'] =reverseData
                    print(reverseData)
                else:
                    reverseData=reverseGoogle(str(row['Lat']).strip(),str(row['Long']).strip())
                    df.at[index,'Processed'] =reverseData
                    print(reverseData)
                

                # time.sleep(1)
        break
    except Exception as e:
        print(f"An Error occured: {e}")
        # playsound('errorm.mp3')
        traceback.print_exc()
        time.sleep(retry_delay)
    finally :
        df.to_excel(file_name,index=False,engine='openpyxl')



def old_processing():
    df = pd.read_excel('ActualDta.xlsx')
    df.to_excel('output_new.xlsx',index=False)
