import pandas as pd
import json
import re
import requests
import traceback

def reverse(lat,lng):
    # Google Geocoding API endpoint
    url = 'https://maps.googleapis.com/maps/api/geocode/json'

    # Your API key (optional, but recommended)
    api_key = 'AIzaSyATARwR76HifXrqVCeNHYdnGN_8arD5M1c'

    # Parameters for the API request
    params = {
        'latlng': f'{lat},{lng}',
        'key': api_key
    }

    # Make the API request
    response = requests.get(url, params=params)

    # Parse the JSON response
    data = response.json()
    print(data)
    # Print the formatted address
    if data['status'] == 'OK':
       return data['results'][0]['formatted_address']
    else:
        return 'Error:', data['status']


df = pd.read_excel('Final_Processing.xlsx',engine='openpyxl')

def findPincode(data):
    pattern = r'\b\d{6}\b'
    matches = re.findall(pattern,data)
    return matches

def googleData():
    try :
        for index,row in df.iterrows():
                # print(row['Address'])
                #print(index)
            reverseData=""
            print(index)
            
            if(not pd.isna(row['Lat']) or  not pd.isna(row['Long']) 
               and ( pd.isna(row['GADDRESS']) or df.at[index,'GADDRESS'] =="" ) 
                 ):
                reverseData=reverse(str(row['Lat']),str(row['Long']))
                df.at[index,'GADDRESS']=reverseData
            googlePincode=""
            googlePincode=findPincode(str(reverseData))
            if (len(googlePincode)>0):
                df.at[index,'GPIN']=googlePincode[0]
                if(str(df.at[index,'Postal Code'])==str(googlePincode[0])):
                    df.at[index,'PIN_MATCH']='Y'
            if(index==5):
                break
    except Exception as e:
        print(f"An Error occured: {e}") 
        traceback.print_exc()
    finally :
        print("done")
        # df.to_excel('Final_Processing.xlsx',index=False,engine='openpyxl')

googleData()