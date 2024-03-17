import pandas as pd
import json
import re
import time
from geopy.geocoders import Nominatim
import traceback


file_name="./Batches/BATCH_8_4895_PROCESSED.xlsx"



def findPincode(data):
    # pattern = r'\b\d{6}\b'
    pattern=r'(?<!\d)\d{6}(?!\d)'
    matches = re.findall(pattern,data)
    return matches

def checkCount():
    df = pd.read_excel(file_name)
    check=0
    for index,row in df.iterrows():
        pinInGData=findPincode(str(df.at[index,'Processed']))
        pinInReal=findPincode(str(df.at[index,'Address']))
        pinIn2=findPincode(str(df.at[index,'Processed2']))
        if(len(pinInGData)==0 and len(pinInReal)==0 and  (df.at[index,'Lat'] != 0 and df.at[index,'Lat'] !=0) 
           and len(pinIn2)==0
           ):
            check=check+1
    print(check)  

stateList = ['Andhra Pradesh', 'Arunachal Pradesh', 'Himachal Pradesh', 'Madhya Pradesh', 'Tamil Nadu', 'Chhattisgarh', 'Assam', 'Gujarat', 'Haryana', 'Jharkhand', 'Karnataka', 'Kerala', 'Maharashtra', 'Manipur', 'Meghalaya', 'Mizoram', 'Nagaland', 'Odisha', 'Rajasthan', 'Sikkim', 'Telangana', 'Tripura', 'Uttarakhand', 'Uttar Pradesh', 'West Bengal', 'Jammu and Kashmir', 'New Delhi', 'Delhi', 'Goa', 'Bihar', 'Punjab',"Daman"  ]

def checkStateInFile():
    df = pd.read_excel(file_name)
    check=0
    cityListSet=set()
    for index,row in df.iterrows():
        try:
            # if(index==2):
            #     break
            address=str(df.at[index,'Processed2'])
            # print(address)
            # for state in stateList:
            #     print(state)
            #     if state.lower() in address.lower():
            #         cityListSet.add(state)
            if(  not pd.isna(row['State']) 
            #    and  row['State']=="Gujarat"
               ):
                words = address.split(', ')
                for i in range(len(words)):
                    if 'District'.lower() in words[i].lower():
                        data_city=words[i].replace('District','')
                        cityListSet.add(data_city.strip())
                        if data_city.strip().lower() in row['Address'].lower():
                            check=check+1
        except Exception as e:
            print(f" Some issue {e}")
            traceback.print_exc()
    cityListData=list(cityListSet)
    print(cityListData)
    print(check)  



def addMissingPincode():
    df = pd.read_excel(file_name,engine='openpyxl')
    if 'Processed2' not in df.columns:
        df['Processed2']=""
    max_retries=50
    retry_delay=20
    for _ in range(max_retries):
        try:
            for index,row in df.iterrows():
                
                # pinInGData=findPincode(str(df.at[index,'Processed']))
                # pinInReal=findPincode(str(df.at[index,'Address']))
                # if(len(pinInGData)==0 and len(pinInReal)==0 ):
                    if ( (not pd.isna(row['Lat']) or  not pd.isna(row['Long']) ) 
                        and (df.at[index,'Lat'] != 0 and df.at[index,'Lat'] !=0)
                            and ( pd.isna(row['Processed2']) or df.at[index,'Processed2'] =="" )
                            )  :
                        print(index,"-",df.at[index,'Sr No'])
                        #nom=ArcGIS()
                        #df.at[index,'Processed'] = nom.reverse((str(row['Lat']),str(row['Lon'])))
                        try :
                            if("°" in str(row['Lat']).strip() and  ( (not pd.isna(row['map']) or str(row['map'])!=""  ))):
                                print(index,"-",df.at[index,'Sr No'])
                                getLoc = Nominatim(user_agent="GetLoc")
                                locname=getLoc.reverse((str(row['map'])))
                                df.at[index,'Processed2'] =locname
                            else:
                                print(index,"-",df.at[index,'Sr No'])
                                getLoc = Nominatim(user_agent="GetLoc")
                                locname=getLoc.reverse((str(row['Lat']),str(row['Long'])))
                                df.at[index,'Processed2'] =locname
                        except Exception as e:
                                print(f'some error {e}')
                        time.sleep(1.5)
            break
        except Exception as e:
            print(f"An Error occured: {e}")
            # playsound('errorm.mp3')
            time.sleep(retry_delay)
        finally :
            df.to_excel(file_name,index=False,engine='openpyxl')



def checkingPincodeInTexts():
    df = pd.read_excel(file_name,engine='openpyxl')
    if 'CHECK' not in df.columns:
        df['CHECK']=""
    if 'CHECK_SUM' not in df.columns:
        df['CHECK_SUM']=""
    try:
        for index,row in df.iterrows():
            print(index,"-",df.at[index,'Sr No'])
            address_actual=row['Address']
            address1=row['Address Line 1']
            address2=row['Address Line 2']
            pattern=r'(?<!\d)\d{6}(?!\d)'
            mobile_pattrn = r'\d{10}'
            large_nu_pattrn= r'\d{11,}'
            pattern_pincode_with_space=r'(?<!\d)\d{3} \d{3}(?!\d)'
            list_pincode_space=re.findall(pattern_pincode_with_space,str(address_actual))
            list_pincode=re.findall(pattern,str(address_actual))

            """
            
            if len(list_pincode)>0:
                df.at[index,'CHECK_SUM']=str(int(row['Postal Code']))==str(list_pincode[0])
            
            
            
            if len(list_pincode)>1:
                df.at[index,'CHECK_SUM']='PIN_ISSUE'
                if(  pd.isna(row['Postal Code'])):
                    df.at[index,'CHECK']=list_pincode[0]

            if(  ( not pd.isna(address_actual) and len(list_pincode_space)>0 ) ):
                df.at[index,'CHECK']='PIN_SPACE'
                # if str(list_pincode_space[0]).replace(' ','') ==str(row['Postal Code']) :
                df.at[index,'CHECK_SUM']=str(int(row['Postal Code']))==str(str(list_pincode_space[0]).replace(' ',''))

            

            
            if len(list_pincode)>0:
                df.at[index,'CHECK_SUM']=str(int(row['Postal Code']))==str(list_pincode[0])
              
            
            
            
            
            
            """ 
            
            if(  ( not pd.isna(address1) and len(re.findall(large_nu_pattrn,str(address1)))>0 ) or
               ( not pd.isna(address2) and len(re.findall(large_nu_pattrn,str(address2)))>0 )):
                df.at[index,'CHECK']='LARGE_NUM_PRESSENT'
            elif(  ( not pd.isna(address1) and len(re.findall(pattern,str(address1)))>0 ) or
               ( not pd.isna(address2) and len(re.findall(pattern,str(address2)))>0 )):
                df.at[index,'CHECK']='PIN_PRESSENT'
            elif(  ( not pd.isna(address1) and len(re.findall(mobile_pattrn,str(address1)))>0 ) or
               ( not pd.isna(address2) and len(re.findall(mobile_pattrn,str(address2)))>0 )):
                df.at[index,'CHECK']='MOBILE_PRESSENT'
            elif(  ( not pd.isna(address1) and   ( "\\".lower() in address1.lower() or "Kumar".lower() in address1.lower() 
                      or "PIN".lower() in address1.lower()  or "Singh".lower() in address1.lower() or "Delhi".lower() in address1.lower() 
                      ) ) or  
                      ( not pd.isna(address2) and    
                       (  "\\".lower() in address2.lower() or "Kumar".lower() in address2.lower() 
                      or "PIN".lower() in address2.lower()  or "Singh".lower() in address2.lower() or "Delhi".lower() in address2.lower()  ))) :
                    df.at[index,'CHECK']='INVALID'
               
    except Exception as e:
        print(f"An Error occured: {e}")
        traceback.print_exc()
        # playsound('errorm.mp3')
    finally :
        df.to_excel(file_name,index=False,engine='openpyxl')



def find_city(state):
    json_data={}
    with open('./Cities/cities.json','r') as file:
        json_data = json.load(file)
        cityListSet=set(json_data[state])
        cityListData=list(cityListSet)
        cityList=sorted(cityListData,key=lambda x:len(set(x)),reverse=True)
        print(cityList)
        print()
        print(str(cityList).replace("'","\""))
        # for city in cityList:
        #     print(city)


# find_city("West Bengal")
# print()
# find_city("Gujarat")
# print()
# find_city("Uttarakhand")
# checkCount()
# addMissingPincode()
checkingPincodeInTexts()

# checkStateInFile()




# data="I-36B, Thokar No.4, Abul Fazal Enc,Jamia Nagar Okhla New Delhi-110 025Mashkoor Alam-98999999482"
# # pattern = r'\d{10}'
# pattern = r'\d{11,}'
# # pattern=r'\d{6}(?!\d{4})'

# pattern=r'(?<!\d)\d{6}(?!\d)'

# pattern=r'(?<!\d)\d{3} \d{3}(?!\d)'
# matches = re.findall(pattern,data)
# print(matches)
# print(findPincode(data))
