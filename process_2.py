import pandas as pd
import json
import re
from playsound import playsound
import traceback

def findMob(data):
    pattern = r'\d{10}'
    matches = re.findall(pattern,data)
    return matches

def findPincode(data):
    # pattern = r'\b\d{6}\b'
    pattern=r'(?<!\d)\d{6}(?!\d)'
    matches = re.findall(pattern,data)
    return matches


stateList = ['Andhra Pradesh', 'Arunachal Pradesh', 'Himachal Pradesh', 'Madhya Pradesh', 'Tamil Nadu', 'Chhattisgarh', 'Assam', 'Gujarat', 'Haryana', 'Jharkhand', 'Karnataka', 'Kerala', 'Maharashtra', 'Manipur', 'Meghalaya', 'Mizoram', 'Nagaland', 'Odisha', 'Rajasthan', 'Sikkim', 'Telangana', 'Tripura', 'Uttarakhand', 'Uttar Pradesh', 'West Bengal', 'Jammu and Kashmir', 'New Delhi', 'Delhi', 'Goa', 'Bihar', 'Punjab',"Daman" 'PUDUCHERRY', 'Chandigarh'  ]


def find_city(state,address):
    json_data={}
    with open('./Cities/cities.json','r') as file:
        json_data = json.load(file)
    try:
        # cityList=json_data[state]
        cityListSet=set(json_data[state])
        cityListData=list(cityListSet)
        cityList=sorted(cityListData,key=lambda x:len(set(x)),reverse=True)
        for city in cityList:
            if city.lower() in address.lower():
                return city
    except:
        return None
            

def find_state(address):
    for state in stateList:
        if state.lower() in address.lower():
            return state
    return None

def split_address(address):
    parts = address.split(',')
    middle_index = len(parts)//2
    if len(parts)%2==0:
        middle_index -=1
    part1 = ','.join(parts[:middle_index+1])
    part2 = ','.join(parts[middle_index+1:])
    return part1.strip(),part2.strip()

def removeUnwanted(s):
    s=s.strip()
    while s.endswith(',') or s[-2:]=="()" or s.endswith('-') or s.endswith('–')  or s.endswith('.') :
        if s[-2:]=="()":
            s=s[:-2]
        else:
            s=s[:-1]
        s=s.strip()
    return s

file_name="./Batches/BATCH_8_5195.xlsx"

df = pd.read_excel(file_name,engine='openpyxl')


def process():
    df['Country']='INDIA'
    if 'PINDATA' not in df.columns:
        df['PINDATA']=""
    if 'STATEDATA' not in df.columns:
        df['STATEDATA']=""
    if 'CITYDATA' not in df.columns:
        df['CITYDATA']=""
    for index,row in df.iterrows():
        # print(index)
        if ( not pd.isna(row['Processed']) 
            # and index==76
            )  :


            print(index)
            """
            pinRealData=findPincode(row['Address'])
            pinLocData =findPincode(row['Processed'])
            if len(pinRealData) >1 :
                df.at[index,'Postal Code']=pinRealData[0]
                df.at[index,'PINDATA']='PIN_ISSUE_BUT_ZERO_INDEX'
            elif len(pinRealData) ==1 and 1 == len(pinLocData) and pinRealData[0]==pinLocData[0] :
                df.at[index,'Postal Code']=pinRealData[0]
                df.at[index,'PINDATA']='FULL_MATCH'
            elif len(pinRealData) == 1 and len(pinLocData) > 1:
                df.at[index,'Postal Code']=pinRealData[0]
                df.at[index,'PINDATA']='PIN_IN_REAL_ISSUE_IN_FETCHED'
            elif len(pinRealData) ==0 and len(pinLocData) > 1:
                df.at[index,'Postal Code']=pinLocData[0]
                df.at[index,'PINDATA']='PIN_ISSUE'
            elif len(pinRealData) ==0 and len(pinLocData) == 1 :
                df.at[index,'Postal Code']=pinLocData[0]
                df.at[index,'PINDATA']='PIN_FETCHED'
            elif len(pinRealData) ==1 and len(pinLocData) == 0 :
                df.at[index,'Postal Code']=pinRealData[0]
                df.at[index,'PINDATA']='PIN_IN_REAL_NOT_IN_FETCHED'
            elif len(pinRealData) ==1 and 1 == len(pinLocData) and pinRealData[0]!=pinLocData[0] :
                df.at[index,'Postal Code']=pinRealData[0]
                df.at[index,'PINDATA']='PIN_IN_BOTH_NOT_IN_MATCHED'
            elif len(pinRealData) ==0 and len(pinLocData) == 0 and ( not pd.isna(row['Processed2']) and df.at[index,'Processed2'] !="" ):
                pinInProcessed2= findPincode(row['Processed2'])
                if len(pinInProcessed2)==1:
                    df.at[index,'Postal Code']=pinInProcessed2[0]
                    df.at[index,'PINDATA']='PIN_FETCHED_2'
                elif len(pinInProcessed2)>1:
                    df.at[index,'PINDATA']='PIN_ISSUE_2'

            stateInFetch = find_state(row['Processed2'])
            stateInRealdata= find_state(row['Address'])
            if stateInFetch == stateInRealdata and stateInFetch!=None:
                df.at[index,'State']=stateInFetch
                df.at[index,'STATEDATA']='FULL_MATCH'
            elif stateInFetch != None and stateInRealdata == None:
                df.at[index,'State']=stateInFetch
                df.at[index,'STATEDATA']='STATE_FETCHED'
            elif stateInFetch == None and stateInRealdata != None:
                df.at[index,'State']=stateInRealdata
                df.at[index,'STATEDATA']='STATE_NOT_FETCHED_DATA_FROM_REAL'
            elif stateInFetch != None and stateInRealdata != None:
                df.at[index,'State']=stateInFetch
                df.at[index,'STATEDATA']='STATE_MISMATCH'
            else :
                df.at[index,'STATEDATA']=stateInRealdata 
                
            """
            cityInRealdata=None

            if( not pd.isna(row['State']) or df.at[index,'State']!='' or df.at[index,'State']!=None  ):
                to_check=True
                words = row['Processed2'].split(', ')
                for i in range(len(words)):
                    if 'District'.lower() in words[i].lower():
                        data_city=words[i].replace('District','')
                        if data_city.strip().lower() in row['Address'].lower():
                            df.at[index,'City']=data_city.strip()
                            df.at[index,'CITYDATA']='EXACT_MATCH'
                            to_check=False
                            break
                if to_check == True:            
                    cityInFetch = find_city(df.at[index,'State'],row['Processed2'])
                    cityInRealdata= find_city(df.at[index,'State'],row['Address'])
                    print(cityInFetch)
                    print(cityInRealdata)
                    if cityInFetch == cityInRealdata and cityInFetch !=None :
                        df.at[index,'City']=cityInRealdata
                        df.at[index,'CITYDATA']='FULL_MATCH'
                    elif cityInFetch != None and cityInRealdata != None and cityInFetch != cityInRealdata:
                        df.at[index,'City']=cityInRealdata
                        df.at[index,'CITYDATA']='CITY_NOT_MATCHED'
                    elif cityInFetch != None and cityInRealdata == None:
                        df.at[index,'City']=cityInFetch
                        df.at[index,'CITYDATA']='CITYDATA_FETCHED'
                    elif cityInFetch == None and cityInRealdata != None:
                        df.at[index,'City']=cityInRealdata
                        df.at[index,'CITYDATA']='CITY_NOT_FETCHED-DATA_FROM_REAL'

            mob_pattern = r'\b\d{10}\b'
            cleanAddress = re.sub(mob_pattern,'',df.at[index,'Address'])
            pin_pattern = r'\b\d{6}\b'
            cleanAddress = re.sub(pin_pattern,'',cleanAddress)


            #--NOt in use 

            # if(stateInRealdata!=None) :
            #     pattern = re.compile(re.escape(stateInRealdata),re.IGNORECASE)
            #     cleanAddress = pattern.sub("",cleanAddress)
            # if(cityInRealdata!=None) :
            #     # cleanAddress = cleanAddress.replace(cityInRealdata,'')
            #     pattern = re.compile(re.escape(cityInRealdata),re.IGNORECASE)
            #     cleanAddress = pattern.sub("",cleanAddress)
            # df.at[index,'Address Line 1'],df.at[index,'Address Line 2']=split_address(cleanAddress)

             #--NOt in use 

            address1=None
            address2=None
            address1,address2=split_address(cleanAddress.strip())
            df.at[index,'Address Line 1']=""
            df.at[index,'Address Line 2']=""

            if(address1!=None):
                # address1=str(address1)
                # address1=row['Address Line 1']
                address1=' '.join(address1.split())
                address1=removeUnwanted(address1)

            if(address2!=None):
                # address2=row['Address Line 2']
                address2=' '.join(address2.split())
                address2=removeUnwanted(address2)


            print(address1)
            print(address2)
        
            if(address1!=None and address2!=None and address2!="" and len(address1)+len(address2)<=30):
                df.at[index,'Address Line 1']=address1+" ,"+address2
                df.at[index,'Address Line 2']=""
            else:
                df.at[index,'Address Line 1']=address1
                df.at[index,'Address Line 2']=address2  
            
        
try :
    process()
except Exception as e:
    print(f"An Error occured: {e}")
    traceback.print_exc()
    # playsound('errorm.mp3')
finally:
    df.to_excel(file_name,index=False,engine='openpyxl')