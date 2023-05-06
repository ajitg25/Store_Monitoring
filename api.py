# importing different files
from fastapi import FastAPI
import pymongo
from datetime import datetime,timedelta,time
import csv
from bson import ObjectId
import pandas as pd
from pydantic import BaseModel
import pytz

# app
app = FastAPI()

#connect to mongodb
def get_database():
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")

    # database name
    mydb = myclient["Restro"]

    # calling collections
    active_data = mydb["ActiveHr"]
    business_hour = mydb["BusinessHr"]
    timezone = mydb["timezone"]

    print("Connected Succesfully...")
    return mydb,active_data,business_hour,timezone

# get the current time and data
current_time = datetime.now()  #actual
current_time = datetime(2023, 1, 25, 12, 0, 0) #for test purpose as data is old

UTC_format = '%Y-%m-%d %H:%M:%S.%f %Z'

# declaring date of one week before and previous day with respective to present day
week_ago = (current_time - timedelta(days=7)).replace(hour=0, minute=0, second=0)
today_start = current_time.replace(hour=0, minute=0, second=0)
previous_date = (current_time - timedelta(days=1)).replace(hour=0, minute=0, second=0)
previous_hour = current_time - timedelta(hours=1)

# Business Hour
clean_Business = {}
def get_business_hour(business_hour,storeID):

    query = {'store_id': storeID}
    din = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']

    # getting the business our of only that particular store ID
    mydoc = business_hour.find(query)

    # local timezone of that store
    real_timezone = timezone.find_one(query)
    local = pytz.timezone(real_timezone['timezone_str'])

    for x in mydoc:
        temp = {}

        # converting local timezone to UTC for both starting and ending usiness hour
        naive = datetime.strptime( str(current_time.date()) +' ' + x['start_time_local'], "%Y-%m-%d %H:%M:%S")
        local_dt = local.localize(naive, is_dst=None)
        utc_dt = local_dt.astimezone(pytz.utc)
        utc_dt.strftime("%Y-%m-%d %H:%M:%S")
        temp['start'] = (str(utc_dt).split(' ')[1]).split('+')[0]

        naive = datetime.strptime( str(current_time.date()) +' ' + x['end_time_local'], "%Y-%m-%d %H:%M:%S")
        local_dt = local.localize(naive, is_dst=None)
        utc_dt = local_dt.astimezone(pytz.utc)
        utc_dt.strftime("%Y-%m-%d %H:%M:%S")
        temp['end'] = (str(utc_dt).split(' ')[1]).split('+')[0]

        # makig dictionary with key as day and start and end time of business hour in UTC
        clean_Business[din[x['day']]] = temp
    

# active hours
def get_active_details(active_data,storeID):
    query = {'store_id': storeID}

    clean_active = []
    clean_active_day = []
    clean_active_hr = []

    # all the data of that store then sorting them according to timestamp
    active_mydoc = active_data.find(query).sort("timestamp_utc",1)

    for x in active_mydoc:
        datetime_str = datetime.strptime(x['timestamp_utc'], UTC_format)
        prev_date = datetime_str.date()
        cur_dat = current_time.date()

        #pass all the rows which are older than 7 days
        if(cur_dat-prev_date>timedelta(days=7)):
            continue 

        # get the day, status , timestamp and number of days between today and the day in DB
        temp = [ datetime_str.strftime('%A'),x['status'],x['timestamp_utc'],cur_dat-prev_date]
        if(len(clean_active)==0):
            clean_active.append(temp)
        elif(clean_active[-1][1]!=x['status']):
            if(cur_dat-prev_date!=timedelta(days=0)): #append for  previous week
                clean_active.append(temp)
            if(cur_dat-prev_date==timedelta(days=1)): #append for previous day
                clean_active_day.append(temp)
            if((current_time-datetime_str).total_seconds()>=0 and (current_time-datetime_str).total_seconds()<=3600):  #append for previous hour
                clean_active_hr.append(temp)  


    #fixing the day of a week before
    if(len(clean_active)>0):
        week_ago_str = week_ago.strftime('%Y-%m-%d %H:%M:%S.%f') + ' UTC'
        today_start_str = today_start.strftime('%Y-%m-%d %H:%M:%S.%f') + ' UTC'
        clean_active.insert(0,[clean_active[0][0],clean_active[0][1],week_ago_str,clean_active[0][3]])
        clean_active.append([clean_active[-1][0],'inactive' if (clean_active[-1][1]=='active') else 'active',today_start_str,clean_active[-1][3]])

    #fixing the day of yesterday
    if(len(clean_active_day)>0):
        previous_date_str = previous_date.strftime('%Y-%m-%d %H:%M:%S.%f') + ' UTC'
        today_start_str = today_start.strftime('%Y-%m-%d %H:%M:%S.%f') + ' UTC'
        clean_active_day.insert(0,[clean_active_day[0][0],clean_active_day[0][1],previous_date_str,clean_active_day[0][3]])
        clean_active_day.append([clean_active_day[-1][0],'inactive' if (clean_active_day[-1][1]=='active') else 'active',today_start_str,clean_active_day[-1][3]])

    #fixing the day of last hour
    if(len(clean_active_hr)>0):
        previous_hour_str = previous_hour.strftime('%Y-%m-%d %H:%M:%S.%f') + ' UTC'
        current_time_str = current_time.strftime('%Y-%m-%d %H:%M:%S.%f') + ' UTC'
        clean_active_hr.insert(0,[clean_active_hr[0][0],clean_active_hr[0][1],previous_hour_str,clean_active_hr[0][3]])
        clean_active_hr.append([clean_active_hr[-1][0],'inactive' if (clean_active_hr[-1][1]=='active') else 'active',current_time_str,clean_active_hr[-1][3]])

    return clean_active,clean_active_day,clean_active_hr


#calculate active and inactive time
def cal_act_in(clean_active):

    inactive = 0
    active = 0
    for i in range(1,len(clean_active)):
        x = clean_active[i]
        # print(x)
        T_prev = clean_active[i-1][2]
        T = x[2]

        T_prev = datetime.strptime(T_prev, UTC_format)
        T_time = datetime.strptime(T, UTC_format)

        delSec = (T_time-T_prev).total_seconds()
        deltatSeconds = (delSec if delSec>0 else delSec+24*60*60)
        # if the key exist so that mean the business hour is defined 
        if x[0] in clean_Business.keys():
            bstart = clean_Business[x[0]]['start']
            bend = clean_Business[x[0]]['end']
            bstart = datetime.strptime(bstart, '%H:%M:%S').time()
            bend = datetime.strptime(bend, '%H:%M:%S').time()

            bstart = datetime.combine(T_time.date(), bstart)
            bend = datetime.combine(T_time.date(), bend)
       
            if(T_prev<bstart):
                T_prev = bstart

            
            if(x[1]=='inactive'):
                # print("inactive IN")
                # print("active time: ",T_time-T_prev)
                active = active + deltatSeconds
            else:
                # print("active IN")
                # print("inactive time: ",T_time-T_prev)
                inactive = inactive + deltatSeconds       
        # store is opened for 24*7
        else:
            if(x[1]=='inactive'):
                # print("inactive OUT")
                # print("active time: ",T_time-T_prev)
                active = active + deltatSeconds
            else:
                # print("active OUT")
                # print("inactive time: ",T_time-T_prev)
                inactive = inactive + (T_time-T_prev).total_seconds()
           

    return [active,inactive]


to_hrs = 3600
to_mins = 60

# connect the DB and get all collections
mydb,active_data,business_hour,timezone = get_database()

@app.get("/")
async def root():
    return {"message": "Hello World"}

#base model to recieve the req from the /trigger_report endpoint
class storeIDD(BaseModel):
    ID: int

@app.post("/trigger_report")
async def trigger(item: storeIDD):
    get_business_hour(business_hour,item.ID)
    print(clean_Business)
    print()

    clean_active,clean_active_day,clean_active_hr = get_active_details(active_data,item.ID)
    print("clean active: " ,clean_active)
    print()
    print("clean active_day: " ,clean_active_day)
    print()
    print("clean active_hr: " ,clean_active_hr)

    uptime_last_week, downtime_last_week = cal_act_in(clean_active)
    uptime_last_day, downtime_last_day = cal_act_in(clean_active_day)
    uptime_last_hour, downtime_last_hour = cal_act_in(clean_active_hr)

    val = {'store_id': item.ID, 'uptime_last_hour(in minutes)': round(uptime_last_hour/to_mins,2), 'uptime_last_day(in hours)': round(uptime_last_day/to_hrs,2),
            'uptime_last_week(in hours)': round(uptime_last_week/to_hrs,2),'downtime_last_hour(in minutes)': round(downtime_last_hour/to_mins,2),
            'downtime_last_day(in hours)': round(downtime_last_day/to_hrs,2),'downtime_last_week(in hours)': round(downtime_last_week/to_hrs,2)}
    
    #storing the report in separate "reports" collection
    mycol = mydb["reports"]
    r = mycol.insert_one(val).inserted_id

    #returning a reportID using which they can get their report
    return {"ReportID": str(r)}

@app.get("/get_report/{report_id}")
async def get_report(report_id:str):
    mycol = mydb["reports"]
    object_id = ObjectId(report_id)

    #finding the reportID in mongoDB
    document = mycol.find_one({'_id': object_id})

    #converting into csv format
    df = pd.DataFrame([document])
    report_csv = df.to_csv(index=False)

    # returning the report
    return {"Report" : report_csv}

