from fastapi import FastAPI
import pymongo
from datetime import datetime,timedelta,time
import csv
from bson import ObjectId
import pandas as pd
from pydantic import BaseModel
import pytz

app = FastAPI()

#connect to mongodb
def get_database():
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["Restro"]

    active_data = mydb["ActiveHr"]
    business_hour = mydb["BusinessHr"]
    timezone = mydb["timezone"]

    print("Connected Succesfully...")
    return mydb,active_data,business_hour,timezone

current_time = datetime.now()  #actual
current_time = datetime(2023, 1, 25, 12, 0, 0) #for test purpose as data is old

UTC_format = '%Y-%m-%d %H:%M:%S.%f %Z'
storeID = 2964897505220625860 #this will be recieved from frontend

query = {'store_id': storeID}

week_ago = (current_time - timedelta(days=7)).replace(hour=0, minute=0, second=0)
today_start = current_time.replace(hour=0, minute=0, second=0)
previous_date = (current_time - timedelta(days=1)).replace(hour=0, minute=0, second=0)
previous_hour = current_time - timedelta(hours=1)

# print("current_time: ",current_time)
# print("week_ago: ",week_ago)
# print("today_start: ",today_start)
# print("previous_date: ",previous_date)
# print("previous_hour: ",previous_hour)
   
   
# Business Hour
clean_Business = {}
def get_business_hour(business_hour):
    din = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
    mydoc = business_hour.find(query)

    real_timezone = timezone.find_one(query)
    local = pytz.timezone(real_timezone['timezone_str'])
    for x in mydoc:
        temp = {}

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

        clean_Business[din[x['day']]] = temp

    # #hardcoded for test purpose to increase edge cases
    # clean_Business['Wednesday'] = clean_Business['Monday']
    # clean_Business['Wednesday']['start'] = '10:00:00'
    # clean_Business['Wednesday']['end'] = '12:00:00'


# active hours
def get_active_details(active_data):
    clean_active = []
    clean_active_day = []
    clean_active_hr = []

    active_mydoc = active_data.find(query).sort("timestamp_utc",1)

    for x in active_mydoc:
        datetime_str = datetime.strptime(x['timestamp_utc'], UTC_format)
        prev_date = datetime_str.date()
        cur_dat = current_time.date()

        #logic to check the difference in todays date and the date in the database
        # only take the prev week
        if(cur_dat-prev_date>timedelta(days=7)):
            continue 

        temp = [ datetime_str.strftime('%A'),x['status'],x['timestamp_utc'],cur_dat-prev_date]
        if(len(clean_active)==0):
            clean_active.append(temp)
            
        elif(clean_active[-1][1]!=x['status']):
            if(cur_dat-prev_date!=timedelta(days=0)):
                clean_active.append(temp)
            if(cur_dat-prev_date==timedelta(days=1)):
                clean_active_day.append(temp)

            if((current_time-datetime_str).total_seconds()>=0 and (current_time-datetime_str).total_seconds()<=3600):
                clean_active_hr.append(temp)  


    week_ago_str = week_ago.strftime('%Y-%m-%d %H:%M:%S.%f') + ' UTC'
    today_start_str = today_start.strftime('%Y-%m-%d %H:%M:%S.%f') + ' UTC'
    clean_active.insert(0,[clean_active[0][0],clean_active[0][1],week_ago_str,clean_active[0][3]])
    clean_active.append([clean_active[-1][0],'inactive' if (clean_active[-1][1]=='active') else 'active',today_start_str,clean_active[-1][3]])
    # print("clean active: " ,clean_active)

    previous_date_str = previous_date.strftime('%Y-%m-%d %H:%M:%S.%f') + ' UTC'
    today_start_str = today_start.strftime('%Y-%m-%d %H:%M:%S.%f') + ' UTC'
    clean_active_day.insert(0,[clean_active_day[0][0],clean_active_day[0][1],previous_date_str,clean_active_day[0][3]])
    clean_active_day.append([clean_active_day[-1][0],'inactive' if (clean_active_day[-1][1]=='active') else 'active',today_start_str,clean_active_day[-1][3]])
    # print("clean active_day: " ,clean_active_day)

    previous_hour_str = previous_hour.strftime('%Y-%m-%d %H:%M:%S.%f') + ' UTC'
    current_time_str = current_time.strftime('%Y-%m-%d %H:%M:%S.%f') + ' UTC'
    clean_active_hr.insert(0,[clean_active_hr[0][0],clean_active_hr[0][1],previous_hour_str,clean_active_hr[0][3]])
    clean_active_hr.append([clean_active_hr[-1][0],'inactive' if (clean_active_hr[-1][1]=='active') else 'active',current_time_str,clean_active_hr[-1][3]])
    # print("clean active_hr: " ,clean_active_hr)

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
                active = active + (T_time-T_prev).total_seconds()

            else:
                # print("active IN")
                # print("inactive time: ",T_time-T_prev)
                inactive = inactive + (T_time-T_prev).total_seconds()

        else:
            if(x[1]=='inactive'):
                # print("inactive OUT")
                # print("active time: ",T_time-T_prev)
                active = active + (T_time-T_prev).total_seconds()

            else:
                # print("active OUT")
                # print("inactive time: ",T_time-T_prev)
                inactive = inactive + (T_time-T_prev).total_seconds()

    return [active,inactive]

to_hrs = 3600
to_mins = 60

mydb,active_data,business_hour,timezone = get_database()

get_business_hour(business_hour)
print(clean_Business)
# clean_active,clean_active_day,clean_active_hr = get_active_details(active_data)


@app.get("/")
async def root():
    return {"message": "Hello World"}

# report_id = str(storeID)+"_"+str(datetime.now().strftime("%Y-%m-%d_%H-%M-%S"))
@app.get("/trigger_report")
async def trigger():
    get_business_hour(business_hour)
    clean_active,clean_active_day,clean_active_hr = get_active_details(active_data)
    uptime_last_week, downtime_last_week = cal_act_in(clean_active)
    uptime_last_day, downtime_last_day = cal_act_in(clean_active_day)
    uptime_last_hour, downtime_last_hour = cal_act_in(clean_active_hr)

    # print(clean_active)
    field_names= ['store_id', 'uptime_last_hour(in minutes)', 'uptime_last_day(in hours)',
                   'uptime_last_week(in hours)','downtime_last_hour(in minutes)',
                   'downtime_last_day(in hours)','downtime_last_week(in hours)']
    
    val = {'store_id': storeID, 'uptime_last_hour(in minutes)': round(uptime_last_hour/to_mins,2), 'uptime_last_day(in hours)': round(uptime_last_day/to_hrs,2),
            'uptime_last_week(in hours)': round(uptime_last_week/to_hrs,2),'downtime_last_hour(in minutes)': round(downtime_last_hour/to_mins,2),
            'downtime_last_day(in hours)': round(downtime_last_day/to_hrs,2),'downtime_last_week(in hours)': round(downtime_last_week/to_hrs,2)}
    
    mycol = mydb["reports"]
    r = mycol.insert_one(val).inserted_id



    # report_id = str(r.inserted_id)
    # with open(report_id+'.csv', 'w') as csvfile:
    #     writer = csv.DictWriter(csvfile, fieldnames = field_names)
    #     writer.writeheader()
    #     writer.writerows(val)

    return {"message": str(r)}

@app.get("/get_report/{report_id}")
async def get_report(report_id:str):
    mycol = mydb["reports"]
    # a= mongoose.Types.ObjectId()
    object_id = ObjectId(report_id)
    document = mycol.find_one({'_id': object_id})

    df = pd.DataFrame([document])
    my_csv_string = df.to_csv(index=False)

    results = {report_id : my_csv_string}

    return results

class logIN(BaseModel):
    name: str
    password: str
    store_ID: int


@app.post("/login/")
async def login(item: logIN):
    print(item)
    storeID = item.store_ID

    return {"message": "logged in"}