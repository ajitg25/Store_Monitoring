# Store_Monitoring

## Installation

To install this project, follow these steps:

1. `pip install -r requirements.txt`
2. Declare database namy as "Restro"
   Colections as:
    store_status.csv as "ActiveHr"
    Menu_hours.csv as "BusinessHr"
    timezone.csv as "timezone"  in mongoDB Compass
3. Run the api file using,
    `uvicorn api:app --reload`
    

## Description

It defines a FastAPI application instance app, connects to a MongoDB database named Restro and initializes collections active_data, business_hour, and timezone. It then gets the current time and date, and declares variables for one week before and the previous day.

It has three functions cal_act_in, get_business_hour() and get_active_details() which take business_hour, active_data and storeID as arguments, and retrieve business hours and active details of a store from MongoDB.

The get_business_hour() function retrieves the business hours of a store, converts the local timezone of the store to UTC time, and creates a dictionary with the day and start and end time of business hours in UTC.

The get_active_details() function retrieves all the data of a store, sorts them according to timestamp, and filters out rows older than 7 days. It then creates a list with the day, status, timestamp, and number of days between today and the day in the database. It also creates three more lists for the previous week, the previous day, and the previous hour.

The cal_act_in function is responsible for the ccalculation of the total active and inactive time taking care all the conditions whether that day has some business hour or its open for whole day. It calculates all the time in seconds and return the active and inactive time.

There are two endpoint:
1. /trigger_report, this with the help of all three mentioned functions gets the report and insert it into database in the report collection. It returns the name of the report with which its stored in mongodb and this report name can be used to fetch the report from database.
2. /get_report, this endpoint gets the report from the database and displays in the front end after converting it into csv format.



## Video Link:
https://drive.google.com/file/d/1GNcw4SDzzU_NDzv7AAE6YmYvF4yXfdSr/view?usp=share_link
