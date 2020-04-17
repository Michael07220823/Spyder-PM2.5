import pymongo as mongo
from pymongo.errors import ServerSelectionTimeoutError
import requests as req
from requests import ConnectTimeout,ConnectionError,RequestException,HTTPError,Timeout,TooManyRedirects
import time

def request_AQI_data(url="http://opendata.epa.gov.tw/webapi/Data/REWIQA/?$orderby=SiteName&$skip=0&$top=1000&format=json"):
    error_dict_message = "[INFO] aqi list is empty !"
    print("[INFO] Start capture AQI data...")
    web_page_response = req.get(url, verify = False, timeout = 60)
    aqi = web_page_response.json()
    if len(aqi) == 0:
        raise Exception(error_dict_message)

    capture_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    for item in aqi:
        item["PM25"] = item.get("PM2.5")
        item["PM25_AVG"] = item.get("PM2.5_AVG")
        item["Capture_Time"] = capture_time
        item.pop("PM2.5", "PM2.5_AVG")
        item.pop("PM2.5_AVG")
    # 格式化成2016-03-20 11:45:39形式
    print("[INFO] Capture time: %s" % capture_time)
    print("[INFO] Saving data to mongodb has finish.")
    print("[INFO] Quantity count: ", len(aqi))
    print("[INFO] Sleep 1 hour.")
    print()
    return aqi

def insert_data_to_mongo(ip="localhost", port="27017", data=None, database_name="Air", collection_name="AQI"):
    mongo_client = mongo.MongoClient("mongodb://%s:%s" % (ip, port))
    mongo_database = mongo_client[database_name]
    mongo_collection = mongo_database[collection_name]
    # If key string have '.', insert data will have problem.https://stackoverflow.com/questions/28664383/mongodb-not-allowing-using-in-key
    insert_AQI = mongo_collection.insert_many(data)
    print("[INFO] Data insert successful!")

def insert_error_dict_message_to_mongo(ip="localhost", port="27017", err=None, database_name="Air", collection_name="Error"):
    print("[INFO] Error: ", err)
    mongo_client = mongo.MongoClient("mongodb://%s:%s" %(ip, port))
    mongo_database = mongo_client[database_name]
    mongo_error_dict_collection = mongo_database[collection_name]
    error_dict = dict()
    error_dict["Time"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    error_dict["Message"] = err
    insert_error_dict = mongo_error_dict_collection.insert_one(error_dict)
    print("[INFO] Error message insert successful!")

if __name__ == "__main__":
    delay_time = 3660
    # Use to wait mongodb enable.
    time.sleep(180)

    while True:
        try:
            aqi = request_AQI_data()
            insert_data_to_mongo(data=aqi)
            time.sleep(delay_time)
        except (ConnectTimeout,RequestException,HTTPError,ConnectionError,Timeout,TooManyRedirects,ServerSelectionTimeoutError) as err:
            insert_error_dict_message_to_mongo(err=str(err))
            break
        except KeyboardInterrupt:
            err = "KeyboardInterrupt"
            insert_error_dict_message_to_mongo(err=err)
            break