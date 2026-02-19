import requests
import urllib3
import pandas as pd
import os
import gspread
import json
from gspread_dataframe import set_with_dataframe

urllib3.disable_warnings()
def get_aqi():
    slugs = [
            "india/gujarat/ahmedabad", "india/karnataka/bangalore",
            "india/tamil-nadu/chennai", "india/telangana/hyderabad",
            "india/west-bengal/kolkata", "india/maharashtra/mumbai",
            "india/delhi/new-delhi", "india/maharashtra/pune",
            "india/uttar-pradesh/lucknow", "india/punjab/ludhiana",
            "india/madhya-pradesh/bhopal", "india/haryana/gurgaon",
            "india/kerala/kochi", "india/odisha/bhubaneswar",
            "india/jharkhand/ranchi", "india/goa/madgaon",
            "india/rajasthan/jaipur", "india/chhattisgarh/raipur",
            "india/assam/guwahati", "india/bihar/patna",
            "india/uttarakhand/dehradun", "india/himachal-pradesh/shimla",
            "india/sikkim/gangtok"
        ]
    aqi_data=[]
    for slug in slugs:
        url = f"https://apiserver.aqi.in/aqi/v3/getLocationDetailsBySlug?slug={slug}&type=3&source=web"
        Headers={
            "Authorization":"bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VySUQiOjEsImlhdCI6MTc3MTMwNzA2MiwiZXhwIjoxNzcxOTExODYyfQ.yk-P80gW-gYUCZpr4NkX6-JLyeGzhDafOgbYbX86VFs",
           "User-agent" :"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36 Edg/144.0.0.0"
        }
        response = requests.get(url,headers=Headers,verify=False)
        jsondata =response.json()

        for df in jsondata["data"]:
            record={
                "city":df["city"],
                "state":df["state"],
                "aqi":df["iaqi"]["aqi"],
                "co":df["iaqi"]["co"],
                "humidity":df["weather"]["humidity"]
            }
            aqi_data.append(record)

        return aqi_data


def put_hotweatherdata():
    hot_weather_data = get_aqi()

    # Convert list → DataFrame
    hot_weather_df = pd.DataFrame(hot_weather_data)

    GSHEET_NAME = 'aqi'
    TAB_NAME = 'aqi'

    creds_json = os.environ.get("GSHEET_CREDENTIALS")
    if not creds_json:
        raise ValueError("GSHEET_CREDENTIALS not found")

    creds_dict = json.loads(creds_json)
    gc = gspread.service_account_from_dict(creds_dict)

    sh = gc.open(GSHEET_NAME)
    worksheet = sh.worksheet(TAB_NAME)

    existing_rows = len(worksheet.get_all_values())

    if existing_rows == 0:
        start_row = 1
        include_header = True
    else:
        start_row = existing_rows + 1
        include_header = False

    set_with_dataframe(
        worksheet,
        hot_weather_df,
        row=start_row,
        include_index=False,
        include_column_header=include_header
    )

    print("✅ Data loaded successfully to Google Sheets!")
