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
    aqi_data = []
    
    # Note: If this token is expired, the API will fail.
    headers = {
        "Authorization": "bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VySUQiOjEsImlhdCI6MTc3MTMwNzA2MiwiZXhwIjoxNzcxOTExODYyfQ.yk-P80gW-gYUCZpr4NkX6-JLyeGzhDafOgbYbX86VFs",
        "User-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"
    }

    for slug in slugs:
        try:
            url = f"https://apiserver.aqi.in/aqi/v3/getLocationDetailsBySlug?slug={slug}&type=3&source=web"
            response = requests.get(url, headers=headers, verify=False)
            jsondata = response.json()

            if "data" in jsondata:
                for df in jsondata["data"]:
                    record = {
                        "city": df.get("city"),
                        "state": df.get("state"),
                        "aqi": df.get("iaqi", {}).get("aqi"),
                        "co": df.get("iaqi", {}).get("co"),
                        "humidity": df.get("weather", {}).get("humidity")
                    }
                    aqi_data.append(record)
            else:
                print(f"Warning: No data found for {slug}. Response: {jsondata}")
        except Exception as e:
            print(f"Error fetching data for {slug}: {e}")

    return aqi_data # MOVED OUTSIDE THE LOOP

def put_hotweatherdata():
    hot_weather_data = get_aqi()
    
    if not hot_weather_data:
        print("❌ No data collected. Check API token or slugs.")
        return

    hot_weather_df = pd.DataFrame(hot_weather_data)

    GSHEET_NAME = 'aqi'
    TAB_NAME = 'aqi'

    creds_json = os.environ.get("GSHEET_CREDENTIALS")
    if not creds_json:
        raise ValueError("GSHEET_CREDENTIALS environment variable is empty!")

    try:
        creds_dict = json.loads(creds_json)
        gc = gspread.service_account_from_dict(creds_dict)
        
        # Open the spreadsheet
        sh = gc.open(GSHEET_NAME)
        worksheet = sh.worksheet(TAB_NAME)

        existing_rows = len(worksheet.get_all_values())
        start_row = 1 if existing_rows == 0 else existing_rows + 1
        include_header = True if existing_rows == 0 else False

        set_with_dataframe(
            worksheet,
            hot_weather_df,
            row=start_row,
            include_index=False,
            include_column_header=include_header
        )
        print("✅ Data loaded successfully to Google Sheets!")

    except gspread.exceptions.SpreadsheetNotFound:
        print(f"❌ Error: Spreadsheet '{GSHEET_NAME}' not found. Did you share it with the Service Account email?")
    except json.JSONDecodeError:
        print("❌ Error: GSHEET_CREDENTIALS is not valid JSON. Check your GitHub Secret format.")
    except Exception as e:
        print(f"❌ An unexpected error occurred: {e}")

if __name__ == "__main__":
    put_hotweatherdata()
 
