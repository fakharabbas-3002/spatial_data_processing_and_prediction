from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
from datetime import date, timedelta
from selenium.webdriver.chrome.options import Options
import os


# set webdriver path
chrome_options = Options()
chrome_options.add_argument("--headless")
# Install chrome driver with this link in Ubuntu - https://skolo.online/documents/webscrapping/#step-2-install-chromedriver
driver = webdriver.Chrome('/usr/bin/chromedriver',chrome_options=chrome_options) 
# create dir for saving files
if not os.path.exists("avg_daily_weather"):
    os.makedirs("avg_daily_weather")

if not os.path.exists("daily_hourly_weather"):
    os.makedirs("daily_hourly_weather")

# set starting the date
date1 = date(2014,1, 24) ##YYYY-MM-dd
while(date1!=(date(2014, 1, 30))) : # end date
    url="https://www.wunderground.com/history/daily/jp/tokyo/RJTT/date/"+str(date1) # get this url from wunderground https://www.wunderground.com/history, right now: tokyo city
    driver.get(url)
    tables = WebDriverWait(driver,20).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "table")))
    avg=tables[0]
    new_avg=pd.read_html(avg.get_attribute('outerHTML')) # daily avergae weather features
    new_avgFillna=new_avg[0].fillna('')
    new_avgFillna.to_csv("avg_daily_weather/"+str(date1)+".csv",index=False) # save avg weather features in folder
   
    daily=tables[1]
    new_daily=pd.read_html(daily.get_attribute('outerHTML')) # daily weather features
    new_dailyFillna=new_daily[0].fillna('')
    new_dailyFillna.to_csv("daily_hourly_weather/"+str(date1)+".csv",index=False) # save hourly weather features in folder
    date1 = date1 + timedelta(days=1)
