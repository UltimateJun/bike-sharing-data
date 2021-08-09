#!/home/cloud/environments/bike_env/bin python3
# coding: utf-8

# In[112]:


import requests
import os
from datetime import date
from datetime import datetime
import math

# get date and format it
today = date.today()
today_formatted = today.strftime("%Y-%m-%d")

# get time and format it (hh-mm)
now = datetime.now()
now_formatted = now.strftime("%H-%M")

# directory for json raw data: json/date/time
# path without / or else it lands up in root directory!
path = "json/" + today_formatted + "/" + now_formatted

# create sub-directory named after timestamp (and directory with date if not existent)
os.makedirs(path, exist_ok=True, mode=0o777) # grant full permissions to json folder

# get current nextbike JSON in Berlin and save as file in path
resp = requests.get('https://api.nextbike.net/maps/nextbike-live.json?city=362')
with open(path+'/nextbike.json', 'wb') as f:
    f.write(resp.content)
    
# get current Call-A-Bike JSON with 10km radius around mid-Berlin
lat = "&lat=52.518611"
lon = "&lon=13.408333"
radius = "&radius=10000"
limit = "&limit=100"
url = "https://api.deutschebahn.com/flinkster-api-ng/v1/bookingproposals?providernetwork=2" + lat + lon + radius + limit
headers = {
    'Accept': 'application/json',
    'Authorization': 'Bearer 56b6c4f18d92c4869078102e978ec8b9',
}

resp = requests.get(url, headers=headers)
# get number of available bikes, divide by 100 and round up to get number of necessary requests
requests_no = math.ceil(resp.json()['size'] / 100)
# save first 100 bikes
with open(path+'/callabike-0.json', 'wb') as f:
    f.write(resp.content)
# start counting at 1 (first one already saved) until number of necessary requests reached
for j in range(1, requests_no):
    # scroll through bikes in steps of 100 by incrementally increasing offset (starting with 100)
    offset = "&offset=" + str(j*100)
    # request json with given offset
    url = "https://api.deutschebahn.com/flinkster-api-ng/v1/bookingproposals?providernetwork=2" + lat + lon + radius + offset + limit
    resp = requests.get(url, headers=headers)
    # save JSON with numbered filename in directory
    with open(path+'/callabike-'+str(j)+'.json', 'wb') as f:
        f.write(resp.content)

