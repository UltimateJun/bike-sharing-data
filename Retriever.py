#!/home/cloud/environments/bike_env/bin python3
# coding: utf-8

import requests
import os
from datetime import date
from datetime import datetime
import math
from abc import ABC, abstractmethod

class StoragePath:
    # store destination path for json files
    def __init__(self):
        # default : sub-directory json
        self.dirPath = "json/"
    def getPathForToday(self):
        # get date and format it
        today = date.today()
        today_formatted = today.strftime("%Y-%m-%d")
        # get time and format it (hh-mm)
        now = datetime.now()
        now_formatted = now.strftime("%H-%M")

        # directory for json raw data: json/date/time
        # path named after timestamp
        self.dirPath = "json/" + today_formatted + "/" + now_formatted
    def createDir(self):
        # create time sub-directory (and date sub-directory if not existent)
        # and grant full permissions to folder
        try:
            os.makedirs(self.dirPath, exist_ok=True, mode=0o777)
        except IOError as error:
            print("Directory could not be created!")

class Retriever(ABC):
    @abstractmethod
    def __init__(self, storagePath: StoragePath):
        pass
    @abstractmethod
    def getURL(self):
        pass
    @abstractmethod
    def getFile(self):
        pass

class CallabikeRetriever(Retriever):
    def __init__(self, storagePath: StoragePath):
        self.storagePath = storagePath
    def getURL(self):
        self.URL = 'http://httpstat.us/550'
    def getFile(self):
        urlStorer = URLRetriever(self.URL)
        urlStorer.openURL()
        path = self.storagePath.dirPath + '/callabike-0.json'
        urlStorer.storeFile(path)

class NextbikeRetriever(Retriever):
    def __init__(self, storagePath: StoragePath):
        self.storagePath = storagePath
    def getURL(self):
        pass
    def getFile(self):
        pass

class URLRetriever:
    def __init__(self, url):
        self.url = url
    def openURL(self):
        try:
            # get requested content from URL
            resp = requests.get(self.url)
            # if HTTP status not OK: raise HTTPError exception
            resp.raise_for_status()
            self.fileWriter = JSONWriter(resp.content)
        # if HTTP error occured
        except requests.exceptions.HTTPError as error:
            # create string with error code to be written in file
            error_string = "ERROR "+str(resp.status_code)
            self.fileWriter = StringWriter(error_string)
        except requests.exceptions.ConnectionError as error:
            error_string = "Connection Error"
            self.fileWriter = StringWriter(error_string)
        except requests.exceptions.Timeout as error:
            error_string = "Timeout error"
            self.fileWriter = StringWriter(error_string)
        except requests.exceptions.RequestException as error:
            error_string = "Error occured"
            self.fileWriter = StringWriter(error_string)
    def storeFile(self, path):
        fileStorer = FileStorer(path)
        fileStorer.storeFile(self.fileWriter)

class FileWriter(ABC):
    @abstractmethod
    def __init__(self, content):
        pass
    @abstractmethod
    def writeFile(self, path):
        pass

class JSONWriter(FileWriter):
    def __init__(self, response):
        self.response = response
    # write to file with (content: bytes)
    def writeFile(self, path):
        with open(path, 'wb') as f:
            f.write(self.response)

class StringWriter(FileWriter):
    def __init__(self, string):
        self.string = string
    # write to file with string
    def writeFile(self, path):
        with open(path, 'w') as f:
            f.write(self.string)

class FileStorer:
    def __init__(self, path):
        self.path = path
    def storeFile(self, filewriter: FileWriter):
        try:
            filewriter.writeFile(self.path)
        except IOError as error:
            print("File system could not be read!")

def altlasten():
    try:
        resp = requests.get('https://api.nextbike.net/maps/nextbike-live.json?city=362')
        resp.raise_for_status()
        with open('nextbike.json', 'wb') as f:
            f.write(resp.content)
    # if HTTP error occured
    except requests.exceptions.HTTPError as error:
        # write to file with string
        with open('nextbike.json', 'w') as f:
            f.write("ERROR "+str(resp.status_code)+": ")
        # mode: append to file with bytes
        with open('nextbike.json', 'ab') as f:
            f.write(resp.content)
    except requests.exceptions.ConnectionError as error:
        print ("Error Connecting:",error)
    except requests.exceptions.Timeout as error:
        print ("Timeout Error:",error)
    except requests.exceptions.RequestException as error:
        print ("Other error:",error)
        
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

    try:
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        # get number of available bikes, divide by 100 and round up to get number of necessary requests
        requests_no = math.ceil(resp.json()['size'] / 100)
        # save first 100 bikes
    # if HTTP error occured
    except requests.exceptions.HTTPError as error:
        # write to file with string
        with open('callabike-0.json', 'w') as f:
            f.write("ERROR "+str(resp.status_code)+": ")
        # mode: append to file with bytes
        with open('callabike-0.json', 'ab') as f:
            f.write(resp.content)
    except requests.exceptions.ConnectionError as error:
        print ("Error Connecting:",error)
    except requests.exceptions.Timeout as error:
        print ("Timeout Error:",error)
    except requests.exceptions.RequestException as error:
        print ("Other error:",error)

    with open('callabike-0.json', 'wb') as f:
        f.write(resp.content)

    # start counting at 1 (first one already saved) until number of necessary requests reached
    for j in range(1, 5):# TODO: requests_no):
        # scroll through bikes in steps of 100 by incrementally increasing offset (starting with 100)
        offset = "&offset=" + str(j*100)
        # request json with given offset
        url = "https://api.deutschebahn.com/flinkster-api-ng/v1/bookingproposals?providernetwork=2" + lat + lon + radius + offset + limit
        try:
            resp = requests.get(url, headers=headers)
            resp.raise_for_status()
        # if HTTP error occured
        except requests.exceptions.HTTPError as error:
            # write error code to file with string
            with open('callabike-'+str(j)+'.json', 'w') as f:
                f.write("ERROR "+str(resp.status_code)+": ")
        except requests.exceptions.ConnectionError as error:
            # write error code to file with string
            with open('callabike-'+str(j)+'.json', 'w') as f:
                f.write("Connection error")
        except requests.exceptions.Timeout as error:
            # write error code to file with string
            with open('callabike-'+str(j)+'.json', 'w') as f:
                f.write("Timeout Error")
        except requests.exceptions.RequestException as error:
            # write error code to file with string
            with open('callabike-'+str(j)+'.json', 'w') as f:
                f.write("Other error")

        # save JSON with numbered filename in directory
        # mode: append to file with bytes
        with open('callabike-'+str(j)+'.json', 'ab') as f:
            f.write(resp.content)

def main():
    # create new StoragePath class, get path and create directory
    storagePath = StoragePath()
    storagePath.getPathForToday()
    storagePath.createDir()

    # get current nextbike JSON in Berlin and save as file in path
    callabikeRetriever = CallabikeRetriever(storagePath)
    callabikeRetriever.getURL()
    callabikeRetriever.getFile()
    nextbikeRetriever = NextbikeRetriever(storagePath)

if __name__ == "__main__":
    main()
