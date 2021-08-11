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
    # default path: sub-directory json
    def __init__(self, dirPath = "json/"):
        self.dirPath = dirPath
    def getPathForToday(self):
        # get date and format it
        today = date.today()
        today_formatted = today.strftime("%Y-%m-%d")
        # get time and format it (hh-mm)
        now = datetime.now()
        now_formatted = now.strftime("%H-%M")

        # directory for json raw data: json/date/time
        # path named after timestamp
        self.dirPath = "json/" + today_formatted + "/" + now_formatted + "/"
    def createDir(self):
        # create time sub-directory (and date sub-directory if not existent)
        # and grant full permissions to folder
        try:
            os.makedirs(self.dirPath, exist_ok=True, mode=0o777)
        except IOError as error:
            print("Directory could not be created!")

# abstract class for Retriever classes
class Retriever(ABC):
    @abstractmethod
    def __init__(self, storagePath: StoragePath):
        pass
    @abstractmethod
    def getURL(self):
        pass
    @abstractmethod
    def saveFile(self):
        pass

# eine Instanz für alle Abrufe oder eine Instanz pro Abruf?
class CallabikeRetriever(Retriever):
    def __init__(self, storagePath: StoragePath):
        self.storagePath = storagePath
        self.getFirstJSON()
        self.getAllJSON()
    def getFirstJSON(self):
        # save first file
        self.offset = "&offset=0"
        self.getURL()
        # save file with suffix 0 (callabike-0.json)
        self.saveFile(0)
        # determine number of required files (if no error occured)
        # -> round up number of available bikes divided by 100
        if self.resp != 'Error occured':
            self.file_number = math.ceil(self.resp.json()['size'] / 100)
    def getAllJSON(self):
        # save all remaining files
        for j in range(1, self.file_number):
            self.offset = "&offset=" + str(j*100)
            self.getURL()
            # get file with suffix j
            self.saveFile(j)
    def getURL(self):
        # set parameters: 10km radius around mid-Berlin, limit file to 100 entries, offset
        lat = "&lat=52.518611"
        lon = "&lon=13.408333"
        radius = "&radius=10000"
        limit = "&limit=100"
        parameters = lat + lon + radius + limit + self.offset
        self.url = "https://api.deutschebahn.com/flinkster-api-ng/v1/bookingproposals?providernetwork=2" + parameters
        # set authorization credentials in header
        self.headers = {
            'Accept': 'application/json',
            'Authorization': 'Bearer 56b6c4f18d92c4869078102e978ec8b9',
        }
    def saveFile(self, j):
        # create instance of URL retriever with URL and header
        urlStorer = URLRetriever(self.url, self.headers)
        # use urlStorer to open URL and get response
        self.resp = urlStorer.openURL()
        # generate path for new file
        path = self.storagePath.dirPath + '/callabike-'+str(j)+'.json'
        # save response to new file
        urlStorer.storeFile(path)

class NextbikeRetriever(Retriever):
    def __init__(self, storagePath: StoragePath):
        self.storagePath = storagePath
        # Methodenaufrufe hier oder in main()?
        self.getURL()
        self.saveFile()
    def getURL(self):
        # URL hier oder im Methoden-Aufruf?
        self.URL = 'https://api.nextbike.net/maps/nextbike-live.json?city=362'
    def saveFile(self):
        urlStorer = URLRetriever(self.URL)
        urlStorer.openURL()
        path = self.storagePath.dirPath + 'nextbike.json'
        urlStorer.storeFile(path)

class URLRetriever:
    # headers is an empty list if not passed
    def __init__(self, url, headers={}):
        self.url = url
        self.headers = headers
    def openURL(self):
        try:
            # get requested content from URL (passing headers)
            resp = requests.get(self.url, headers=self.headers)
            # if HTTP status not OK: raise HTTPError exception
            resp.raise_for_status()
            # create instance of JSONWriter, save reference as object attribute
            self.fileWriter = JSONWriter(resp.content)
            return resp
        # if HTTP error occured
        except requests.exceptions.HTTPError as error:
            # create string with error code to be written in file
            error_string = "ERROR "+str(resp.status_code)
            # create instance of StringWriter, save reference as object attribute
            self.fileWriter = StringWriter(error_string)
            return 'Error occured'
        except requests.exceptions.ConnectionError as error:
            error_string = "Connection Error"
            self.fileWriter = StringWriter(error_string)
            return 'Error occured'
        except requests.exceptions.Timeout as error:
            error_string = "Timeout error"
            self.fileWriter = StringWriter(error_string)
            return 'Error occured'
        except requests.exceptions.RequestException as error:
            error_string = "Error occured"
            self.fileWriter = StringWriter(error_string)
            return 'Error occured'
    def storeFile(self, path):
        # create instance of FileStorer with associated path
        fileStorer = FileStorer(path)
        # store file using fileStorer (pass fileWriter as argument)
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
    # try to write file with passed FileWriter
    def storeFile(self, filewriter: FileWriter):
        try:
            filewriter.writeFile(self.path)
        except IOError as error:
            print("File system could not be read!")

def main():
    # create new StoragePath class, get path and create directory
    storagePath = StoragePath()
    storagePath.getPathForToday()
    storagePath.createDir()

    # create Retrievers for both Call a Bike and nextbike
    callabikeRetriever = CallabikeRetriever(storagePath)
    nextbikeRetriever = NextbikeRetriever(storagePath)

if __name__ == "__main__":
    main()
