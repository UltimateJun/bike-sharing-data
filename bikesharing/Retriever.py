#!/home/cloud/environments/bike_env/bin python3
# coding: utf-8

import requests
import os
from datetime import date
from datetime import datetime
import math
from abc import ABC, abstractmethod
import sys

def main():
    # create new StoragePath class, get path and create directory
    storagePath = StoragePath()

    # create Retrievers for both Call a Bike and nextbike
    successWriter = JSONWriter(storagePath)
    errorWriter = StringWriter(storagePath)
    urlRetriever = URLRetriever(successWriter, errorWriter)
    CallabikeRetriever(urlRetriever)
    NextbikeRetriever(urlRetriever)

class StoragePath:
    # store destination path for json files
    # default path: sub-directory json
    def __init__(self, rootPath = "json/"):
        self.rootPath = rootPath
    def getPathForCurrentTime(self):
        # get date and format it
        today = date.today()
        today_formatted = today.strftime("%Y-%m-%d")
        # get time and format it (hh-mm)
        now = datetime.now()
        now_formatted = now.strftime("%H-%M")

        # directory for json raw data: json/date/time
        # path named after timestamp
        return self.rootPath + today_formatted + "/" + now_formatted + "/"
    def createDir(self, path):
        # create time sub-directory (and date sub-directory if not existent)
        # and grant full permissions to folder
        try:
            os.makedirs(path, exist_ok=True, mode=0o777)
        except IOError as error:
            # print exception to standard error if directory could not be created
            print("Directory could not be created! " + str(error), file=sys.stderr)

class URLRetriever:
    # headers is an empty list if not passed
    def __init__(self, successWriter, errorWriter):
        self.successWriter = successWriter
        self.errorWriter = errorWriter
    def retrieveURL(self, fileName, url, headers={}):
        try:
            # get requested content from URL (passing headers)
            resp = requests.get(url, headers=headers)
            # if HTTP status not OK: raise HTTPError exception
            resp.raise_for_status()
            # create instance of JSONWriter, save reference as object attribute
            self.successWriter.createDirAndFile(resp.content, fileName)
            return resp # returns response
        # if HTTP error occured
        except requests.exceptions.HTTPError as error:
            # create string with error code to be written in file
            error_string = "ERROR "+str(resp.status_code)
            self.errorWriter.createDirAndFile(error_string, fileName)
            return None # returns no response (because error occured)
        except requests.exceptions.ConnectionError as error:
            error_string = "Connection Error"
            self.errorWriter.createDirAndFile(error_string, fileName)
            return None
        except requests.exceptions.Timeout as error:
            error_string = "Timeout error"
            self.errorWriter.createDirAndFile(error_string, fileName)
            return None
        except requests.exceptions.RequestException as error:
            error_string = "Error occured"
            self.errorWriter.createDirAndFile(error_string, fileName)
            return None

# abstract class for Retriever classes
class Retriever(ABC):
    @abstractmethod
    def __init__(self, urlRetriever: URLRetriever):
        pass
    @abstractmethod
    def getURL(self):
        pass
    @abstractmethod
    def saveFile(self):
        pass

class CallabikeRetriever(Retriever):
    def __init__(self, urlRetriever: URLRetriever):
        self.urlRetriever = urlRetriever
        self.getFirstJSON()
        self.getAllJSON()
    def getFirstJSON(self):
        # save first file
        self.offset = "&offset=0"
        url, headers = self.getURL()
        # save file with suffix 0 (callabike-0.json)
        self.saveFile(0, url, headers)
        # determine number of required files (if reponse exists, i.e. no error occured)
        # -> round up number of available bikes divided by 100
        if self.response:
            self.file_number = math.ceil(self.response.json()['size'] / 100)
    def getAllJSON(self):
        # save all remaining files
        for j in range(1, self.file_number):
            self.offset = "&offset=" + str(j*100)
            url, headers = self.getURL()
            # get file with suffix j
            self.saveFile(j, url, headers)
    def getURL(self):
        # set parameters: 10km radius around mid-Berlin, limit file to 100 entries, offset
        lat = "&lat=52.518611"
        lon = "&lon=13.408333"
        radius = "&radius=10000"
        limit = "&limit=100"
        parameters = lat + lon + radius + limit + self.offset
        url = "https://api.deutschebahn.com/flinkster-api-ng/v1/bookingproposals?providernetwork=2" + parameters
        # set authorization credentials in header
        headers = {
            'Accept': 'application/json',
            'Authorization': 'Bearer 56b6c4f18d92c4869078102e978ec8b9',
        }
        return url, headers
    def saveFile(self, j, url, headers):
        # use urlRetriever to open URL and get response
        fileName = 'callabike-'+str(j)+'.json'
        self.response = self.urlRetriever.retrieveURL(fileName, url, headers)
        # save response to new file
        # urlRetriever.storeFile(path)

class NextbikeRetriever(Retriever):
    def __init__(self, urlRetriever: URLRetriever):
        self.urlRetriever = urlRetriever
        url = self.getURL()
        self.saveFile(url)
    def getURL(self):
        url = 'https://api.nextbike.net/maps/nextbike-live.json?city=362'
        return url
    def saveFile(self, url):
        fileName = 'nextbike.json'
        self.urlRetriever.retrieveURL(fileName, url)
        # urlRetriever.storeFile(path)

class FileWriter(ABC):
    def __init__(self,  storagePath: StoragePath):
        self.storagePath = storagePath

    def createDirAndFile(self, content, fileName):
        try:
            # create sub-directory of current time
            self.storagePath.createDir(self.storagePath.getPathForCurrentTime())
            self.writeFile(content, fileName)
        except IOError as error:
            print("File system could not be read! " + str(error), file=sys.stderr)
            
    @abstractmethod
    def writeFile(self, content, fileName):
        pass

class JSONWriter(FileWriter):
    # write to file with (content: bytes)
    def writeFile(self, response, fileName):
        with open(self.storagePath.getPathForCurrentTime() + fileName, 'wb') as f:
            f.write(response)

class StringWriter(FileWriter):
    # write to file with string
    def writeFile(self, string, fileName):
        with open(self.storagePath.getPathForCurrentTime() + fileName, 'w') as f:
            f.write(string)

if __name__ == "__main__":
    main()
