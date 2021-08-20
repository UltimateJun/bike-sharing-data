from mysql.connector import connect, Error
import os, os.path, datetime, json
import requests
from json.decoder import JSONDecodeError
import pprint  
import math
from abc import ABC, abstractmethod
from dataclasses import dataclass
import sys

def main():
    # set path in parameters
    parameters = Parameters("json/")

    # instantiate database manangers (SQL connection)
    callabikeDataManager = CallabikeDataManager()
    # get dates of last status (= last parsing)
    callabikeLastStatusDate = callabikeDataManager.getLastStatusDate()
    # set start dates to the day after last parsing
    callabikeDataManager.setStartDate(callabikeLastStatusDate)
    # get end dates from terminal input
    callabikeEndDay = InputGetter().getInputFromTerminal(callabikeLastStatusDate, "Call A Bike")
    callabikeDataManager.setEndDate(callabikeEndDay)
    
    nextbikeDataManager = NextbikeDataManager()
    nextbikeLastStatusDate = nextbikeDataManager.getLastStatusDate()
    nextbikeDataManager.setStartDate(nextbikeLastStatusDate)
    nextbikeEndDay = InputGetter().getInputFromTerminal(nextbikeLastStatusDate, "Nextbike")
    nextbikeDataManager.setEndDate(nextbikeEndDay)

    # instantiate process managers (handle entire for each API)
    callabikeManager = CallabikeProcessManager(parameters, callabikeDataManager)
    # process all files of a provider and insert into database
    callabikeManager.executeProcess()
    nextbikeManager = NextbikeProcessManager(parameters, nextbikeDataManager)
    nextbikeManager.executeProcess()

class DataManager(ABC):
    def __init__(self):
        try:
            # create MySQL connection and cursor
            self.connection = connect(option_files='mysql.conf')
            self.connection.autocommit = True
            self.cursor = self.connection.cursor(named_tuple=True)
        except (Error) as e:
            # print exception to standard error if error occured while connecting to database
            print("Database connection could not be established: " + str(e), file=sys.stderr)
        
        # set up pretty printer with indentation
        self.pp = pprint.PrettyPrinter(indent=4)

        self.setProvider()
        self.createAllRidesInstance()
        self.createDataClasses()
    @abstractmethod
    def setProvider(self):
        # set provider of data manager in subclass
        pass
    @abstractmethod
    def createAllRidesInstance(self):
        # create instance in sub-class to contain all ride instances
        pass
    def createDataClasses(self):
        # create data structures (and aggregators of data classes instances) as attributes
        self.allStatus = AllStatus()
        self.newStations = NewStations()
        self.stationsIDSet = self.getStationIDs()
        self.exceptions = APIExceptions()
        self.newBikes = []
    def getLastStatusDate(self):
        # get date of the last status from provider
        self.cursor.execute("SELECT DATE(MAX(since)) AS date FROM bike_last_status NATURAL JOIN bike WHERE provider = '" + self.provider + "'")
        # set date to result (None if no entry found)
        lastStatusDate = self.cursor.fetchone().date
        # if no entry found: set last status to June 3rd 2021 (one day before data collection began at June 4th)
        lastStatusDate = datetime.date(2021, 6, 3) if not lastStatusDate else lastStatusDate
        return lastStatusDate
    def setStartDate(self, lastStatusDate):
        # start date of parsing: one day after date of last status
        startDate = lastStatusDate + datetime.timedelta(days=1)
        self.parseYear = startDate.year
        self.parseMonth = startDate.month
        self.startDay = startDate.day
    def setEndDate(self, endDay):
        self.endDay = endDay
    def getLastStatus(self):
        # get last status of all bikes from provider
        self.cursor.execute("SELECT bike_id, ST_Y(coordinates) AS lat, ST_X(coordinates) AS lon, station_id, since FROM bike_last_status NATURAL JOIN bike WHERE provider='" + self.provider + "'")
        # go through all retrieved status
        for bikeLastStatus in self.cursor:
            bikeStatus = Status()
            bikeStatus.bike_id = str(bikeLastStatus.bike_id)
            # get longitude (Y-coordinate) and latitude (X-coordinate)
            bikeStatus.lat = str(bikeLastStatus.lat)
            bikeStatus.lon = str(bikeLastStatus.lon)
            # if not at a station: set to None (would otherwise result in string 'None')
            bikeStatus.station_id = str(bikeLastStatus.station_id) if bikeLastStatus.station_id is not None else None
            bikeStatus.since = bikeLastStatus.since
            # add status instance to AllStatus instance
            self.allStatus.add(bikeStatus)
        # return AllStatus instance containing references to all status instances
        return self.allStatus
    def getStationIDs(self):
        # get IDs of all existing stations and save in new set
        self.cursor.execute("SELECT station_id FROM station WHERE provider='" + self.provider + "'")
        # set of all station IDs (no dataclass as it is only one single attribute)
        stationsIDSet = {str(station[0]) for station in self.cursor.fetchall()}
        return stationsIDSet
    def insertStations(self):
        newStationsList = [(station.station_id, station.coordinates, station.name, station.provider, station.station_capacity) for station in self.newStations.stationsList]
        self.cursor.executemany("INSERT INTO station (station_id, coordinates, name, provider, capacity) VALUES (%s,  ST_GeomFromText(%s, 4326), %s, %s, %s) ON DUPLICATE KEY UPDATE station_id=station_id", newStationsList)
        print('Inserting new stations done at ' + datetime.datetime.now().strftime("%H:%M:%S"))
    def insertBikes(self):
        newBikesList = [(newBike, self.provider) for newBike in self.newBikes]
        # print('New bikes:')
        # self.pp.pprint(newBikesList)
        self.cursor.executemany("INSERT INTO bike (bike_id, provider) VALUES (%s, %s) ON DUPLICATE KEY UPDATE bike_id=bike_id", newBikesList)
        print('Inserting bikes done at ' + datetime.datetime.now().strftime("%H:%M:%S"))
    def insertBikeRides(self):
        bikeRidesList = []
        for ride in self.allRides.ridesList:
            start_coordinates = 'POINT(' + ride.start_lat + ' ' + ride.start_lon + ')'
            end_coordinates = 'POINT(' + ride.end_lat + ' ' + ride.end_lon + ')'
            bikeRidesList.append((ride.bike_id, start_coordinates, end_coordinates, ride.start_station, ride.end_station, ride.since, ride.until))
        # insert bike ride list into bike_ride table
        # ON DUPLICATE KEY UPDATE bike_id=bike_id: ignore insert if combination of bike_id and since exists already!
        # ensures idempotence as rides cannot be duplicated
        self.cursor.executemany("INSERT INTO bike_ride (bike_id, start_coordinates, end_coordinates, start_station_id, end_station_id, since, until) VALUES (%s, ST_GeomFromText(%s, 4326), ST_GeomFromText(%s, 4326), %s, %s, %s, %s) ON DUPLICATE KEY UPDATE bike_id=bike_id", bikeRidesList)
        print('Inserting bike rides done at ' + datetime.datetime.now().strftime("%H:%M:%S"))
    def insertLastStatus(self):
        lastBikeStatusList = []
        for bikeStatus in self.allStatus.statusDict.values():
            bikeStatusCoordinates =  'POINT(' + bikeStatus.lat + ' ' + bikeStatus.lon + ')'
            lastBikeStatusList.append((bikeStatus.bike_id, bikeStatusCoordinates, bikeStatus.station_id, bikeStatus.since))
        # overwrite (REPLACE) is not supported by executemany -> sends one query per entry instead,
        # self.cursor.executemany("REPLACE INTO bike_last_status (bike_id, coordinates, station_id, since) values (%s, ST_GeomFromText(%s, 4326), %s, %s)", lastBikeStatusList)
        # bike_last_status table with new status list
        self.cursor.execute("DELETE FROM bike_last_status WHERE bike_id IN (SELECT bike_id FROM bike WHERE provider = '" + self.provider + "')")
        self.cursor.executemany("INSERT INTO bike_last_status (bike_id, coordinates, station_id, since) values (%s, ST_GeomFromText(%s, 4326), %s, %s)", lastBikeStatusList)
        print('Inserting last bike status done at ' + datetime.datetime.now().strftime("%H:%M:%S"))
    def insertExceptions(self):
        exceptionList = []
        for apiException in self.exceptions.exceptionsDict.values():
            exceptionList.append((self.provider, apiException.error_type, apiException.timestamp_mysql))
        self.cursor.executemany("INSERT INTO exception (provider, error_type, time) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE exception_id=exception_id", exceptionList)
        print('Exceptions:')
        self.pp.pprint(exceptionList)
        print('Inserting exceptions done at ' + datetime.datetime.now().strftime("%H:%M:%S"))
    def close(self):
        # close the MySQL connection
        self.connection.close()

class CallabikeDataManager(DataManager):
    def setProvider(self):
        # set provider as object attribute
        self.provider = "callabike"
    def createAllRidesInstance(self):
        # create data structures (and aggregators of data classes instances) as attributes
        self.allRides = CallabikeAllRides()

class NextbikeDataManager(DataManager):
    def setProvider(self):
        # set provider as object attribute
        self.provider = "nextbike"
    def createAllRidesInstance(self):
        # create data structures (and aggregators of data classes instances) as attributes
        self.allRides = NextbikeAllRides()

@dataclass(init=True)
class Parameters:
    # set rootPath of JSON files
    rootPath: str

class InputGetter:
    def getInputFromTerminal(self, lastStatusDate, provider):
        # print last status date for provider and request end date
        parsingStartDate = lastStatusDate + datetime.timedelta(days=1)
        print("The previous parsing for "+provider+" ended on " + str(lastStatusDate) + ".")
        print("The current parsing for "+provider+" will start on " + str(parsingStartDate)+".")
        print("Please enter end day of parsing for "+provider+": ")
        try:
            endDay = int(input())
            # check if entered day is part of a valid date
            endDate = parsingStartDate.replace(day = endDay)
            if endDate >= datetime.date.today():
                # if entered day is today or later, print error message and restart input
                print("Please enter a day in the past! Try again:")
                return self.getInputFromTerminal(lastStatusDate, provider)
        except ValueError:
            # if invalid value is entered, print error message and restart input
            print("Please enter a valid day! Try again:")
            return self.getInputFromTerminal(lastStatusDate, provider)
        # return input end day
        return endDay

class ProcessManager(ABC):
    def __init__(self, parameters: Parameters, dataManager: DataManager):
        # set passed parameters as attributes
        self.parameters = parameters
        self.dataManager = dataManager
    @abstractmethod
    def executeProcess(self):
        pass
    def insertIntoDatabase(self):
        self.dataManager.insertStations()
        self.dataManager.insertBikes()
        self.dataManager.insertBikeRides()
        self.dataManager.insertLastStatus()
        self.dataManager.insertExceptions()

class CallabikeProcessManager(ProcessManager):
    def executeProcess(self):
        lastAllStatus = self.dataManager.getLastStatus()

        callabikeStationsParser = CallabikeStationsParser(self.dataManager)
        callabikeStationsParser.getNewStations(self.parameters)
        callabikeStationsParser.parseNewStations(self.parameters)

        allFilesParser = CallabikeAllFilesParser(self.dataManager)
        allFilesParser.parseAllFiles(self.parameters, lastAllStatus)
        self.insertIntoDatabase()

class NextbikeProcessManager(ProcessManager):
    def executeProcess(self):
        lastAllStatus = self.dataManager.getLastStatus()
        allFilesParser = NextbikeAllFilesParser(self.dataManager)
        allFilesParser.parseAllFiles(self.parameters, lastAllStatus)
        self.insertIntoDatabase()

@dataclass(init=False)
class Status():
    bike_id: str
    lon: str
    lat: str
    station_id: str
    since: datetime

class AllStatus:
    def __init__(self):
        self.statusDict = {}
    def add(self, status):
        self.statusDict[status.bike_id] = status

class AllFilesParser(ABC):
    @abstractmethod
    def __init__(self, dataManager: DataManager):
        # set associated data manager in sub-class
        pass
    @abstractmethod
    def parseFilesOfAMinute(self, dirPath, lastAllStatus, datetime_current):
        pass
    def parseAllFiles(self, parameters, lastAllStatus):
        # get start and end date from data manager
        startDay = self.dataManager.startDay
        parseMonth = self.dataManager.parseMonth
        parseYear = self.dataManager.parseYear
        endDay = self.dataManager.endDay
        # go through all days from start day to end day
        for day in range(startDay, endDay+1):
            # get date of parse by joining year, month (from parameters) and day (from loop)
            dateOfParse = f'{parseYear:04d}' + '-' + f'{parseMonth:02d}' + '-' + f'{day:02d}'
            # informative terminal output: start of parsing of a day
            print(dateOfParse + ' started at ' + datetime.datetime.now().strftime("%H:%M:%S"))
            # for all minutes of that date
            for hour in range(0,24):
                for minute in range(0,60):
                    # format date and time strings (YYYY-MM-DD and HH-MM), append to directory path
                    time_hhmm = f'{hour:02d}' + '-' + f'{minute:02d}'
                    dirPath = parameters.rootPath + dateOfParse + '/' + time_hhmm;
                    # timestamp of batch of files currently being processed, and timestamp of previous minute
                    datetime_current = datetime.datetime.strptime(dateOfParse + '-' +  time_hhmm, '%Y-%m-%d-%H-%M')
                    # if given directory exists
                    if os.path.exists(dirPath):
                        # go through numbers 0-18
                        # for callabikeJSONNumber in range(0,1):
                        try:
                            # parse the files of the given minute
                            self.parseFilesOfAMinute(dirPath, lastAllStatus, datetime_current)
                        # if error occured while reading file: remember exception for database insert
                        except ValueError as e:
                            self.exceptionHandler.handleAPIException(self.dataManager, lastAllStatus, datetime_current, "ValueError")
                        except KeyError as e:
                            self.exceptionHandler.handleAPIException(self.dataManager, lastAllStatus, datetime_current, "KeyError")
                        except TypeError as e:
                            self.exceptionHandler.handleAPIException(self.dataManager, lastAllStatus, datetime_current, "TypeError")
                        except IndexError as e:
                            self.exceptionHandler.handleAPIException(self.dataManager, lastAllStatus, datetime_current, "IndexError")
                        except Error as e:
                            self.exceptionHandler.handleAPIException(self.dataManager, lastAllStatus, datetime_current, "Error")
    def handleJSONDecodeError(self, path, lastAllStatus, datetime_current):
        # default error: JSONDecodeError
        errorCode = "JSONDecodeError"
        # if not a JSON file: read ERROR code from file
        try:
            with open(path) as errorJSON:
                # if ERROR code is present in file: overwrite errorCode
                jsonContent = errorJSON.read()
                if jsonContent.startswith("ERROR"):
                    errorCode = jsonContent
        except IOError as error:
            print("File system could not be read! " + str(error), file=sys.stderr)
        # handle exception with found error code
        self.exceptionHandler.handleAPIException(self.dataManager, lastAllStatus, datetime_current, errorCode)

class CallabikeAllFilesParser(AllFilesParser):
    def __init__(self, dataManager: DataManager):
        self.exceptionHandler = CallabikeExceptionHandler()
        self.dataManager = dataManager
        self.fileParser = CallabikeFileParser(dataManager)
    def parseFilesOfAMinute(self, dirPath, lastAllStatus, datetime_current):
        firstJSONPath = dirPath+'/callabike-0.json'
        if os.path.exists(firstJSONPath):
            try:
                # get cut-off positions and numbers of bikes from first file
                # cut-off positions are updated every minute this way (in case it suddenly changes)
                bikeNumber = self.fileParser.parseFirstFile(firstJSONPath)
            except JSONDecodeError as e:
                # if not a JSON file: try to read error code in file, remember exception for database insert
                self.handleJSONDecodeError(firstJSONPath, lastAllStatus, datetime_current)
                return

            # calculate expected number of files (round up number of bikes divided by 100)
            # e.g. 16 files expected if there are 1550 bikes
            numberOfFiles = math.ceil(bikeNumber/100)

            for callabikeJSONNumber in range(0, numberOfFiles):
                # if call-a-bike JSON with given number exists
                jsonPath = dirPath+'/callabike-'+str(callabikeJSONNumber)+'.json'
                if os.path.exists(jsonPath):
                    # parse bike rides and new status from given file
                    try:
                        lastAllStatus = self.fileParser.parseFile(jsonPath, lastAllStatus, datetime_current)
                    except JSONDecodeError as e:
                        # if not a JSON file: try to read error code in file, remember exception for database insert
                        print(jsonPath)
                        self.handleJSONDecodeError(jsonPath, lastAllStatus, datetime_current)
                
                # if the last file should exist given the number of bikes
                # e.g. callabike-15 is missing even though number of bikes is 1550
                # -> check if missing json number is 15 (1550 rounded down)
                elif callabikeJSONNumber == math.floor(self.fileParser.bike_number/100):
                    self.exceptionHandler.handleLastFileMissingException(lastAllStatus, datetime_current)
                    break
                # if more than one file is missing: set timestamp to current time for all previously available bikes
                elif callabikeJSONNumber < math.floor(self.fileParser.bike_number/100):
                    self.exceptionHandler.handleAPIException(self.dataManager, lastAllStatus, datetime_current, "SeveralFilesMissing")
                    break
        # if callabike-0 does not exist in directory -> API exception!
        else:
            self.exceptionHandler.handleAPIException(self.dataManager, lastAllStatus, datetime_current, "AllFilesMissing")

class NextbikeAllFilesParser(AllFilesParser):
    def __init__(self, dataManager: DataManager):
        self.dataManager = dataManager
        self.fileParser = NextbikeFileParser(dataManager)
        self.exceptionHandler = NextbikeExceptionHandler()
    def parseFilesOfAMinute(self, dirPath, lastAllStatus, datetime_current):
        jsonPath = dirPath+'/nextbike.json'
        if os.path.exists(jsonPath):
            # parse bike rides and new status from given file
            try:
                lastAllStatus = self.fileParser.parseFile(jsonPath, lastAllStatus, datetime_current)
            except JSONDecodeError as e:
                # if not a JSON file: try to read error code in file, remember exception for database insert
                self.handleJSONDecodeError(jsonPath, lastAllStatus, datetime_current)
        # if nextbike.json does not exist in directory -> API exception!
        else:
            self.exceptionHandler.handleAPIException(self.dataManager, lastAllStatus, datetime_current, "FileMissing")

class FileParser(ABC):
    def __init__(self, dataManager: DataManager):
        self.dataManager = dataManager
    @abstractmethod
    def parseFile(self, path, lastAllStatus: AllStatus, datetime_current):
        pass

class CallabikeFileParser(FileParser):
    # define attributes for cutoff-positions in href for bike and station ID
    left_cut_bike: int
    right_cut_bike: int
    left_cut_station: int
    right_cut_station: int
    bike_number: int

    def parseFirstFile(self, path):
        # set all relevant values as attributes of the parser
        # open first JSON and write subarray 'items' to array bikes
        try:
            with open(path) as bike_json:
                jsonObject = json.load(bike_json)
        except IOError as error:
            print("File system could not be read! " + str(error), file=sys.stderr)
        bikes = jsonObject['items']

        # take rentalObject href of first bike
        href_sample_bike = str(bikes[0]['rentalObject'])
        # find out beginning and end position of bike ID within that href
        self.left_cut_bike = href_sample_bike.rfind("rentalobjects/")+14
        self.right_cut_bike = href_sample_bike.rfind("'}'")-1

        # take area href of first bike
        href_sample_station = str(bikes[0]['area'])
        # find out beginning and end position of station ID within that href
        self.left_cut_station = href_sample_station.rfind("areas/")+6
        self.right_cut_station = href_sample_station.rfind("'}'")-1

        # get number of bikes available for this minute
        self.bike_number = jsonObject['size']

        return self.bike_number
    def parseFile(self, path, lastAllStatus: AllStatus, datetime_current):
        # open JSON and write subarray 'items' to array bikes
        try:    
            with open(path) as bike_json:
                jsonObject = json.load(bike_json)
        except IOError as error:
            print("File system could not be read! " + str(error), file=sys.stderr)
        bikes = jsonObject['items']
        
        # datetime of the previous minute: current time minus one minute
        datetime_last_minute = datetime_current - datetime.timedelta(minutes=1)

        for bike in bikes:
            bike_href = str(bike['rentalObject'])
            # cut bike ID out of rentalobject href
            bike_id = bike_href[:self.right_cut_bike][self.left_cut_bike:]

            station_href = str(bike['area'])
            # if area is 65D863257FDF847B1F2807E7E346B83F8C752E3F (Stadtgebiet Berlin), indicating a free-floating bike
            # comparing whole href does NOT work, as the URL changes over time!
            # if station_href == "{'href': 'https://api.deutschebahn.com:80/apiv4/v1/areas/65D863257FDF847B1F2807E7E346B83F8C752E3F'}":
            if '65D863257FDF847B1F2807E7E346B83F8C752E3F' in station_href:
                station_id = None
            else:
                # if bike is at an actual station: cut station ID out of area href
                station_id = station_href[:self.right_cut_station][self.left_cut_station:]
            
            coordinates = str(bike['position']['coordinates'])
            coordinates_comma_position = coordinates.index(", ")
            lat = coordinates[coordinates_comma_position+2:-1]
            lon = coordinates[1:coordinates_comma_position]

            # if bike doesn't exist in database yet
            if bike_id not in lastAllStatus.statusDict.keys():
                # add to set of new bikes
                self.dataManager.newBikes.append(bike_id)
            # else (= exists) if "since" timestamp of current status is NOT equal to the previous minute 
            # => bike was not previously available and therefore gone
            # AND not equal to current minute -> work-around for Call-A-Bike exception with multiple entries of same bike within same minute
            else:
                bikeLastStatusTime = lastAllStatus.statusDict[bike_id].since
                if (bikeLastStatusTime != datetime_last_minute and bikeLastStatusTime != datetime_current):
                    # create new BikeRide instance and set end of ride parameters
                    # current coordinates / station / last datetime will be end coordinates / station / datetime of ride
                    bikeRide = BikeRide()
                    bikeRide.bike_id = bike_id
                    bikeRide.end_lat = lat
                    bikeRide.end_lon = lon
                    bikeRide.end_station = station_id
                    # bike ride until the minute before it became available again
                    bikeRide.until = datetime_current - datetime.timedelta(minutes=1)
                    # current coordinates / station / datetime will be end coordinates / station / datetime of ride
                    self.dataManager.allRides.addBikeRide(bikeRide, lastAllStatus)
            # update new available status in allStatus instance (or create new status if not yet existent)
            newStatus = Status()
            newStatus.bike_id = bike_id
            newStatus.lon = lon
            newStatus.lat = lat
            newStatus.station_id = station_id
            newStatus.since = datetime_current
            lastAllStatus.add(newStatus)
        return lastAllStatus

class NextbikeFileParser(FileParser):
    def parseFile(self, path, lastAllStatus: AllStatus, datetime_current):
        # open JSON and write subarray 'places' to array bikes
        try:
            with open(path) as bike_json:
                jsonObject = json.load(bike_json)
        except IOError as error:
            print("File system could not be read! " + str(error), file=sys.stderr)
        places = jsonObject['countries'][0]['cities'][0]['places']

        # datetime of the previous minute: current time minus one minute
        datetime_last_minute = datetime_current - datetime.timedelta(minutes=1)

        for place in places:
            # get coordinates of the place
            lat = str(place['lat'])
            lon = str(place['lng'])

            # if flag "spot" is set to true -> place is a station, not a free-floating bike
            if place['spot']:
                station_id = str(place['uid'])
                # if station not yet in database
                if station_id not in self.dataManager.stationsIDSet:
                    # retrieve station data and add to new station instance
                    # Format for MySQL ST_GeomFromText and SRID 4326: Point(52.53153 13.38651) -> in reverse order!
                    newStation = Station()
                    newStation.station_id = station_id
                    newStation.coordinates = 'Point(' + lat + ' ' + lon + ')'
                    newStation.name = place['name']
                    newStation.provider = 'nextbike'
                    newStation.station_capacity = str(place['bike_racks'])
                    # add new station to newStations instance
                    self.dataManager.newStations.add(newStation)
                    self.dataManager.stationsIDSet.add(station_id)
            # otherwise, set station of free-floating bike to None
            else:
                station_id = None

            # go through all bikes in bike_list
            for bike in place['bike_list']:
                # get bike ID
                bike_id = str(bike['number'])

                # if bike doesn't exist in database yet
                if bike_id not in lastAllStatus.statusDict.keys():
                    # add bike in a tuple (, required) to list of new bikes
                    self.dataManager.newBikes.append(bike_id)
                # else (= exists) if "since" timestamp of current status is NOT equal to the previous minute 
                # => bike was not previously available and therefore gone
                else:
                    bikeLastStatusTime =  lastAllStatus.statusDict[bike_id].since
                    if bikeLastStatusTime != datetime_last_minute:
                        # create new BikeRide instance and set end of ride parameters
                        # current coordinates / station / datetime will be end coordinates / station / datetime of ride
                        bikeRide = BikeRide()
                        bikeRide.bike_id = bike_id
                        bikeRide.end_lat = lat
                        bikeRide.end_lon = lon
                        bikeRide.end_station = station_id
                        # bike ride until the minute before it became available again
                        bikeRide.until = datetime_current - datetime.timedelta(minutes=1)
                        # current coordinates / station / datetime will be end coordinates / station / datetime of ride
                        self.dataManager.allRides.addBikeRide(bikeRide, lastAllStatus)
                # update new available status in allStatus instance (or create new status if not yet existent)
                newStatus = Status()
                newStatus.bike_id = bike_id
                newStatus.lon = lon
                newStatus.lat = lat
                newStatus.station_id = station_id
                newStatus.since = datetime_current
                lastAllStatus.add(newStatus)
        return lastAllStatus

class CallabikeStationsParser:
    def __init__(self, dataManager: DataManager):
        self.dataManager = dataManager
    def getNewStations(self, parameters):
        # get current Call-A-Bike station JSON with 10km radius around mid-Berlin
        lat = "&lat=52.518611"
        lon = "&lon=13.408333"
        radius = "&radius=10000"
        limit = "&limit=100"
        url = "https://api.deutschebahn.com/flinkster-api-ng/v1/areas?providernetwork=2" + lat + lon + radius + limit
        headers = {
            'Accept': 'application/json',
            'Authorization': 'Bearer 56b6c4f18d92c4869078102e978ec8b9',
        }
        resp = requests.get(url, headers=headers)

        # get number of available stations, divide by 100 and round up to get number of necessary requests
        self.requests_no = math.ceil(resp.json()['size'] / 100)
        # directory path for station files
        rootPath = parameters.rootPath
        dirPath = rootPath + 'Stations'
        # save first 100 stations
        try:
            with open(dirPath+'/callabike_station_0.json', 'wb') as f:
                f.write(resp.content)
        except IOError as error:
            print("File system could not be accessed! " + str(error), file=sys.stderr)
        # start counting at 1 (first one already saved) until number of necessary requests reached
        for i in range(1, self.requests_no):
            # scroll through stations in steps of 100 by incrementally increasing offset (starting with 100)
            offset = "&offset=" + str(i*100)
            # request json with given offset
            url = "https://api.deutschebahn.com/flinkster-api-ng/v1/areas?providernetwork=2" + lat + lon + radius + offset + limit
            resp = requests.get(url, headers=headers)
            # save JSON with numbered filename in directory
            with open(dirPath+'/callabike_station_'+str(i)+'.json', 'wb') as f:
                f.write(resp.content)
        # return number of necessary requests
    def parseNewStations(self, parameters):
        # directory path for station files
        rootPath = parameters.rootPath
        dirPath = rootPath + 'Stations'
        # go through all gathered files
        for j in range(0, self.requests_no):
            try:
                # get stations of file
                with open(dirPath+'/callabike_station_'+str(j)+'.json') as station_json:
                    jsonObject = json.load(station_json)
                    station_json.close()
                stations = jsonObject['items']
                # for every station 
                for station in stations:
                    station_id =  station['uid']
                    # if station doesn't exist in database yet
                    if station_id not in self.dataManager.stationsIDSet:
                        # retrieve station data and add to new station instance
                        newStation = Station()
                        newStation.station_id = station_id
                        station_latitude = str(station['geometry']['centroid']['coordinates'][1])
                        station_longitude = str(station['geometry']['centroid']['coordinates'][0])
                        # Format for MySQL ST_GeomFromText and SRID 4326: Point(52.53153 13.38651) -> in reverse order!
                        newStation.coordinates = 'Point(' + station_latitude + ' ' + station_longitude + ')'
                        newStation.name = station['name']
                        newStation.provider =  'callabike'
                        # no station_capacity (as not provided by Call-A-Bike)
                        self.dataManager.newStations.add(newStation)
                        self.dataManager.stationsIDSet.add(station_id)
            except IOError as error:
                print("File system could not be read! " + str(error), file=sys.stderr)

@dataclass(init=False)
class BikeRide:
    bike_id: str
    start_lat: float
    start_lon: float
    end_lat: float
    end_lon: float
    start_station: str
    end_station: str
    since: datetime
    until: datetime

# class to handle all bike rides (storing, and analyzing)
class AllRides(ABC):
    def __init__(self):
        self.ridesList = []
    def add(self, bikeRide):
        self.ridesList.append(bikeRide)
    @abstractmethod
    def addBikeRide(self, bikeRide: BikeRide, lastAllStatus: AllStatus):
        pass

class CallabikeAllRides(AllRides):
    def addBikeRide(self, bikeRide: BikeRide, lastAllStatus: AllStatus):
        bike_id = bikeRide.bike_id

        # get last saved coordinates and station from status (will be start coordinates and station)
        bikeRide.start_lat = lastAllStatus.statusDict[bike_id].lat
        bikeRide.start_lon = lastAllStatus.statusDict[bike_id].lon
        bikeRide.start_station = lastAllStatus.statusDict[bike_id].station_id

        # bike gone since: the last time it was available
        bikeRide.since = lastAllStatus.statusDict[bike_id].since

        # handle "bike taken at the moment of file retrieval" exception:
        # skip rides with identical start / end coordinates AND a duration of one minute
        isBikeTakenAtMomentOfFileRetrieval = (bikeRide.start_lat == bikeRide.end_lat and bikeRide.start_lon == bikeRide.end_lon) and bikeRide.since+datetime.timedelta(minutes=1)==bikeRide.until
        if not (isBikeTakenAtMomentOfFileRetrieval):
            # write ride details (+ timestamps) to list
            self.add(bikeRide)

class NextbikeAllRides(AllRides):
    def addBikeRide(self, bikeRide: BikeRide, lastAllStatus: AllStatus):
        bike_id = bikeRide.bike_id

        # get ride details (start coordinates, start station)
        bikeRide.start_lat = lastAllStatus.statusDict[bike_id].lat
        bikeRide.start_lon = lastAllStatus.statusDict[bike_id].lon
        bikeRide.start_station = lastAllStatus.statusDict[bike_id].station_id
        # bike gone since: the last time it was available
        bikeRide.since = lastAllStatus.statusDict[bike_id].since

        # write ride details (+ timestamps) to list
        self.add(bikeRide)

@dataclass(init=False)
class Station:
    station_id: str
    name: str
    coordinates: str
    provider: str
    station_capacity: str = None

class NewStations:
    def __init__(self):
        self.stationsList = []
    def add(self, station):
        self.stationsList.append(station)

@dataclass
class APIException:
    timestamp_mysql: str
    error_type: str

class APIExceptions:
    def __init__(self):
        self.exceptionsDict = {}
    def add(self, exception):
        self.exceptionsDict[exception.timestamp_mysql]=exception

class APIExceptionHandler(ABC):
    def handleAPIException(self, dataManager: DataManager, allStatus: AllStatus, datetime_current, error_type):
        # datetime of the previous minute: current time minus one minute
        datetime_last_minute = datetime_current - datetime.timedelta(minutes=1)
        # convert current datetime to MySQL format
        timestamp_mysql = datetime_current.strftime('%Y-%m-%d %H:%M:%S')
        # make sure that exception with same timestamp does not exist already
        if timestamp_mysql not in dataManager.exceptions.exceptionsDict.keys():
            # set status of all bikes that were previously (at the last minute) available to current timestamp
            # that way, the missing files do not cause a gap between status timestamps, which would result in a ride
            for bikeStatus in allStatus.statusDict.values():
                # if bike was available at the previous minute
                if bikeStatus.since == datetime_last_minute:
                    # manually set since-timestamp of bike to current time
                    bikeStatus.since = datetime_current
            # write error type with timestamp to exception dictionary
            exception = APIException(timestamp_mysql, error_type)
            dataManager.exceptions.add(exception)

class CallabikeExceptionHandler(APIExceptionHandler):
    def handleLastFileMissingException(self, allStatus: AllStatus, datetime_current):
        # datetime of the previous minute: current time minus one minute
        datetime_last_minute = datetime_current - datetime.timedelta(minutes=1)
        # convert current datetime to MySQL format
        timestamp_mysql = datetime_current.strftime('%Y-%m-%d %H:%M:%S')
        # all stations found in callabike-15.json
        callabike15_stations_list = ['E4816436351084C8665A14A791C92B9029FFE5A9', 'E53918D18D77DEC63E7617E6D576407B59A1CC64', 'E5F13D0D680098E71BCD58C24D50D2798DA105A2', 'E63983D1B9B421396BB47E084C48FF98D9F6541E', 'E7207FECDF4A6C2970B605043BD13509BC6E8BF2', 'E896DAE5C5C8641085D78F3454039BF11F88B490', 'E95B9D2D4BC6DFF1B4E4041EFA48B2E854F39F50', 'E985E95024AE08B95C5D02A99C243F3C12B21958', 'E9A4731FB1469854AF2EB04B46A20C06C7BDEFE0', 'E9C917B09D272FD073B8BAB13AFE3949D3C52D02', 'EA5B62A69DE3FFB1D78033143E67EC545149081C', 'EA7E078A544EB0EF3CE0912FEA6350BD72B9248C', 'EAB7557DE2C637133DE7FD9F59DF95B9B032FDFC', 'EB877F6C8D0310034D417C8E19856045DB312256', 'EBC6DA5A9FCFD298FD89B53DF8D3D479C77CA8A1', 'ED6733F7153177BEC3C53FC0B407F166ABAA4771', 'ED81B3D57430CD69F63A870DF80DAE082F00F2B6', 'EDA2F36F10E2351F6139CC1F7A1FAD7CE9BD7B88', 'EE2ACD8B04EDEB34DCA8F8729016AD4BAFEEF8B7', 'EEDD68FC212BB4852D306CB1507FD8AF9CA06870', 'EF978FF5850CA7680283244BBE5512E36BBBF88C', 'F1C71A67DBD7BF6E059564D42054CF0221CA1710', 'F5CB0C0F66E9AA727FD5741E82E1BECD21782E9E', 'F8C609BDD350C0D808599DE060F0C642D041B120', 'FA6605EDA10E7F1EFBA3FEA1F0F7CC226CC705A5', 'FADA02AE379131540D07A408B1ECA90C3C16C4D2', 'FADED0BBC56DA27A818D38D38947E4370BF7985C', 'FB926E835660040267689B27E176A0C5AC80AEF2', 'FF75924F15B8AADEF786B73F8DC05C5AD6223F7D', 'b180c44f-09f5-4d58-b942-6813f4b39297', 'd84aedfa-3037-43d8-b79b-bb1cc8d4bd9c', 'FDB7669045616ADB335F09EE45BCAAA5BD071A33', 'DF55D0F1B7D70B2007B4B9AB7CE751EC07555453', 'E0E1DF16874C60B2C232F94EBC8C0674D663FFE3', 'DF03F0C3EDB6CA037E09BDECB50430601FAE9275', 'DF3B9426ADBB698CB6BC513DEDF961B24FF96133', 'DF44F9023C5E9494F56F0DA04D4C0695975EA4D1', 'DEF9D70BC70DD6FDB97D15C24D172C976AED6E17', 'de1c2b90-46f9-49f3-8a16-5d39cdc04c3d', 'e61f8443-4c1f-430b-bf80-7b4350ac1957', 'f6a9894e-83a8-4404-8e1a-f4db66a82681', 'FE65FCFE5B2D6C431B7079FFE98A006F94E7E72E', 'E71F725330C12D573ECF9102452A33422581EE99', 'DF86F79CD685C1D22C87A4C7403F2D693EF53EBE', 'EB56CE583D16CF380246658016A17A1075CCCD01']
        # set timestamp to current time for previously available bikes at "callabike-15.json" stations
        for bikeStatus in allStatus.statusDict.values():
            # if bike was at one of the callabike-15 stations at the previous minute
            if bikeStatus.station_id in callabike15_stations_list and bikeStatus.since == datetime_last_minute:
                # as the last status is potentially missing
                # manually set since-timestamp of bike to current time
                bikeStatus.since = datetime_current

class NextbikeExceptionHandler(APIExceptionHandler):
    pass

if __name__ == "__main__":
    main()