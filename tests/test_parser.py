from bikesharing import Parser
from datetime import date, datetime
import os, json, pytest
from _pytest.monkeypatch import MonkeyPatch
from unittest import TestCase
import warnings

class TestParserClass:
    
    # automatically start setup function as first step
    @pytest.fixture(autouse=True)
    def setup_function(self):
        # mysql connector throws negligible DeprecationWarning: suppress that warning
        warnings.filterwarnings("ignore", category=DeprecationWarning) 
    
    # test if parsing of call a bike files works
    def testCallabikeParsing(self):
        # initialize parameters with path
        path = "json/"
        parameters = Parser.Parameters(path)
        
        # instantiate database manangers (SQL connection)
        callabikeDataManager = Parser.CallabikeDataManager()
        # set last status date to June 3rd
        # this will start the parsing with the files of June 4th
        lastStatusDate = date(2021,6,3)
        # set start date (one day after last status date)
        callabikeDataManager.setStartDate(lastStatusDate)
        # set end date to start day -> runtime of parsing: 1 day
        callabikeDataManager.setEndDate(callabikeDataManager.startDay)
        
        # get empty AllStatus from data manager
        # (do not get current status from database as it would not work with June 4th files)
        lastAllStatus = callabikeDataManager.allStatus 
        allFilesParser = Parser.CallabikeAllFilesParser(callabikeDataManager)
        allFilesParser.parseAllFiles(parameters, lastAllStatus)

        # get list of all calculated ride instances
        listOfAllRides = callabikeDataManager.allRides.ridesList
        # get first element (first ride) of rides list
        firstRide = listOfAllRides[0]
        # check that more than one ride is present
        assert len(listOfAllRides) > 0
        # check that the first list element is of class BikeRide
        assert isinstance(firstRide, Parser.BikeRide)
        # check that bike ride has a bike id string
        assert isinstance(firstRide.bike_id, str)
        # check that bike ride has a since datetime
        assert isinstance(firstRide.since, datetime)

        # get first status instance of updated AllStatus instance
        updatedStatusDict = callabikeDataManager.allStatus.statusDict
        firstStatus = next(iter(updatedStatusDict.values()))
        # check that status dict is not empty
        assert len(updatedStatusDict) > 0
        # check that bike id of first status is a string
        assert isinstance(firstStatus.bike_id, str)
        # check that since of first status is a datetime
        assert isinstance(firstStatus.since, datetime)
        # check that latitude string could be converted to float
        assert isinstance(float(firstStatus.lat), float)
    
    # test if parsing of call a bike stations works
    def testCallabikeStations(self, tmp_path):
        # initialize parameters with temporary path
        path = str(tmp_path) + "/"
        os.makedirs(path+"Stations", exist_ok=True, mode=0o777)
        parameters = Parser.Parameters(path)

        callabikeDataManager = Parser.CallabikeDataManager()
        callabikeStationsParser = Parser.CallabikeStationsParser(callabikeDataManager)
        callabikeStationsParser.getNewStations(parameters)
        callabikeStationsParser.parseNewStations(parameters)
    
    # test if parsing of nextbike files works
    def testNextbikeParsing(self):
        # initialize parameters with path
        path = "json/"
        parameters = Parser.Parameters(path)
        
        # instantiate database manangers (SQL connection)
        nextbikeDataManager = Parser.NextbikeDataManager()
        # set last status date to June 3rd
        # this will start the parsing with the files of June 4th
        lastStatusDate = date(2021,6,3)
        # set start date (one day after last status date)
        nextbikeDataManager.setStartDate(lastStatusDate)
        # set end date to start day -> runtime of parsing: 1 day
        nextbikeDataManager.setEndDate(nextbikeDataManager.startDay)
        
        # get empty AllStatus from data manager
        # (do not get current status from database as it would not work with June 4th files)
        lastAllStatus = nextbikeDataManager.allStatus  
        allFilesParser = Parser.NextbikeAllFilesParser(nextbikeDataManager)
        allFilesParser.parseAllFiles(parameters, lastAllStatus)

        # get list of all calculated ride instances
        listOfAllRides = nextbikeDataManager.allRides.ridesList
        # get first element (first ride) of rides list
        firstRide = listOfAllRides[0]
        # check that more than one ride is present
        assert len(listOfAllRides) > 0
        # check that the first list element is of class BikeRide
        assert isinstance(firstRide, Parser.BikeRide)
        # check that bike ride has a bike id string
        assert isinstance(firstRide.bike_id, str)
        # check that bike ride has a since datetime
        assert isinstance(firstRide.since, datetime)

        # get first status instance of updated AllStatus instance
        updatedStatusDict = nextbikeDataManager.allStatus.statusDict
        firstStatus = next(iter(updatedStatusDict.values()))
        # check that status dict is not empty
        assert len(updatedStatusDict) > 0
        # check that bike id of first status is a string
        assert isinstance(firstStatus.bike_id, str)
        # check that since of first status is a datetime
        assert isinstance(firstStatus.since, datetime)
        # check that latitude string could be converted to float
        assert isinstance(float(firstStatus.lat), float)
    
    # test if getting last status works for callabike 
    # (only works if database is not empty, i.e. parsing has run at least once)
    def testCallabikeGetStatus(self):
        # instantiate database manangers (SQL connection)
        callabikeDataManager = Parser.CallabikeDataManager()
        
        # get last status from database
        lastAllStatus = callabikeDataManager.getLastStatus()

        # get all status instance and first status instance
        statusDict = lastAllStatus.statusDict
        firstStatus = next(iter(statusDict.values()))
        # check that at least one status is present
        assert len(statusDict) > 0
        # check that bike id of first status is a string
        assert isinstance(firstStatus.bike_id, str)
        # check that since of first status is a datetime
        assert isinstance(firstStatus.since, datetime)
        # check that latitude string could be converted to float
        assert isinstance(float(firstStatus.lat), float)

    # test if getting last status works for nextbike 
    # (only works if database is not empty, i.e. parsing has run at least once)
    def testNextbikeGetStatus(self):
        # instantiate database manangers (SQL connection)
        nextbikeDataManager = Parser.NextbikeDataManager()
        
        # get last status from database
        lastAllStatus = nextbikeDataManager.getLastStatus()

        # get all status instance and first status instance
        statusDict = lastAllStatus.statusDict
        firstStatus = next(iter(statusDict.values()))
        # check that at least one status is present
        assert len(statusDict) > 0
        # check that bike id of first status is a string
        assert isinstance(firstStatus.bike_id, str)
        # check that since of first status is a datetime
        assert isinstance(firstStatus.since, datetime)
        # check that latitude string could be converted to float
        assert isinstance(float(firstStatus.lat), float)

    # test parsing of a single call a bike file
    def testCallabikeParseSingleFile(self):
        # instantiate database manangers (SQL connection)
        callabikeDataManager = Parser.CallabikeDataManager()

        # get empty AllStatus from data manager
        # (do not get current status from database as it would not work with June 4th files)
        lastAllStatus = callabikeDataManager.allStatus 

        # set path to first folder (on June 4th at midnight)
        dirPath = "json/2021-06-04/00-00"
        # set current datetime to first minute
        datetime_current = datetime(2021,6,4,0,0)
        # get cut-off positions and numbers of bikes from first file
        # cut-off positions are updated every minute this way (in case it suddenly changes)
        fileParser = Parser.CallabikeFileParser(callabikeDataManager)
        fileParser.parseFirstFile(dirPath+'/callabike-0.json')
        lastAllStatus = fileParser.parseFile(dirPath+'/callabike-0.json', lastAllStatus, datetime_current)
        
        # get all status instance and first status instance
        statusDict = lastAllStatus.statusDict
        firstStatus = next(iter(statusDict.values()))
        # check that at least one status is present
        assert len(statusDict) > 0
        # check that bike id of first status is a string
        assert isinstance(firstStatus.bike_id, str)
        # check that since of first status is a datetime
        assert isinstance(firstStatus.since, datetime)
        # check that latitude string could be converted to float
        assert isinstance(float(firstStatus.lat), float)
    
    # test parsing of a single nextbike file
    def testNextbikeParseSingleFile(self):
        # instantiate database manangers (SQL connection)
        nextbikeDataManager = Parser.NextbikeDataManager()

        # get empty AllStatus from data manager
        # (do not get current status from database as it would not work with June 4th files)
        lastAllStatus = nextbikeDataManager.allStatus 

        # set path to first folder (on June 4th at midnight)
        dirPath = "json/2021-06-04/00-00"
        # set current datetime to first minute
        datetime_current = datetime(2021,6,4,0,0)
        # get cut-off positions and numbers of bikes from first file
        # cut-off positions are updated every minute this way (in case it suddenly changes)
        fileParser = Parser.NextbikeFileParser(nextbikeDataManager)
        lastAllStatus = fileParser.parseFile(dirPath+'/nextbike.json', lastAllStatus, datetime_current)
        
        # get all status instance and first status instance
        statusDict = lastAllStatus.statusDict
        firstStatus = next(iter(statusDict.values()))
        # check that at least one status is present
        assert len(statusDict) > 0
        # check that bike id of first status is a string
        assert isinstance(firstStatus.bike_id, str)
        # check that since of first status is a datetime
        assert isinstance(firstStatus.since, datetime)
        # check that latitude string could be converted to float
        assert isinstance(float(firstStatus.lat), float)