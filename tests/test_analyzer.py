from bikesharing import Analyzer
from datetime import date, datetime
import os, json, pytest
from _pytest.monkeypatch import MonkeyPatch
from unittest import TestCase
import warnings

class TestAnalyzerClass(TestCase):
    # create own instance of monkeypatch 
    # (otherwise does not work within a test class)
    def setUp(self):
        self.monkeypatch = MonkeyPatch()
    def testParametersInput(self):
        parameters = Analyzer.Parameters

        # create dictionary with mock input values
        parametersInputDict = dict()
        parametersInputDict['start_date'] = "2021-06-01"
        parametersInputDict['end_date'] = "2021-06-15"
        parametersInputDict['provider'] = "callabike"
        parametersInputDict['min_distance'] = ""
        parametersInputDict['max_distance'] = ""
        parametersInputDict['min_duration'] = "5"
        parametersInputDict['max_duration'] = "2880"
        parametersInputDict['flagSameStationAsSuspicious'] = "y"
        parametersInputDict['maxSimultaneousPickupCount'] = ""

        # create iterator object with input values
        inputIterator = iter(parametersInputDict.values())

        # use monkeypatch to mock terminal input with iterator
        self.monkeypatch.setattr('builtins.input', lambda _: next(inputIterator))

        # call getInput function
        Analyzer.InputGetter().getInputFromTerminal(parameters)
        # check if parameters (and data types) are set correctly in parameters dataclass
        assert parameters.start_date == "2021-06-01"
        assert parameters.end_date == "2021-06-15"
        assert parameters.provider == "callabike"
        # no input should result in default values 50 and 15000
        assert parameters.min_distance == 50 
        assert parameters.max_distance == 15000
        assert parameters.min_duration == 5
        assert parameters.max_duration == 2880
        assert parameters.flagSameStationAsSuspicious == True
        assert parameters.maxSimultaneousPickupCount == None
    def testGetRides(self): 
        parameters = Analyzer.Parameters
        # set parameters to one day
        parameters.start_date = "2021-07-01"
        parameters.end_date = "2021-07-01"

        # mysql connector throws negligible DeprecationWarning: suppress that warning
        warnings.filterwarnings("ignore", category=DeprecationWarning) 
        bikeRides = Analyzer.DatabaseManager().getAllRides(parameters)
        # turn off suppression of DeprecationWarnings again
        warnings.filterwarnings("default", category=DeprecationWarning)

        # get rides list, thereof start and end dates of first ride
        rides_list = bikeRides.rides_list
        startDateOfFirstRide = rides_list[0].since.date()
        endDateOfFirstRide = rides_list[0].until.date()
        
        # make sure that start and end of first ride are within given day
        assert startDateOfFirstRide == date(2021, 7, 1)
        assert endDateOfFirstRide == date(2021, 7, 1)
    def testGetRideCount(self):
        parameters = Analyzer.Parameters
        # set parameters to one day
        parameters.start_date = "2021-07-01"
        parameters.end_date = "2021-07-01"

        # mysql connector throws negligible DeprecationWarning: suppress that warning
        warnings.filterwarnings("ignore", category=DeprecationWarning) 
        rideCounts = Analyzer.DatabaseManager().getRideCountByMinute(parameters)
        # turn off suppression of DeprecationWarnings again
        warnings.filterwarnings("default", category=DeprecationWarning)

        # check that both providers have at least 1 count entry
        assert len(rideCounts["callabike"]) > 0
        assert len(rideCounts["nextbike"]) > 0

        # get first key in each array of both providers
        # get date of that key (e.g. from "2021-07-01 00:10:00")
        firstKeyInNextbike = next(iter(rideCounts["nextbike"]))
        firstKeyInCallabike = next(iter(rideCounts["callabike"]))
        dateFromKeyInNextbike = datetime.strptime(firstKeyInNextbike, "%Y-%m-%d %H:%M:%S").date()
        dateFromKeyInCallabike = datetime.strptime(firstKeyInCallabike, "%Y-%m-%d %H:%M:%S").date()
        # check that date from that key corresponds to given date
        assert dateFromKeyInNextbike == date(2021,7,1)
        assert dateFromKeyInCallabike == date(2021,7,1)
    def testAnalyzer(self):
        # get default parameters
        parameters = Analyzer.Parameters

        # imitate bike ride with test values
        bikeRide = Analyzer.BikeRide
        bikeRide.bike_ride_id = 1
        bikeRide.distance = 100
        bikeRide.start_station_id = "teststation"
        bikeRide.end_station_id = "teststation"
        bikeRide.since = datetime(2021,7,1,10,0)
        bikeRide.until = datetime(2021,7,1,12,0)
        bikeRide.provider = "callabike"

        # imitate ride count dictionary (5 simultaneous rides at time of pickup)
        rideCounts = {"callabike": {"2021-07-01 10:00:00": 5}}
        rideAnalyzer = Analyzer.RideAnalyzer(bikeRide)
        rideAnalyzer.analyzeRide(parameters, rideCounts)

        assert bikeRide.duration == 120
        assert bikeRide.startAndEndSameStations == True
        assert bikeRide.suspicious == False

    