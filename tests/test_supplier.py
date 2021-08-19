from bikesharing import Supplier
from abc import ABC, abstractmethod
import json
import warnings

class ParameterTester(ABC):
    def setUpSupplier(self):
        # mysql connector throws negligible DeprecationWarning: suppress that warning
        warnings.filterwarnings("ignore", category=DeprecationWarning) 
        self.databaseManager = Supplier.DatabaseManager()
        # instantiate parameters object
        self.parameters = Supplier.Parameters()
        
        # limit all results to a defined timeframe (test would take too long otherwise)
        self.parameters.since_min=  "2021-06-05 10:00"
        self.parameters.since_max= "2021-06-05 18:00"
        # manually set all remaining parameters to None
        # (will be set individually by the test sub-classes)
        self.parameters.until_min= None
        self.parameters.until_max= None
        self.parameters.time_min= None # string format: HH:MM
        self.parameters.time_max= None
        self.parameters.weekday_weekend= None # wd or we
        self.parameters.provider= None
        self.parameters.requireStartStation= None
        self.parameters.requireEndStation= None
        self.parameters.requireEitherStation= None
        self.parameters.start_coordinates= None # string format: 52.52 13.42
        self.parameters.start_radius= None # radius in m
        self.parameters.end_coordinates= None
        self.parameters.end_radius= None
        self.parameters.start_district= None # official district number
        self.parameters.end_district= None
        self.parameters.includeNearbyStations= None
        self.parameters.proximityStations= None
        self.parameters.flagExceptions= None

    def getJSON(self, path):
        rideQueryString = self.databaseManager.generateQuery(self.parameters)
        
        bikeRides = self.databaseManager.executeQuery(self.parameters, rideQueryString)
        # close the connection
        self.databaseManager.close()

        # generate JSON with all rides
        bikeRides.generateJSONofRides(path)
        with open(path) as file:
            # try to open file as JSON (exception if not a JSON)
            jsonResponse = json.loads(file.read())
            # check if JSON object contains at least one element
            assert len(jsonResponse)>0
            print(str(len(jsonResponse)))
        
        # turn off suppression of DeprecationWarnings again
        warnings.filterwarnings("default", category=DeprecationWarning)
    @abstractmethod
    def testParameter(self):
        pass

class TestSince(ParameterTester):
    def testParameter(self, tmp_path):
        super().setUpSupplier()
        self.parameters.since_min = "2021-06-05 10:00"
        self.parameters.since_max = "2021-06-05 18:00"
        path = tmp_path / "sinceTest.json"
        super().getJSON(path)
class TestUntil(ParameterTester):
    def testParameter(self, tmp_path):
        super().setUpSupplier()
        self.parameters.until_min = "2021-06-05 16:00"
        self.parameters.until_max = "2021-06-05 23:59"
        path = tmp_path / "untilTest.json"
        super().getJSON(path)
class TestTime(ParameterTester):
    def testParameter(self, tmp_path):
        super().setUpSupplier()
        self.parameters.time_min = "12:00"
        self.parameters.time_max = "20:00"
        path = tmp_path / "timeTest.json"
        super().getJSON(path)
class TestWeekdayWeekend(ParameterTester):
    def testParameter(self, tmp_path):
        super().setUpSupplier()
        self.parameters.weekday_weekend = "we"
        path = tmp_path / "weekdayWeekendTest.json"
        super().getJSON(path)
class TestProvider(ParameterTester):
    def testParameter(self, tmp_path):
        super().setUpSupplier()
        self.parameters.provider = "nextbike"
        path = tmp_path / "providerTest.json"
        super().getJSON(path)
class TestRequireStation(ParameterTester):
    def testParameter(self, tmp_path):
        super().setUpSupplier()
        self.parameters.requireStartStation = "y"
        self.parameters.requireEndStation = "y"
        path = tmp_path / "requireStationTest.json"
        super().getJSON(path)
class TestRequireEitherStation(ParameterTester):
    def testParameter(self, tmp_path):
        super().setUpSupplier()
        self.parameters.requireEitherStation = "y"
        path = tmp_path / "requireEitherStationTest.json"
        super().getJSON(path)
class TestStartLocation(ParameterTester):
    def testParameter(self, tmp_path):
        super().setUpSupplier()
        self.parameters.start_coordinates = "52.52 13.42"
        self.parameters.start_radius = "600"
        path = tmp_path / "startLocationTest.json"
        super().getJSON(path)
class TestEndLocation(ParameterTester):
    def testParameter(self, tmp_path):
        super().setUpSupplier()
        self.parameters.end_coordinates = "52.52 13.42"
        self.parameters.end_radius = "600"
        path = tmp_path / "endLocationTest.json"
        super().getJSON(path)
class TestDistrict(ParameterTester):
    def testParameter(self, tmp_path):
        super().setUpSupplier()
        self.parameters.start_district = "1"
        self.parameters.end_district = "2"
        path = tmp_path / "districtTest.json"
        super().getJSON(path)
# nearby stations can be activated if required
# omitted as getting all nearby stations usually takes a long time
# class TestNearbyStations(ParameterTester):
#     def testParameter(self, tmp_path):
#         super().setUpSupplier()
#         self.parameters.includeNearbyStations = "y"
#         self.parameters.proximityStations = "100"
#         path = tmp_path / "nearbyStationsTest.json"
#         super().getJSON(path)
class TestFlagExceptions(ParameterTester):
    def testParameter(self, tmp_path):
        super().setUpSupplier()
        self.parameters.flagExceptions = "y"
        path = tmp_path / "flagExceptionsTest.json"
        super().getJSON(path)
