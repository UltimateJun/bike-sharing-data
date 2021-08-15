from dataclasses import dataclass
from mysql.connector import connect, Error
import os, os.path, json
from abc import ABC, abstractmethod
import geopy.distance
import datetime, time
import decimal
import sys

def main():
    # set up database manager and connect to database
    databaseManager = DatabaseManager()

    # instantiate parameters object
    parameters = Parameters()
    # get parameters from terminal input (pass parameters instance and list of districts)
    InputGetter().getInput(parameters, databaseManager.getDistricts())
    # get list of rides from database
    rideQueryString = databaseManager.generateQuery(parameters)

    bikeRides = databaseManager.executeQuery(parameters, rideQueryString)
    # close the connection
    databaseManager.close()

    # generate JSON with all rides with current datetime in filename
    now_string = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    path = 'queries_json/query_'+now_string+'.json'
    bikeRides.generateJSONofRides(path)

class DatabaseManager:
    def __init__(self):
        # try to create MySQL connection and cursor
        try:
            self.connection = connect(option_files='mysql.conf')
            self.connection.autocommit = True
            self.cursor = self.connection.cursor(named_tuple=True)
        except (Error) as e:
            # print exception to standar error if error occured while connecting to database
            print("Database connection could not be established: " + str(e), file=sys.stderr)
    def getDistricts(self):
        # get all district IDs and names to district list
        try:
            self.cursor.execute("SELECT district_id, name FROM district")
        except (Error) as e:
            # print exception to standar error if error occured while connecting to database
            print("Database connection could not be established: " + str(e), file=sys.stderr)
        district_list = self.cursor.fetchall()
        return district_list
    def generateQuery(self, parameters):
        # create list for query (more efficient than concatenating strings with += as only one string is generated)
        rideQuery = []
        # append query in its most basic form
        # extension for query: get error_type for occured exceptions (if parameter set accordingly)
        exceptionExtension = ", (SELECT error_type FROM exception WHERE ((exception.time BETWEEN bike_ride.since AND bike_ride.until) AND bike.provider=exception.provider) LIMIT 1) AS error_type" if parameters.flagExceptions else ""
        basicQuery = "SELECT DISTINCT(bike_ride.bike_ride_id), bike_id, ST_X(start_coordinates) AS start_lat, ST_Y(start_coordinates) AS start_lon, ST_X(end_coordinates) AS end_lat, ST_Y(end_coordinates) AS end_lon, start_station_id, end_station_id, distance_meters, since, until, duration_min, suspicious, bike.provider" + exceptionExtension + " FROM bike_ride LEFT JOIN ride_analysis ON bike_ride.bike_ride_id=ride_analysis.bike_ride_id NATURAL JOIN bike"
        rideQuery.append(basicQuery)

        # create instance of queryHolder, holding parameters and rideQuery
        queryHolder = QueryHolder(parameters, rideQuery)
        # instantiate all subclasses of QueryAdder and add queries to rideQuery
        for queryAdder in QueryAdder.__subclasses__():
            queryAdder = queryAdder()
            queryAdder.addQuery(queryHolder)
            
        rideQueryString = ' '.join(rideQuery)
        print(rideQueryString)
        return rideQueryString

    def executeQuery(self, parameters, rideQueryString):
        self.cursor.execute(rideQueryString)

        bikeRides = BikeRides()
        for ride in self.cursor:
            # instantiate BikeRide and set query results as attributes
            bikeRide = BikeRide()
            bikeRide.bike_ride_id = ride.bike_ride_id
            bikeRide.bike_id = ride.bike_id
            bikeRide.start_lat = ride.start_lat
            bikeRide.start_lon = ride.start_lon
            bikeRide.end_lat = ride.end_lat
            bikeRide.end_lon = ride.end_lon
            bikeRide.start_station = ride.start_station_id
            bikeRide.end_station = ride.end_station_id
            bikeRide.ride_distance = ride.distance_meters
            bikeRide.ride_start = ride.since
            bikeRide.ride_end = ride.until
            bikeRide.duration_min = ride.duration_min
            bikeRide.suspicious = ride.suspicious
            bikeRide.provider = ride.provider

            if parameters.flagExceptions == "y":
                # if any error was found during the duration of the ride: set exception_occured to true
                bikeRide.exception_occured = True if ride.error_type else False

            # add bikeRide object to list in BikeRides
            bikeRides.add(bikeRide)

        if parameters.includeNearbyStations == "y":
            if parameters.proximityStations:
                self.getNearbyStations(parameters, bikeRides)
        
        return bikeRides
    def getNearbyStations(self, parameters, bikeRides):
        # get all stations from database
        try:
            self.cursor.execute("SELECT station_id, name, ST_X(coordinates) AS lon, ST_Y(coordinates) AS lat, provider FROM station")
        except (Error) as e:
            # print exception to standar error if error occured while connecting to database
            print("Database connection could not be established: " + str(e), file=sys.stderr)
        # instantiate object to collect stations
        stations = Stations()
        # instantiate station object for every result, set attributes
        for station in self.cursor:
            stations.add(station)

        # get all bikes from database
        try:
            self.cursor.execute("SELECT bike_id, provider FROM bike")
        except (Error) as e:
            # print exception to standar error if error occured while connecting to database
            print("Database connection could not be established: " + str(e), file=sys.stderr)
        # no dataclass for bike as we need the dictionary key-lookup functionality
        bike_provider_dict = {bike[0]:bike[1] for bike in self.cursor.fetchall()}

        for ride in bikeRides.bikeRidesList: 
            # new list for object attribute stations_nearby
            ride.stations_nearby = []

            # get coordinates of drop-off point into tuple
            end_coords = (ride.end_lat, ride.end_lon)
            # get provider of bike from dictionary
            bikeProvider = bike_provider_dict[ride.bike_id]

            # go through all stations
            for station in stations.stationsList:
                # only check stations of same provider
                if station.provider == bikeProvider:
                    station_coords = (station.lon, station.lat)
                    station_distance = geopy.distance.distance(end_coords, station_coords).meters
                    # if station is within given distance
                    if station_distance < int(parameters.proximityStations):
                        # get distance rounded to one decimal place
                        station_distance = round(station_distance,1)
                        # write station id, name and distance to nearby stations attribute
                        ride.stations_nearby.append({'station_id':station.station_id, 'station_name':station.name, 'station_distance':station_distance})
    def close(self):
        # close the MySQL connection
        self.connection.close()

@dataclass(init=False)
class Parameters:
    # parameters can be set directly on the object
    # parameters object is instantiated with default values
    since_min: str # string format: YYYY-MM-DD HH:MM
    since_max: str
    until_min: str
    until_max: str
    time_min: str # string format: HH:MM
    time_max: str
    weekday_weekend: str # wd or we
    provider: str
    requireStartStation: str
    requireEndStation: str
    requireEitherStation: str
    start_coordinates: str # string format: 52.52 13.42
    start_radius: str # radius in m
    end_coordinates: str
    end_radius: str
    start_district: str # official district number
    end_district: str
    includeNearbyStations: str
    proximityStations: str
    flagExceptions: str

class BikeRides:
    def __init__(self):
        self.bikeRidesList = []
    def add(self, bikeRide):
        self.bikeRidesList.append(bikeRide)
    def generateJSONofRides(self, path):
        # create list of dictionaries made from bike ride objects
        ride_list = [ride.__dict__ for ride in self.bikeRidesList]
        # create JSON string from list
        jsonString = json.dumps(ride_list, cls=DecimalAndDatetimeHandler)
        try:
            with open(path, 'w') as f:
                # write JSON string to file
                f.write(jsonString)
        except IOError as error:
            print("File system could not be read! " + str(error), file=sys.stderr)
        print('Query successfully saved to '+str(path)+'!')

@dataclass(init=False)
class BikeRide:
    bike_ride_id: int
    bike_id: str
    start_lat: float
    start_lon: float
    end_lat: float
    end_lon: float
    start_station: str
    end_station: str
    ride_distance: float
    ride_start: datetime
    ride_end: datetime
    duration_min: int
    suspicious: bool
    provider: str
    # stations_nearby and exception_occured only added if parameter set accordingly
    # if not requested: will not be included in JSON output
    # stations_nearby: list
    # exception_occured: bool

class Stations:
    stationsList = []
    def add(self, station):
        self.stationsList.append(station)

@dataclass(init=False)
class Station:
    station_id: str
    name: str
    lon: float
    lan: float
    provider: str

class InputGetter:
    def getInput(self, parameters, district_list):
        try:
            # request parameters from terminal input
            print('Please enter filter parameters of your query! Leave blank to skip that filter.')
            parameters.since_min = input(f"Ride beginning at YYYY-MM-DD HH:MM or later: ")
            # check if input is in correct format -> throws exception if format is wrong
            if parameters.since_min: datetime.datetime.strptime(parameters.since_min, "%Y-%m-%d %H:%M")
            parameters.since_max = input(f"Ride beginning at YYYY-MM-DD HH:MM or earlier: ")
            if parameters.since_max: datetime.datetime.strptime(parameters.since_max, "%Y-%m-%d %H:%M")
            parameters.until_min = input(f"Ride ending at YYYY-MM-DD HH:MM or later: ")
            if parameters.until_min: datetime.datetime.strptime(parameters.until_min, "%Y-%m-%d %H:%M")
            parameters.until_max = input(f"Ride ending at YYYY-MM-DD HH:MM or earlier: ")
            if parameters.until_max: datetime.datetime.strptime(parameters.until_max, "%Y-%m-%d %H:%M")
            parameters.time_min = input(f"Ride beginning at HH:MM or later: ")
            if parameters.time_min: time.strptime(parameters.time_min, "%H:%M")
            parameters.time_max = input(f"Ride beginning at HH:MM or earlier: ")
            if parameters.time_max: time.strptime(parameters.time_max, "%H:%M")
            parameters.weekday_weekend = input(f"Ride beginning on a weekday (wd) or weekend (we): ")
            parameters.provider = input(f"Provider (callabike / nextbike): ")
            parameters.requireStartStation = input(f"Ride starting at a station (y / n): ")
            parameters.requireEndStation = input(f"Ride ending at a station (y / n): ")
            parameters.requireEitherStation = input(f"Ride starting and/or ending at a station (y / n): ")
            parameters.start_coordinates = input(f"Ride starting in area - center coordinates (format: 52.52 13.42): ")
            # also request start radius if if start coordinates were entered
            if parameters.start_coordinates:
                parameters.start_radius = input(f"Ride starting in area - radius (in m) around center coordinate: ")
            parameters.end_coordinates = input(f"Ride ending in area - center coordinates (format: 52.52 13.42): ")
            # also request end radius if if end coordinates were entered
            if parameters.end_coordinates:
                parameters.end_radius = input(f"Ride ending in area - radius (in m) around center coordinate: ")
            print("Here's a list of the districts in Berlin:")
            for district in district_list:
                print(str(district[0]) + ': ' + district[1])
            parameters.start_district = input(f"Ride beginning in district number: ")
            parameters.end_district = input(f"Ride ending in district number: ")
            includeNearbyStations = input(f"Should stations in proximity of drop-off point also be retrieved? This will increase lookup time significantly. (y/n): ")
            parameters.includeNearbyStations = includeNearbyStations
            parameters.proximityStations = input(f"Stations in proximity - radius (in m) around drop-off point: ") if includeNearbyStations == "y" else ""
            parameters.flagExceptions = input(f"Should rides be flagged if an API exception occured during the ride? (y/n): ")
        # if invalid value is entered, print error message and restart input
        except ValueError:
            print("Please enter a valid value! Try again:")
            self.getInput(parameters, district_list)

# class to create queryConnector and hold parameters and rideQuery
# QueryAdders can access these three to create the query
class QueryHolder():
    def __init__(self, parameters, rideQuery):
        self.queryConnector = QueryConnector()
        self.parameters = parameters
        self.rideQuery = rideQuery
        
class QueryAdder(ABC):
    @abstractmethod
    def addQuery(self):
        pass
class SinceMinAdder(QueryAdder):
    def addQuery(self, queryHolder):
        # generate MySQL clause strings depending on given parameters
        if queryHolder.parameters.since_min:
            queryParameter = "since >= '" + queryHolder.parameters.since_min + "'"
            queryHolder.rideQuery = queryHolder.queryConnector.appendParameter(queryHolder.rideQuery, queryParameter)
class SinceMaxAdder(QueryAdder):
    def addQuery(self, queryHolder):
        if queryHolder.parameters.since_max:
            queryParameter = "since <= '" + queryHolder.parameters.since_max + "'"
            queryHolder.rideQuery = queryHolder.queryConnector.appendParameter(queryHolder.rideQuery, queryParameter)
class UntilMinAdder(QueryAdder):
    def addQuery(self, queryHolder):
        if queryHolder.parameters.until_min:
            queryParameter = "until >= '" + queryHolder.parameters.until_min + "'"
            queryHolder.rideQuery = queryHolder.queryConnector.appendParameter(queryHolder.rideQuery, queryParameter)
class UntilMaxAdder(QueryAdder):
    def addQuery(self, queryHolder):
        if queryHolder.parameters.until_max:
            queryParameter = "until <= '" + queryHolder.parameters.until_max + "'"
            queryHolder.rideQuery = queryHolder.queryConnector.appendParameter(queryHolder.rideQuery, queryParameter)
class TimeMinAdder(QueryAdder):
    def addQuery(self, queryHolder):
        if queryHolder.parameters.time_min:
            queryParameter = "TIME(since) >= '" + queryHolder.parameters.time_min + "'"
            queryHolder.rideQuery = queryHolder.queryConnector.appendParameter(queryHolder.rideQuery, queryParameter)
class TimeMaxAdder(QueryAdder):
    def addQuery(self, queryHolder):
        if queryHolder.parameters.time_max:
            queryParameter = "TIME(since) <= '" + queryHolder.parameters.time_max + "'"
            queryHolder.rideQuery = queryHolder.queryConnector.appendParameter(queryHolder.rideQuery, queryParameter)
class WeekdayWeekendAdder(QueryAdder):
    def addQuery(self, queryHolder):
        if queryHolder.parameters.weekday_weekend:
            if queryHolder.parameters.weekday_weekend == 'wd':
                # condition: weekday of since is any day from Monday to Friday
                queryParameter = "WEEKDAY(since) IN (0,1,2,3,4)"
                queryHolder.rideQuery = queryHolder.queryConnector.appendParameter(queryHolder.rideQuery, queryParameter)
            if queryHolder.parameters.weekday_weekend == 'we':
                # condition: weekday of since is either Saturday or Sunday
                queryParameter = "WEEKDAY(since) IN (5,6)"
                queryHolder.rideQuery = queryHolder.queryConnector.appendParameter(queryHolder.rideQuery, queryParameter)
class ProviderAdder(QueryAdder):
    def addQuery(self, queryHolder):
        if queryHolder.parameters.provider:
            queryParameter = "provider = '" + queryHolder.parameters.provider + "'"
            queryHolder.rideQuery = queryHolder.queryConnector.appendParameter(queryHolder.rideQuery, queryParameter)
class RequireStartStationAdder(QueryAdder):
    def addQuery(self, queryHolder):
        if queryHolder.parameters.requireStartStation == "y":
            queryParameter = "start_station_id IS NOT NULL"
            queryHolder.rideQuery = queryHolder.queryConnector.appendParameter(queryHolder.rideQuery, queryParameter)
class RequireEndStationAdder(QueryAdder):
    def addQuery(self, queryHolder):
        if queryHolder.parameters.requireEndStation == "y":
            queryParameter = "end_station_id IS NOT NULL"
            queryHolder.rideQuery = queryHolder.queryConnector.appendParameter(queryHolder.rideQuery, queryParameter)
class RequireEitherStationAdder(QueryAdder):
    def addQuery(self, queryHolder):
        if queryHolder.parameters.requireEitherStation == "y":
            queryParameter = "(start_station_id IS NOT NULL OR end_station_id IS NOT NULL)"
            queryHolder.rideQuery = queryHolder.queryConnector.appendParameter(queryHolder.rideQuery, queryParameter)
class StartCoordinatesAdder(QueryAdder):
    def addQuery(self, queryHolder):
        if queryHolder.parameters.start_coordinates:
            mySqlGeom = "ST_GeomFromText('POINT(" + queryHolder.parameters.start_coordinates + ")', 4326)"
            queryParameter = "ST_Distance_Sphere(start_coordinates, "+mySqlGeom+") < " + queryHolder.parameters.start_radius
            queryHolder.rideQuery = queryHolder.queryConnector.appendParameter(queryHolder.rideQuery, queryParameter)
class EndCoordinatesAdder(QueryAdder):
    def addQuery(self, queryHolder):
        if queryHolder.parameters.end_coordinates:
            mySqlGeom = "ST_GeomFromText('POINT(" + queryHolder.parameters.end_coordinates + ")', 4326)"
            queryParameter = "ST_Distance_Sphere(start_coordinates, "+mySqlGeom+") < " + queryHolder.parameters.end_radius
            queryHolder.rideQuery = queryHolder.queryConnector.appendParameter(queryHolder.rideQuery, queryParameter)
class StartDistrictAdder(QueryAdder):
    def addQuery(self, queryHolder):
        if queryHolder.parameters.start_district:
            queryParameter = "ST_Within(start_coordinates, " + "(SELECT area FROM district WHERE district_id=" + queryHolder.parameters.start_district + "))"
            queryHolder.rideQuery = queryHolder.queryConnector.appendParameter(queryHolder.rideQuery, queryParameter)
class EndDistrictAdder(QueryAdder):
    def addQuery(self, queryHolder):
        if queryHolder.parameters.end_district:
            queryParameter = "ST_Within(end_coordinates, " + "(SELECT area FROM district WHERE district_id=" + queryHolder.parameters.end_district + "))"
            queryHolder.rideQuery = queryHolder.queryConnector.appendParameter(queryHolder.rideQuery, queryParameter)

# class maintaining the string connecting the WHERE clauses
class QueryConnector():
    # when initiated for the first time: append WHERE next
    def __init__(self):
        self.connector_string = "WHERE"
    # after the first clause: change connector to AND
    def appendConnector(self, rideQuery):
        rideQuery.append(self.connector_string)
        self.connector_string = "AND"
        return rideQuery
    # appending a new parameter
    def appendParameter(self, rideQuery, queryParameter):
        # append connector string (WHERE or AND) first
        rideQuery = self.appendConnector(rideQuery)
        # append given query parameter
        rideQuery.append(queryParameter)
        return rideQuery

# extension for JSONEncoder: if decimal or datetime encountered, convert to string first
class DecimalAndDatetimeHandler(json.JSONEncoder):
    def default(self, attribute):
        if isinstance(attribute, decimal.Decimal):
            return str(attribute)
        if isinstance(attribute, datetime.datetime):
            return str(attribute)
        return super(DecimalAndDatetimeHandler, self).default(attribute)

if __name__ == "__main__":
    main()