from mysql.connector import connect, Error
import datetime
from dataclasses import dataclass
from abc import ABC, abstractmethod
from collections import defaultdict
import sys
            
def main():
    # instantiate database mananger (SQL connection)
    databaseManager = DatabaseManager()
    # get parameters from terminal input
    parameters = Parameters()
    InputGetter().getInputFromTerminal(parameters)
    # get all rides from database
    bikeRides = databaseManager.getAllRides(parameters)
    
    # get dictionary of number of rides per minute
    rideCounts = databaseManager.getRideCountByMinute(parameters)
    # analyze and evaluate all resulting rides
    bikeRides.analyzeAllRides(parameters, rideCounts)
    # insert analysis into database
    databaseManager.insertAnalysis(bikeRides.rides_list)
    databaseManager.close()

class DatabaseManager:
    def __init__(self):
        # try to create MySQL connection and cursor
        try:
            self.connection = connect(option_files='mysql.conf')
            self.cursor = self.connection.cursor(named_tuple=True)
        except (Error) as e:
            # print exception to standard error if error occured while connecting to database
            print("Database connection could not be established: " + str(e), file=sys.stderr)
    def getAllRides(self, parameters):
        # if provider was given: add string with natural join and provider-clause in WHERE, else add only WHERE
        where_provider = "WHERE" if not parameters.provider else "NATURAL JOIN bike WHERE provider='" + parameters.provider + "' AND"
        # build SQL query and execute
        bikeRides = BikeRides()
        try:
            self.cursor.execute("SELECT bike_ride_id, ST_Distance_Sphere(start_coordinates, end_coordinates) AS distance, start_station_id, end_station_id, since, until, provider FROM bike_ride NATURAL JOIN bike "+where_provider+" since BETWEEN '"+parameters.start_date+" 00:00:00' AND '"+parameters.end_date+" 23:59:59'")
            # create new instance of BikeRides
            for ride in self.cursor:
                # create new instance of a bike ride
                bikeRide = BikeRide()
                # set attributes of bike ride instance to SQL results
                bikeRide.bike_ride_id = ride.bike_ride_id
                bikeRide.distance = ride.distance
                bikeRide.start_station_id = ride.start_station_id
                bikeRide.end_station_id = ride.end_station_id
                bikeRide.since = ride.since
                bikeRide.until = ride.until
                bikeRide.provider = ride.provider
                # add bike ride instance to BikeRides instance
                bikeRides.add(bikeRide)
        except (Error) as e:
            # print exception if database error occured
            print("Database connection could not be established: " + str(e), file=sys.stderr)
        return bikeRides
    def insertAnalysis(self, analyzed_rides_list):
        # use list comprehension to generate list of tuples containing values to be inserted
        ride_analysis_list = [(ride.bike_ride_id, ride.duration, ride.distance, ride.startAndEndSameStations, ride.simultaneousRidesCount, ride.suspicious) for ride in analyzed_rides_list]
        # insert list with all analysis of rides into ride_analysis table (replace existing entries)
        try:
            self.cursor.executemany("REPLACE INTO ride_analysis VALUES (%s, %s, %s, %s, %s, %s)", ride_analysis_list)
            self.connection.commit()
        except (Error) as e:
            print("Database connection could not be established: " + str(e), file=sys.stderr)
        print('Inserting ride analysis done!')
    def getRideCountByMinute(self, parameters):
        # create a default dictionary for ride counts (will automatically add sub-keys if not yet existent)
        rideCounts =  defaultdict(dict)
        try:
            self.cursor.execute("SELECT provider, since, COUNT(*) AS count FROM bike_ride NATURAL JOIN bike WHERE since BETWEEN '"+parameters.start_date+" 00:00:00' AND '"+parameters.end_date+" 23:59:59' GROUP BY since, provider ORDER BY since")
            for rideCount in self.cursor:
                # nested dictionary: provider -> minute
                rideCounts[rideCount.provider][str(rideCount.since)] = rideCount.count
        except (Error) as e:
            print("Database connection could not be established: " + str(e), file=sys.stderr)
        return rideCounts

    def close(self):
        # close the MySQL connection
        self.connection.close()

@dataclass
class Parameters:
    # parameters can be set directly on the object
    # parameters object is instantiated with default values
    start_date: str = ""
    end_date: str = ""
    provider: str = None
    min_distance: int = 50
    max_distance: int = 15000
    min_duration: int = 3
    max_duration: int = 1440
    flagSameStationAsSuspicious: bool = False
    maxSimultaneousPickupCount: int = None
    
class InputGetter:
    # parameters can also be set using input from terminal
    def getInputFromTerminal(self, parameters):
        try:
            # get input from terminal and save as attribute of parameters object
            parameters.start_date = input(f"Please enter start date of analysis (YYYY-MM-DD): ")
            # try to convert date string -> throws exception if format is wrong
            datetime.datetime.strptime(parameters.start_date, "%Y-%m-%d")
            parameters.end_date = input(f"Please enter end date of analysis (YYYY-MM-DD): ")
            datetime.datetime.strptime(parameters.end_date, "%Y-%m-%d")
            parameters.provider = input(f"Please enter provider (callabike / nextbike) or leave blank for both: ")
            print('Please enter thresholds for suspicious rides, leave blank for default threshold')
            min_distance = input(f"Minimum distance in meters (default 50): ")
            # if some input entered: try to convert to int, else leave attribute of parameters object unchanged
            if min_distance: parameters.min_distance = int(min_distance)
            max_distance = input(f"Maximum distance in meters (default 15000): ")
            if max_distance: parameters.max_distance = int(max_distance)
            min_duration = input(f"Minimum duration in minutes (default 3): ")
            if min_duration: parameters.min_duration = int(min_duration)
            max_duration = input(f"Maximum duration in minutes (default 1440): ")
            if max_duration: parameters.max_duration = int(max_duration)
            flagSameStationAsSuspicious = input(f"Rides starting and ending at the same station (y to mark it as suspicious, default no): ")
            if flagSameStationAsSuspicious == "y": parameters.flagSameStationAsSuspicious = True
            maxSimultaneousPickupCount = input(f"Maximum number of bikes picked up at the same time (default not limited): ")
            if maxSimultaneousPickupCount: parameters.maxSimultaneousPickupCount = int(maxSimultaneousPickupCount)
        # if invalid value is entered, print error message and restart input
        except ValueError:
            print("Please enter a valid value! Try again:")
            self.getInputFromTerminal(parameters)

# data class to represent a bike ride
# do not auto-generate init method as attributes will be set by getAllRides() method
@dataclass(init=False)
class BikeRide:
    bike_ride_id: str
    distance: float
    start_station_id: str
    end_station_id: str
    since: datetime
    until: datetime

# class to handle all bike rides (storing, and analyzing)
class BikeRides:
    rides_list = []
    def add(self, bikeRide):
        self.rides_list.append(bikeRide)
    def analyzeAllRides(self, parameters, rideCounts):
        for bikeRide in self.rides_list:
            rideAnalyzer = RideAnalyzer(bikeRide)
            rideAnalyzer.analyzeRide(parameters, rideCounts)

# class to analyze a given ride
class RideAnalyzer:
    def __init__(self, bikeRide):
        self.bikeRide = bikeRide
    def analyzeRide(self, parameters, rideCounts):
        # boolean if start station is not null and ride started and ended at the same station
        self.bikeRide.startAndEndSameStations = (self.bikeRide.start_station_id is not None and self.bikeRide.start_station_id == self.bikeRide.end_station_id)
        # get duration in minutes (as integer)
        self.bikeRide.duration = int((self.bikeRide.until - self.bikeRide.since).total_seconds() // 60)
        # get number of simultaneous pickups at the minute of ride pickup
        self.bikeRide.simultaneousRidesCount = rideCounts[self.bikeRide.provider][str(self.bikeRide.since)]
        # evaluate whether ride was suspicious or not
        self.evaluateRide(parameters)
    def evaluateRide(self, parameters):
        # initialize suspicious flag with False
        self.bikeRide.suspicious = False
        # check all threshholds to determine whether or not ride is suspicious
        # by looping through all subclasses of threshhold checker (replaces checkClassList!)
        # checkClassList = [ShortDistanceChecker, LongDistanceChecker, ShortDurationChecker, LongDurationChecker, SameStationChecker, SimultaneousRidesChecker]
        for CheckClass in ThreshholdChecker.__subclasses__():
            # check thresshold using check class
            threshholdChecker = CheckClass()
            threshholdChecker.checkSuspicious(self.bikeRide,parameters)

# abstract class for all check classes
class ThreshholdChecker(ABC):
    @abstractmethod
    def checkSuspicious(self, ride, parameters):
        pass
# define check classes (subclasses of ThreshholdChecker)
class ShortDistanceChecker(ThreshholdChecker):
    def checkSuspicious(self, ride, parameters):
        # set suspicious flag to True if thresholds is violated
        if ride.distance < parameters.min_distance:
            ride.suspicious = True
class LongDistanceChecker(ThreshholdChecker):
    def checkSuspicious(self, ride, parameters):
        if ride.distance > parameters.max_distance:
            ride.suspicious = True
class ShortDurationChecker(ThreshholdChecker):
    def checkSuspicious(self, ride, parameters):
        if ride.duration < parameters.min_duration:
            ride.suspicious = True
class LongDurationChecker(ThreshholdChecker):
    def checkSuspicious(self, ride, parameters):
        if ride.duration > parameters.max_duration:
            ride.suspicious = True
class SameStationChecker(ThreshholdChecker):
    def checkSuspicious(self, ride, parameters):
        if parameters.flagSameStationAsSuspicious and ride.startAndEndSameStations:
            ride.suspicious = True
class SimultaneousRidesChecker(ThreshholdChecker):
    def checkSuspicious(self, ride, parameters):
        if parameters.maxSimultaneousPickupCount:
            if ride.simultaneousRidesCount > parameters.maxSimultaneousPickupCount:
                ride.suspicious = True

if __name__ == "__main__":
    main()

