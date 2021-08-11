from mysql.connector import connect
import MySQLConf
import pprint
import sys
import datetime
from dataclasses import dataclass
from abc import ABC, abstractmethod

# TODO: more than x bikes taken at the same time (another query: count group by minute)

@dataclass
class parametersInput:
    # parameters can either be set directly on the object or from terminal using following function
    def getInputFromTerminal(self):
        try:
            self.start_date = input(f"Please enter start date of analysis (YYYY-MM-DD): ")
            # try to convert date string -> throws exception if format is wrong
            datetime.datetime.strptime(self.start_date, "%Y-%m-%d")
            self.end_date = input(f"Please enter end date of analysis (YYYY-MM-DD): ")
            datetime.datetime.strptime(self.end_date, "%Y-%m-%d")
            self.provider = input(f"Please enter provider (callabike / nextbike) or leave blank for both: ")
            print('Please enter thresholds for suspicious rides, leave blank for default threshold')
            self.min_distance = input(f"Minimum distance in meters (default 50): ")
            self.min_distance = int(self.min_distance) if self.min_distance else None
            self.max_distance = input(f"Maximum distance in meters (default 15000): ")
            self.max_distance = int(self.max_distance) if self.max_distance else None
            self.min_duration = input(f"Minimum duration in minutes (default 3): ")
            self.min_duration = int(self.min_duration) if self.min_duration else None
            self.max_duration = input(f"Maximum duration in minutes (default 1440): ")
            self.max_duration = int(self.max_duration) if self.max_duration else None
            self.flagSameStationAsSuspicious = input(f"Rides starting and ending at the same station (y to mark it as suspicious, default no): ")
        # if invalid value is entered, print error message and restart input
        except ValueError:
            print("Please enter a valid value! Try again:")
            self.getInputFromTerminal()

@dataclass
class BikeRides:
    rides_list = []
    def __init__(self, ride):
        self.bike_ride_id = ride.bike_ride_id
        self.distance = ride.distance
        self.start_station_id = ride.start_station_id
        self.end_station_id = ride.end_station_id
        self.since = ride.since
        self.until = ride.until
        self.rides_list.append(self)
    @classmethod
    def analyzeAllRides(cls, parameters):
        for bikeRide in cls.rides_list:
            rideAnalyzer = RideAnalyzer(bikeRide)
            rideAnalyzer.analyzeRide(parameters)
    @classmethod
    def allAnalyzedRidesToList(cls):
        ride_analysis_list = [(self.bike_ride_id, self.duration, self.distance, self.ride_StartEndSameStations, self.suspicious) for self in cls.rides_list]
        return ride_analysis_list

class RideAnalyzer():
    def __init__(self, bikeRide):
        self.bikeRide = bikeRide
    def analyzeRide(self, parameters):
        # boolean if start station is not null and ride started and ended at the same station
        self.bikeRide.ride_StartEndSameStations = (self.bikeRide.start_station_id is not None and self.bikeRide.start_station_id == self.bikeRide.end_station_id)
        # get duration in minutes (as integer)
        self.bikeRide.duration = int((self.bikeRide.until - self.bikeRide.since).total_seconds() // 60)
        # evaluate whether ride was suspicious or not
        self.bikeRide.ride_suspicious = self.evaluateRide(parameters)
    def evaluateRide(self, parameters):
        # initialize suspicious flag with False
        self.bikeRide.suspicious = False
        # list of all checks to be made
        checkClassList = [shortDistanceChecker, longDistanceChecker, shortDurationChecker, longDurationChecker, sameStationChecker]
        for checkClass in checkClassList:
            threshholdChecker = checkClass()
            threshholdChecker.checkSuspicious(self.bikeRide,parameters)

# abstract class for all check classes
class threshholdChecker(ABC):
    @abstractmethod
    def checkSuspicious(self, ride, parameters):
        pass

# define check classes (subclasses of threshholdChecker)
class shortDistanceChecker(threshholdChecker):
    def checkSuspicious(self, ride, parameters):
    # set parameters to terminal input if provided, otherwise to default values
        min_distance = 50 if not parameters.min_distance else parameters.min_distance
        # set suspicious flag to True if thresholds is violated
        if ride.distance < min_distance:
            ride.suspicious = True
class longDistanceChecker(threshholdChecker):
    def checkSuspicious(self, ride, parameters):
        max_distance = 15000 if not parameters.max_distance else parameters.max_distance
        if ride.distance > max_distance:
            ride.suspicious = True
class shortDurationChecker(threshholdChecker):
    def checkSuspicious(self, ride, parameters):
        min_duration = 3 if not parameters.min_duration else parameters.min_duration
        if ride.duration < min_duration:
            ride.suspicious = True
class longDurationChecker(threshholdChecker):
    def checkSuspicious(self, ride, parameters):
        max_duration = 1440 if not parameters.max_duration else parameters.max_duration
        if ride.duration > max_duration:
            ride.suspicious = True
class sameStationChecker(threshholdChecker):
    def checkSuspicious(self, ride, parameters):
        flagSameStationAsSuspicious = True if parameters.flagSameStationAsSuspicious=="y" else False
        if flagSameStationAsSuspicious and ride.ride_StartEndSameStations:
            ride.suspicious = True

def main():
    # create MySQL connection and cursor
    connection = connect(option_files='mysql.conf')
    connection.autocommit = True
    cursor = connection.cursor(named_tuple=True)

    # get parameters from terminal input
    parameters = parametersInput()
    parameters.getInputFromTerminal()
    # get all rides from database
    getAllRides(cursor, parameters)
    
    # analyze and evaluate all resulting rides
    BikeRides.analyzeAllRides(parameters)
    # insert analysis into database
    ride_analysis_list = BikeRides.allAnalyzedRidesToList()
    insertAnalysis(ride_analysis_list, connection, cursor)

def getAllRides(cursor, parameters):
    # if provider was given: add string with natural join and provider-clause in WHERE, else add only WHERE
    where_provider = "WHERE" if not parameters.provider else "NATURAL JOIN bike WHERE provider='" + parameters.provider + "' AND"
    # build SQL query and execute
    cursor.execute("SELECT bike_ride_id, ST_Distance_Sphere(start_coordinates, end_coordinates) AS distance, start_station_id, end_station_id, since, until FROM bike_ride "+where_provider+" since BETWEEN '"+parameters.start_date+" 00:00:00' AND '"+parameters.end_date+" 23:59:59'")
    for ride in cursor:
        bikeRide = BikeRides(ride)

def insertAnalysis(ride_analysis_list, connection, cursor):
    # insert list with all analysis of rides into ride_analysis table
    cursor.executemany("INSERT INTO ride_analysis VALUES (%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE bike_ride_id=bike_ride_id", ride_analysis_list)
    connection.commit()
    print('Inserting ride analysis done!')
    # close the MySQL connection
    connection.close()

if __name__ == "__main__":
    main()

