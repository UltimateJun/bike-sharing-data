from mysql.connector import connect
import MySQLConf
import pprint
from dataclasses import dataclass

# TODO: more than x bikes taken at the same time (another query: count group by minute)

@dataclass
class parametersInput():
    def getInputFromTerminal(self):
        self.start_date = input(f"Please enter start date of analysis (YYYY-MM-DD): ")
        self.end_date = input(f"Please enter end date of analysis (YYYY-MM-DD): ")
        self.provider = input(f"Please enter provider (callabike / nextbike) or leave blank for both: ")
        print('Please enter thresholds for suspicious rides, leave blank for default threshold')
        self.min_distance = input(f"Minimum distance (default 50m): ")
        self.max_distance = input(f"Maximum distance (default 15000m): ")
        self.min_duration = input(f"Minimum duration in minutes (default 3): ")
        self.max_duration = input(f"Maximum duration in minutes (default 1440): ")
        self.flagSameStationAsSuspicious = input(f"Rides starting and ending at the same station (y to mark it as suspicious, default no): ")

@dataclass
class BikeRides():
    rides_list = []
    def __init__(self, ride):
        self.bike_ride_id = ride.bike_ride_id
        self.distance = ride.distance
        self.start_station_id = ride.start_station_id
        self.end_station_id = ride.end_station_id
        self.since = ride.since
        self.until = ride.until
        self.rides_list.append(self)
    def __iter__(self):
        return self
    def analyzeRide(self, parameters):
        # boolean if ride started and ended at the same station
        self.ride_StartEndSameStations = self.start_station_id == self.end_station_id
        # get duration in minutes (as integer)
        self.ride_duration = int((self.until - self.since).total_seconds() // 60)
        # evaluate whether ride was suspicious or not
        self.ride_suspicious = self.evaluateRide(parameters)
    def evaluateRide(self, parameters):
        # initialize suspicious flag with False
        self.suspicious = False
        self.checkShortDistance(parameters)
        self.checkLongDistance(parameters)
        self.checkShortDuration(parameters)
        self.checkLongDuration(parameters)
        self.checkSameStation(parameters)
    def checkShortDistance(self, parameters):
        # set parameters to terminal input if provided, otherwise to default values
        min_distance = 50 if not parameters.min_distance else parameters.min_distance
        # set suspicious flag to True if thresholds is violated
        if self.distance < min_distance:
            self.suspicious = True
    def checkLongDistance(self, parameters):
        max_distance = 15000 if not parameters.max_distance else parameters.max_distance
        if self.distance > max_distance:
            self.suspicious = True
    def checkShortDuration(self, parameters):
        min_duration = 3 if not parameters.min_duration else parameters.min_duration
        if self.distance < min_duration:
            self.suspicious = True
    def checkLongDuration(self, parameters):
        max_duration = 1440 if not parameters.max_duration else parameters.max_duration
        if self.distance > max_duration:
            self.suspicious = True
    def checkSameStation(self, parameters):
        flagSameStationAsSuspicious = True if parameters.flagSameStationAsSuspicious=="y" else False
        if flagSameStationAsSuspicious and self.ride_StartEndSameStations:
            self.suspicious = True
    @classmethod
    def analyzeAllRides(cls, parameters):
        for bikeRide in cls.rides_list:
            bikeRide.analyzeRide(parameters)
    @classmethod
    def allAnalyzedRidesToList(cls):
        ride_analysis_list = [(self.bike_ride_id, self.ride_duration, self.distance, self.ride_StartEndSameStations, self.suspicious) for self in cls.rides_list]
        return ride_analysis_list

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
    
    # set up pretty printer with indentation
    pp = pprint.PrettyPrinter(indent=4)
    # pp.pprint(bike_rides_list)

    # analyze and evaluate all resulting rides
    BikeRides.analyzeAllRides(parameters)
    # insert analysis into database
    ride_analysis_list = BikeRides.allAnalyzedRidesToList()
    insertAnalysis(ride_analysis_list, connection, cursor)
    # close the MySQL connection
    connection.close()

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

if __name__ == "__main__":
    main()

