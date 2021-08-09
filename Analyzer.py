from mysql.connector import connect
import pprint

# TODO: more than x bikes taken at the same time (another query: count group by minute)

def main():
    # create MySQL connection and cursor
    connection = connect(option_files='mysql.conf')
    connection.autocommit = True
    cursor = connection.cursor()

    # get parameters from terminal input
    start_date, end_date, provider, min_distance, max_distance, min_duration, max_duration, flagSameStationAsSuspicious = getInput()
    # get all rides from database
    bike_rides_list = getAllRides(cursor, start_date, end_date, provider)
    
    # set up pretty printer with indentation
    pp = pprint.PrettyPrinter(indent=4)
    # pp.pprint(bike_rides_list)

    # analyze and evaluate all resulting rides
    ride_analysis_list = analyzeRides(bike_rides_list, min_distance, max_distance, min_duration, max_duration, flagSameStationAsSuspicious)
    pp.pprint(ride_analysis_list)
    # insert analysis into database
    insertAnalysis(ride_analysis_list, connection, cursor)
    # close the MySQL connection
    connection.close()

def getInput():
    # request input from terminal
    start_date = input(f"Please enter start date of analysis (YYYY-MM-DD): ")
    end_date = input(f"Please enter end date of analysis (YYYY-MM-DD): ")
    provider = input(f"Please enter provider (callabike / nextbike) or leave blank for both: ")
    print('Please enter thresholds for suspicious rides, leave blank for default threshold')
    min_distance = input(f"Minimum distance (default 50m): ")
    max_distance = input(f"Maximum distance (default 15000m): ")
    min_duration = input(f"Minimum duration in minutes (default 3): ")
    max_duration = input(f"Maximum duration in minutes (default 1440): ")
    flagSameStationAsSuspicious = input(f"Rides starting and ending at the same station (y to mark it as suspicious, default no): ")

    return start_date, end_date, provider, min_distance, max_distance, min_duration, max_duration, flagSameStationAsSuspicious

def getAllRides(cursor, start_date, end_date, provider):
    # if provider was given: add string with natural join and provider-clause in WHERE, else add only WHERE
    where_provider = "WHERE" if not provider else "NATURAL JOIN bike WHERE provider='" + provider + "' AND"
    # build SQL query and execute
    cursor.execute("SELECT bike_ride_id, ST_Distance_Sphere(start_coordinates, end_coordinates), start_station_id, end_station_id, since, until FROM bike_ride "+where_provider+" since BETWEEN '"+start_date+" 00:00:00' AND '"+end_date+" 23:59:59'")
    bike_rides_list = cursor.fetchall()
    return bike_rides_list

def analyzeRides(bike_rides_list, min_distance, max_distance, min_duration, max_duration, flagSameStationAsSuspicious):
    ride_analysis_list = []
    # go through all rides in list
    for ride in bike_rides_list:
        ride_distance = ride[1]
        # boolean if ride started and ended at the same station
        ride_StartEndSameStations = ride[2] == ride[3]
        # get duration in minutes (as integer)
        ride_duration = int((ride[5] - ride[4]).total_seconds() // 60)
        # evaluate whether ride was suspicious or not
        ride_suspicious = evaluateRide(ride_distance, ride_StartEndSameStations, ride_duration, min_distance, max_distance, min_duration, max_duration, flagSameStationAsSuspicious)
        # write ride ID, duration (in minutes), distance (in km), same station boolean and suspicious boolean to list
        ride_analysis_list.append([ride[0], ride_duration, ride_distance/1000, ride_StartEndSameStations, ride_suspicious])
    return ride_analysis_list

def evaluateRide(ride_distance, ride_StartEndSameStations, ride_duration, min_distance, max_distance, min_duration, max_duration, flagSameStationAsSuspicious):
    # initialize suspicious flag with False
    suspicious = False

    # set parameters to terminal input if provided, otherwise to default values
    min_distance = 50 if not min_distance else min_distance
    max_distance = 15000 if not max_distance else max_distance
    min_duration = 3 if not min_duration else min_duration
    max_duration = 1440 if not max_duration else max_duration
    flagSameStationAsSuspicious = True if flagSameStationAsSuspicious=="y" else False

    # set suspicious flag to True if any of the thresholds is violated
    suspicious = True if ride_distance < min_distance else suspicious
    suspicious = True if ride_distance > max_distance else suspicious
    suspicious = True if ride_duration < min_duration else suspicious
    suspicious = True if ride_duration > max_duration else suspicious
    suspicious = True if flagSameStationAsSuspicious and ride_StartEndSameStations else suspicious
    
    return suspicious

def insertAnalysis(ride_analysis_list, connection, cursor):
    # insert list with all analysis of rides into ride_analysis table
    cursor.executemany("INSERT INTO ride_analysis VALUES (%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE bike_ride_id=bike_ride_id", ride_analysis_list)
    connection.commit()
    print('Inserting ride analysis done!')

if __name__ == "__main__":
    main()

