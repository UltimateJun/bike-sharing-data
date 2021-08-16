from mysql.connector import connect, Error
from datetime import datetime
import os, os.path, datetime, json
from json.decoder import JSONDecodeError
import pprint

def main():

    # create MySQL connection and cursor
    connection = connect(option_files='mysql.conf')
    # connection.autocommit = True
    cursor = connection.cursor()

    # set dates for start and end of parsing
    print('Please enter month of parsing: ')
    parse_month = int(input())
    print('Please enter start day of parsing: ')
    start_day = int(input())
    print('Please enter end day of parsing: ')
    end_day = int(input())

    # get set of station IDs from database
    nextbike_stations_set = getStations(cursor)
    # get last status of all bikes from database
    nextbike_bike_status_dict = getLastStatus(cursor)
    # parse all files in given time range
    nextbike_bike_status_dict, nextbike_new_bikes_list, nextbike_new_stations_list, nextbike_bike_rides_list, nextbike_exception_dict = parseFiles(
        nextbike_bike_status_dict, nextbike_stations_set, parse_month, start_day, end_day)

    # after parsing all files: insert new stations, bikes, status, rides and exceptions into database
    insertIntoDatabase(connection, cursor, nextbike_new_bikes_list, nextbike_new_stations_list, nextbike_bike_status_dict, nextbike_bike_rides_list, nextbike_exception_dict)

def getStations(cursor):
    # get IDs of all existing stations and save in new set
    cursor.execute("SELECT station_id FROM station WHERE provider='nextbike'")
    nextbike_stations_set = {str(station[0]) for station in cursor.fetchall()}
    return nextbike_stations_set

def getLastStatus(cursor):
    # create dictionary with status of all bikes
    nextbike_bike_status_dict = {}
    # get last status (coordinates, station and since time) of all bikes available at the last minute
    cursor.execute("SELECT bike_id, ST_Y(coordinates), ST_X(coordinates), station_id, since FROM bike_last_status NATURAL JOIN bike WHERE provider='nextbike'")
    # go through all retrieved status
    for bike_last_status in cursor.fetchall():
        bike_id = str(bike_last_status[0])
        # get longitude (Y-coordinate) and latitude (X-coordinate)
        lng = str(bike_last_status[1])
        lat = str(bike_last_status[2])
        # if not at a station: set to None (would otherwise result in string 'None')
        station_id = str(bike_last_status[3]) if bike_last_status[3] is not None else None
        since = bike_last_status[4]
        # write status (coordinates, station and since time) to status dictionary
        nextbike_bike_status_dict[bike_id] = [lat, lng, station_id, since]
    return nextbike_bike_status_dict

def parseFiles(nextbike_bike_status_dict, nextbike_stations_set, parse_month, start_day, end_day):
    
    # lists for bikes and stations that do not exist yet (not a set as list is required for SQL insertion)
    nextbike_new_bikes_list = []
    nextbike_new_stations_list = []
    # list for calculated bike rides, dictionary for exceptions
    nextbike_bike_rides_list = []
    nextbike_exception_dict = dict()

    for day in range(start_day, end_day+1):
        dateOfParse = '2021-' + f'{parse_month:02d}' + '-' + f'{day:02d}'
        print(dateOfParse + ' started at ' + datetime.datetime.now().strftime("%H:%M:%S"))
        # for all minutes of that date
        for hour in range(0,24):
            for minute in range(0,60):
                # format date and time strings (YYYY-MM-DD and HH-MM), append to directory path
                time_hhmm = f'{hour:02d}' + '-' + f'{minute:02d}'
                DIR = 'json/' + dateOfParse + '/' + time_hhmm;
                # timestamp of batch of files currently being processed, and timestamp of previous minute
                datetime_current = datetime.datetime.strptime(dateOfParse + '-' +  time_hhmm, '%Y-%m-%d-%H-%M')
                datetime_last_minute = datetime_current - datetime.timedelta(minutes=1)
                # if given directory exists
                if os.path.exists(DIR):
                    # NEXTBIKE
                    try:
                        json_path = DIR+'/nextbike.json'
                        if os.path.exists(json_path):
                            # parse bike rides and new status from given file
                            nextbike_bike_status_dict, nextbike_bike_rides_list, nextbike_new_bikes_list, nextbike_new_stations_list = parseFile(json_path, nextbike_bike_status_dict, nextbike_stations_set,
                                nextbike_new_bikes_list, nextbike_new_stations_list, nextbike_bike_rides_list, datetime_last_minute, datetime_current)
                        # if nextbike.json does not exist in directory -> API exception!
                        else:
                            handleAPIException("FileMissing", datetime_current, datetime_last_minute, nextbike_exception_dict, nextbike_bike_status_dict)
                            # break the loop of all files of the minute

                    # catch JSON errors and print them with file path
                    except JSONDecodeError as e:
                        handleAPIException("JSONDecodeError", datetime_current, datetime_last_minute, nextbike_exception_dict, nextbike_bike_status_dict)
                    except ValueError as e:
                        handleAPIException("ValueError", datetime_current, datetime_last_minute, nextbike_exception_dict, nextbike_bike_status_dict)
                    except KeyError as e:
                        handleAPIException("KeyError", datetime_current, datetime_last_minute, nextbike_exception_dict, nextbike_bike_status_dict)
                    except TypeError as e:
                        handleAPIException("TypeError", datetime_current, datetime_last_minute, nextbike_exception_dict, nextbike_bike_status_dict)
                    except IndexError as e:
                        handleAPIException("IndexError", datetime_current, datetime_last_minute, nextbike_exception_dict, nextbike_bike_status_dict)
                    except Error as e:
                        handleAPIException("Error", datetime_current, datetime_last_minute, nextbike_exception_dict, nextbike_bike_status_dict)
    return nextbike_bike_status_dict, nextbike_new_bikes_list, nextbike_new_stations_list, nextbike_bike_rides_list, nextbike_exception_dict

def parseFile(json_path, nextbike_bike_status_dict, nextbike_stations_set, nextbike_new_bikes_list, nextbike_new_stations_list, nextbike_bike_rides_list, datetime_last_minute, datetime_current):
    # open JSON and write subarray 'places' to array bikes
    with open(json_path) as bike_json:
        jsonObject = json.load(bike_json)
        bike_json.close()
    places = jsonObject['countries'][0]['cities'][0]['places']

    for place in places:
        # get coordinates of the place
        lat = str(place['lat'])
        lng = str(place['lng'])

        # if flag "spot" is set to true -> place is a station, not a free-floating bike
        if place['spot']:
            station_id = str(place['uid'])
            # if station not yet in database
            if station_id not in nextbike_stations_set:
                # retrieve station data and add to nextbike station list
                # Format for MySQL ST_GeomFromText and SRID 4326: Point(52.53153 13.38651) -> in reverse order!
                station_coordinates = 'Point(' + lat + ' ' + lng + ')'
                station_name = place['name']
                station_provider = 'nextbike'
                station_capacity = str(place['bike_racks'])
                # append new station to new stations list
                # nextbike_new_stations_list.append(list([station_id,station_coordinates,station_name,station_provider,station_capacity]))
                nextbike_new_stations_list.append((station_id,station_coordinates,station_name,station_provider,station_capacity))
                nextbike_stations_set.add(station_id)
        # otherwise, set station of free-floating bike to None
        else:
            station_id = None

        # go through all bikes in bike_list
        for bike in place['bike_list']:
            # get bike ID
            bike_id = str(bike['number'])

            # if bike doesn't exist in database yet
            if bike_id not in nextbike_bike_status_dict.keys():
                # add bike in a tuple (, required) to list of new bikes
                nextbike_new_bikes_list.append([bike_id])
            # else (= exists) if "since" timestamp of current status is NOT equal to the previous minute 
            # => bike was not previously available and therefore gone
            elif(nextbike_bike_status_dict[bike_id][3] != datetime_last_minute):
                # parse bike ride details and append to list
                # current coordinates / station / datetime will be end coordinates / station / datetime of ride
                nextbike_bike_rides_list = bikeRideToList(nextbike_bike_rides_list, nextbike_bike_status_dict, bike_id, lat, lng, station_id, datetime_current)
            # update new available status in dictionary (or create new status if not yet existent)
            nextbike_bike_status_dict[bike_id] = [lat, lng, station_id, datetime_current]
    return nextbike_bike_status_dict, nextbike_bike_rides_list, nextbike_new_bikes_list, nextbike_new_stations_list

def bikeRideToList(nextbike_bike_rides_list, nextbike_bike_status_dict, bike_id, end_lat, end_lng, end_station, datetime_current):
    start_station =  nextbike_bike_status_dict[bike_id][2]
    # bike gone since: one minute after the last time it was available
    since = nextbike_bike_status_dict[bike_id][3] + datetime.timedelta(minutes=1)

    # get ride details (start and end coordinates, start and end station)
    # get last saved coordinates and station from status (will be start coordinates and station)
    start_coordinates_X = nextbike_bike_status_dict[bike_id][0]
    start_coordinates_Y = nextbike_bike_status_dict[bike_id][1]
    start_coordinates = 'POINT(' + start_coordinates_X + ' ' + start_coordinates_Y + ')'
    
    end_coordinates = 'POINT(' + end_lat + ' ' + end_lng + ')'

    # write ride details (+ timestamps) to list
    # bike ride tuple: bike ID, start coordinates, start station, end coordinates, end station, since, until
    nextbike_bike_rides_list.append((bike_id, start_coordinates, end_coordinates, start_station, end_station, since, datetime_current))

    return nextbike_bike_rides_list

def handleAPIException(error_type, datetime_current, datetime_last_minute, nextbike_exception_dict, nextbike_bike_status_dict):
    # convert current datetime to MySQL format
    timestamp_mysql = datetime_current.strftime('%Y-%m-%d %H:%M:%S')
    # make sure that exception with same timestamp does not exist already
    if timestamp_mysql not in nextbike_exception_dict:
        # set status of all bikes that were previously (at the last minute) available to current timestamp
        # that way, the missing files do not cause a gap between status timestamps, which would result in a ride
        for bike in nextbike_bike_status_dict.values():
            # if bike was available at the previous minute
            if bike[3] == datetime_last_minute:
                # manually set since-timestamp of bike to current time
                bike[3] = datetime_current
        # write error type with timestamp to exception dictionary
        nextbike_exception_dict[timestamp_mysql] = error_type

# %%
def insertIntoDatabase(connection, cursor, nextbike_new_bikes_list, nextbike_new_stations_list, nextbike_bike_status_dict, nextbike_bike_rides_list, nextbike_exception_dict):
    
    # set up pretty printer with indentation
    pp = pprint.PrettyPrinter(indent=4)

    # run SQL query to insert new stations into station table
    cursor.executemany("INSERT INTO station (station_id, coordinates, name, provider, capacity) VALUES (%s,  ST_GeomFromText(%s, 4326), %s, %s, %s) ON DUPLICATE KEY UPDATE station_id=station_id", nextbike_new_stations_list)
    print('Inserting new stations done!')

    print('New bikes:')
    pp.pprint(nextbike_new_bikes_list)
    cursor.executemany("INSERT INTO bike (bike_id, provider) VALUES (%s, 'nextbike') ON DUPLICATE KEY UPDATE bike_id=bike_id", nextbike_new_bikes_list)
    print('Inserting bikes done!')

    # insert bike ride list into bike_ride table
    # ignore (ON DUPLICATE KEY UPDATE bike_id=bike_id) if combination of bike_id and since exists already!
    # ensures idempotence as rides cannot be duplicated
    cursor.executemany("INSERT INTO bike_ride (bike_id, start_coordinates, end_coordinates, start_station_id, end_station_id, since, until) VALUES (%s, ST_GeomFromText(%s, 4326), ST_GeomFromText(%s, 4326), %s, %s, %s, %s) ON DUPLICATE KEY UPDATE bike_id=bike_id", nextbike_bike_rides_list)
    print('Inserting bike rides done!')

    last_bike_status_list = lastBikeStatusToList(nextbike_bike_status_dict)
    # overwrite (REPLACE) bike_last_status table with new status list
    cursor.executemany("REPLACE INTO bike_last_status (bike_id, coordinates, station_id, since) VALUES (%s, ST_GeomFromText(%s, 4326), %s, %s)", last_bike_status_list)
    print('Inserting last bike status done!')

    # convert exception dictionary to list and insert into exception table
    nextbike_exception_list = [(value, key) for key, value in nextbike_exception_dict.items()]
    cursor.executemany("INSERT INTO exception (provider, error_type, time) VALUES ('nextbike', %s, %s) ON DUPLICATE KEY UPDATE exception_id=exception_id", nextbike_exception_list)
    print('Exceptions:')
    pp.pprint(nextbike_exception_list)
    print('Inserting exceptions done!')

    # commit queries and close the connection
    connection.commit()
    connection.close()

# %%
def lastBikeStatusToList(nextbike_bike_status_dict):
    last_bike_status_list = []
    # go through all bike status with key (bike_id) and value (bike: list with status of that bike)
    for bike_id, bike in nextbike_bike_status_dict.items():
        # get coordinates
        coordinates_X = bike[0]
        coordinates_Y = bike[1]
        # get coordinates into MySQL POINT format
        coordinates = 'POINT(' + coordinates_X + ' ' + coordinates_Y + ')'
        # write status (position, station and since) as tuple to status list
        last_bike_status_list.append((bike_id, coordinates, bike[2], str(bike[3])))
    return last_bike_status_list

if __name__ == "__main__":
    main()