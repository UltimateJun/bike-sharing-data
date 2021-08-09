from mysql.connector import connect, Error
import os, os.path, json
import geopy.distance
import datetime
import pprint

# TODO: flag rides where exception has occured

def main():
    # create MySQL connection and cursor
    connection = connect(option_files='mysql.conf')
    connection.autocommit = True
    cursor = connection.cursor()

    # set up pretty printer
    pp = pprint.PrettyPrinter(indent=4)

    # get parameters from terminal input
    parameters_dict = getInput(getDistricts(cursor))
    pp.pprint(parameters_dict)
    # get list of rides from database
    ride_list = getRides(parameters_dict, cursor)
    # pp.pprint(ride_list)
    # close the connection
    connection.close()
    # generate JSON with all rides
    generateJSONofRides(ride_list)

def getDistricts(cursor):
    # get all district IDs and names to district list
    cursor.execute("SELECT district_id, name FROM district")
    district_list = cursor.fetchall()
    return district_list

def getInput(district_list):
    parameters_dict = {}
    # request parameters from terminal input
    print('Please enter filter parameters of your query! Leave blank to skip that filter.')
    parameters_dict['since_min'] = input(f"Ride beginning at YYYY-MM-DD HH:MM or later: ")
    parameters_dict['since_max'] = input(f"Ride beginning at YYYY-MM-DD HH:MM or earlier: ")
    parameters_dict['until_min'] = input(f"Ride ending at YYYY-MM-DD HH:MM or later: ")
    parameters_dict['until_max'] = input(f"Ride ending at YYYY-MM-DD HH:MM or earlier: ")
    parameters_dict['time_min'] = input(f"Ride beginning at HH:MM or later: ")
    parameters_dict['time_max'] = input(f"Ride beginning at HH:MM or earlier: ")
    parameters_dict['weekday_weekend'] = input(f"Ride beginning on a weekday (wd) or weekend (we): ")
    parameters_dict['provider'] = input(f"Provider (callabike / nextbike): ")
    parameters_dict['requireStartStation'] = input(f"Ride starting at a station (y / n): ")
    parameters_dict['requireEndStation'] = input(f"Ride ending at a station (y / n): ")
    parameters_dict['requireEitherStation'] = input(f"Ride starting and/or ending at a station (y / n): ")
    parameters_dict['start_coordinates'] = input(f"Ride starting in area - center coordinates (format: 52.52 13.42): ")
    # also request start radius if if start coordinates were entered
    if parameters_dict['start_coordinates']:
        parameters_dict['start_radius'] = input(f"Ride starting in area - radius (in m) around center coordinate: ")
    parameters_dict['end_coordinates'] = input(f"Ride ending in area - center coordinates (format: 52.52 13.42): ")
    # also request end radius if if end coordinates were entered
    if parameters_dict['end_coordinates']:
        parameters_dict['end_radius'] = input(f"Ride ending in area - radius (in m) around center coordinate: ")
    print("Here's a list of the districts in Berlin:")
    for district in district_list:
        print(str(district[0]) + ': ' + district[1])
    parameters_dict['start_district'] = input(f"Ride beginning in district number: ")
    parameters_dict['end_district'] = input(f"Ride ending in district number: ")
    includeNearbyStations = input(f"Should stations in proximity of drop-off point also be retrieved? (y/n): ")
    parameters_dict['includeNearbyStations'] = includeNearbyStations
    parameters_dict['proximityStations'] = input(f"Stations in proximity - radius (in m) around drop-off point: ") if includeNearbyStations == "y" else ""

    return parameters_dict

def getRides(parameters_dict, cursor):
    
    # create list for query (more efficient than concatenating strings with += as only one string is generated)
    ride_query = []
    # append query in its most basic form
    ride_query.append("SELECT bike_ride.bike_ride_id, bike_id, ST_X(start_coordinates), ST_Y(start_coordinates), ST_X(end_coordinates), ST_Y(end_coordinates), start_station_id, end_station_id, distance_km, since, until, duration_min, suspicious, provider FROM bike_ride LEFT JOIN ride_analysis ON bike_ride.bike_ride_id=ride_analysis.bike_ride_id NATURAL JOIN bike")
    # create instance of class QueryConnector
    queryConnector = QueryConnector()

    # generate MySQL clause strings depending on given parameters
    if parameters_dict['since_min']:
        query_parameter = "since >= '" + parameters_dict['since_min'] + "'"
        ride_query = queryConnector.appendParameter(ride_query, query_parameter)
    if parameters_dict['since_max']:
        query_parameter = "since <= '" + parameters_dict['since_max'] + "'"
        ride_query = queryConnector.appendParameter(ride_query, query_parameter)
    if parameters_dict['until_min']:
        query_parameter = "until >= '" + parameters_dict['until_min'] + "'"
        ride_query = queryConnector.appendParameter(ride_query, query_parameter)
    if parameters_dict['until_max']:
        query_parameter = "until <= '" + parameters_dict['until_max'] + "'"
        ride_query = queryConnector.appendParameter(ride_query, query_parameter)
    if parameters_dict['time_min']:
        query_parameter = "TIME(since) >= '" + parameters_dict['time_min'] + "'"
        ride_query = queryConnector.appendParameter(ride_query, query_parameter)
    if parameters_dict['time_max']:
        query_parameter = "TIME(since) <= '" + parameters_dict['time_max'] + "'"
        ride_query = queryConnector.appendParameter(ride_query, query_parameter)
    if parameters_dict['weekday_weekend']:
        if parameters_dict['weekday_weekend'] == 'wd':
            # condition: weekday of since is any day from Monday to Friday
            query_parameter = "WEEKDAY(since) IN (0,1,2,3,4)"
            ride_query = queryConnector.appendParameter(ride_query, query_parameter)
        if parameters_dict['weekday_weekend'] == 'we':
            # condition: weekday of since is either Saturday or Sunday
            query_parameter = "WEEKDAY(since) IN (5,6)"
            ride_query = queryConnector.appendParameter(ride_query, query_parameter)
    if parameters_dict['provider']:
        query_parameter = "provider = '" + parameters_dict['provider'] + "'"
        ride_query = queryConnector.appendParameter(ride_query, query_parameter)
    if parameters_dict['requireStartStation'] == "y":
        query_parameter = "start_station_id IS NOT NULL"
        ride_query = queryConnector.appendParameter(ride_query, query_parameter)
    if parameters_dict['requireEndStation'] == "y":
        query_parameter = "end_station_id IS NOT NULL"
        ride_query = queryConnector.appendParameter(ride_query, query_parameter)
    if parameters_dict['requireEitherStation'] == "y":
        query_parameter = "(start_station_id IS NOT NULL OR end_station_id IS NOT NULL)"
        ride_query = queryConnector.appendParameter(ride_query, query_parameter)
    if parameters_dict['start_coordinates']:
        mySqlGeom = "ST_GeomFromText('POINT(" + parameters_dict['start_coordinates'] + ")', 4326)"
        query_parameter = "ST_Distance_Sphere(start_coordinates, "+mySqlGeom+") < " + parameters_dict['start_radius']
        ride_query = queryConnector.appendParameter(ride_query, query_parameter)
    if parameters_dict['end_coordinates']:
        mySqlGeom = "ST_GeomFromText('POINT(" + parameters_dict['end_coordinates'] + ")', 4326)"
        query_parameter = "ST_Distance_Sphere(start_coordinates, "+mySqlGeom+") < " + parameters_dict['end_radius']
        ride_query = queryConnector.appendParameter(ride_query, query_parameter)
    if parameters_dict['start_district']:
        query_parameter = "ST_Within(start_coordinates, " + "(SELECT area FROM district WHERE district_id=" + parameters_dict['start_district'] + "))"
        ride_query = queryConnector.appendParameter(ride_query, query_parameter)
    if parameters_dict['end_district']:
        query_parameter = "ST_Within(end_coordinates, " + "(SELECT area FROM district WHERE district_id=" + parameters_dict['end_district'] + "))"
        ride_query = queryConnector.appendParameter(ride_query, query_parameter)

    ride_query_string = ' '.join(ride_query)
    print(ride_query_string)
    cursor.execute(ride_query_string)
    ride_list = [[column for column in ride] for ride in cursor.fetchall()]

    if parameters_dict['includeNearbyStations'] == "y":
        if parameters_dict['proximityStations']:
            ride_list = getNearbyStations(ride_list, int(parameters_dict['proximityStations']), cursor)
    
    return ride_list

# class maintaining the string connecting the WHERE clauses
class QueryConnector():
    # when initiated for the first time: append WHERE next
    def __init__(self):
        self.connector_string = "WHERE"
    # after the first clause: change connector to AND
    def appendConnector(self, ride_query):
        ride_query.append(self.connector_string)
        self.connector_string = "AND"
        return ride_query
    # appending a new parameter
    def appendParameter(self, ride_query, query_parameter):
        # append connector string (WHERE or AND) first
        ride_query = self.appendConnector(ride_query)
        # append given query parameter
        ride_query.append(query_parameter)
        return ride_query
        
def getNearbyStations(ride_list, proximityStations, cursor):
    cursor.execute("SELECT station_id, name, ST_X(coordinates), ST_Y(coordinates), provider FROM station")
    station_list = cursor.fetchall()
    cursor.execute("SELECT bike_id, provider FROM bike")
    bike_provider_dict = {bike[0]:bike[1] for bike in cursor.fetchall()}
    

    for ride_index in range(len(ride_list)): 
        # get coordinates of drop-off point into tuple
        end_coords = (ride_list[ride_index][4], ride_list[ride_index][5])
        # get provider of bike from dictionary
        provider = bike_provider_dict[ride_list[ride_index][1]]

        # go through all stations
        for station in station_list:
            # only check stations of same provider
            if station[4] == provider:
                station_coords = (station[2], station[3])
                station_distance = geopy.distance.distance(end_coords, station_coords).meters
                # if station is within given distance
                if station_distance < proximityStations:
                    # if list with nearby stations does not exist yet (length of list not 14): append new list to ride list
                    if len(ride_list[ride_index])!=14:
                        ride_list[ride_index].append([])
                    # get distance in km rounded to three decimal places
                    station_distance_km = round(station_distance/1000,3)
                    # write station id, name and distance to nearby stations list
                    ride_list[ride_index][14].append({'station_id':station[0], 'station_name':station[1], 'station_distance':station_distance_km})

    return ride_list

def generateJSONofRides(ride_list):
    # create dictionary to assign list values to keys
    ride_list_dict = {}
    for ride in ride_list:
        ride_list_dict[ride[0]] = {
            "bike_id": ride[1],
            "start_lat": ride[2],
            "start_lon": ride[3],
            "end_lat": ride[4],
            "end_lon": ride[5],
            "start_station": ride[6] if ride[6] else '',
            "end_station": ride[7] if ride[7] else '',
            "ride_distance": float(ride[8]) if ride[8] else '',
            "ride_start": str(ride[9]),
            "ride_end": str(ride[10]),
            "duration_min": ride[11] if ride[11] else '',
            "suspicious": (False if ride[12]==0 else True) if ride[12] else '',
            "provider": ride[13],
            "stations_nearby": '' if not len(ride)==15 else ride[14]
        }

    # create JSON string from dictionary
    jsonString = json.dumps(ride_list_dict)

    # create new file with current datetime in filename
    now_string = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    with open('queries_json/query_'+now_string+'.json', 'w') as f:
        # write JSON string to file
        f.write(jsonString)
    print('Query successfully saved to /queries_json/query_'+now_string+'.json!')

if __name__ == "__main__":
    main()