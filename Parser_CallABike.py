# %%

from mysql.connector import connect, Error
from datetime import datetime
import requests
import os, os.path, datetime, json
from json.decoder import JSONDecodeError
import pprint
import math

#TODO: start at last parsing end point
# %%
def main():

    # create MySQL connection and cursor
    connection = connect(option_files='mysql.conf')
    connection.autocommit = True
    cursor = connection.cursor()

    # set dates for start and end of parsing
    print('Please enter month of parsing: ')
    parse_month = int(input())
    print('Please enter start day of parsing: ')
    start_day = int(input())
    print('Please enter end day of parsing: ')
    end_day = int(input())

    # get last status of all bikes from database
    callabike_bike_status_dict = getLastStatus(connection, cursor)
    # parse all files in given time range
    callabike_bike_status_dict, callabike_new_bikes_list, callabike_bike_rides_list, callabike_exception_dict = parseFiles(
        callabike_bike_status_dict, parse_month, start_day, end_day)

    # after parsing all files: insert new stations, bikes, status, rides and exceptions into database
    insertIntoDatabase(connection, cursor, callabike_new_bikes_list, callabike_bike_status_dict, callabike_bike_rides_list, callabike_exception_dict)

# %%
def getLastStatus(connection, cursor):
    # create dictionary with status of all bikes
    callabike_bike_status_dict = {}
    # get last status (coordinates, station and since time) of all bikes available at the last minute
    cursor.execute("SELECT bike_id, ST_Y(coordinates), ST_X(coordinates), station_id, since FROM bike_last_status NATURAL JOIN bike WHERE provider='callabike'")
    # go through all retrieved status
    for bike_last_status in cursor.fetchall():
        # concatenate coordinates to call-a-bike json format (will be compared with json later)
        coordinates = '[' + str(bike_last_status[1]) + ', ' + str(bike_last_status[2]) + ']'
        station_id = bike_last_status[3]
        since = bike_last_status[4]
        # write status (coordinates, station and since time) to status dictionary
        callabike_bike_status_dict[bike_last_status[0]] = [coordinates, station_id, since]
    return callabike_bike_status_dict

# %%

def parseFiles(callabike_bike_status_dict, parse_month, start_day, end_day):
    
    # list for bikes that do not exist yet (not a set as list is required for SQL insertion)
    callabike_new_bikes_list = []
    callabike_bike_rides_list = []
    callabike_exception_dict = dict()

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
                    # CALL-A-BIKE
                    # go through numbers 0-18
                    # for callabike_json_number in range(0,1):
                    try:
                        # get cut-off positions and numbers of bikes from first file
                        # cut-off positions are updated every minute this way (in case it suddenly changes)
                        json_path = DIR+'/callabike-0.json'
                        if os.path.exists(json_path):
                            left_cut_bike, right_cut_bike, left_cut_station, right_cut_station, bike_number = parseFirstFile(json_path)
                            
                            # calculate expected number of files (round up number of bikes divided by 100)
                            # e.g. 16 files expected if there are 1550 bikes
                            number_of_files = math.ceil(bike_number/100)
                            for callabike_json_number in range(0, number_of_files):
                                # if call-a-bike JSON with given number exists
                                json_path = DIR+'/callabike-'+str(callabike_json_number)+'.json'
                                if os.path.exists(json_path):
                                    # parse bike rides and new status from given file
                                    callabike_bike_status_dict, callabike_bike_rides_list, callabike_new_bikes_list = parseSingleFile(json_path, 
                                        right_cut_bike, left_cut_bike, right_cut_station, left_cut_station, callabike_bike_status_dict, 
                                            callabike_new_bikes_list, callabike_bike_rides_list, datetime_last_minute, datetime_current)
                                
                                # if the last file should exist given the number of bikes
                                # e.g. callabike-15 is missing even though number of bikes is 1550
                                # -> check if missing json number is 15 (1550 rounded down)
                                elif callabike_json_number == math.floor(bike_number/100):
                                    handleLastFileMissingException(callabike_bike_status_dict, callabike_exception_dict, datetime_current, datetime_last_minute)
                                    break
                                # if more than one file is missing: set timestamp to current time for all previously available bikes
                                elif callabike_json_number < math.floor(bike_number/100):
                                    handleAPIException("SeveralFilesMissing", datetime_current, datetime_last_minute, callabike_exception_dict, callabike_bike_status_dict)
                                    break
                        # if callabike-0 does not exist in directory -> API exception!
                        else:
                            handleAPIException("AllFilesMissing", datetime_current, datetime_last_minute, callabike_exception_dict, callabike_bike_status_dict)
                            # break the loop of all files of the minute

                    # catch JSON errors and print them with file path
                    except JSONDecodeError as e:
                        handleAPIException("JSONDecodeError", datetime_current, datetime_last_minute, callabike_exception_dict, callabike_bike_status_dict)
                    except ValueError as e:
                        handleAPIException("ValueError", datetime_current, datetime_last_minute, callabike_exception_dict, callabike_bike_status_dict)
                    except KeyError as e:
                        handleAPIException("KeyError", datetime_current, datetime_last_minute, callabike_exception_dict, callabike_bike_status_dict)
                    except TypeError as e:
                        handleAPIException("TypeError", datetime_current, datetime_last_minute, callabike_exception_dict, callabike_bike_status_dict)
                    except IndexError as e:
                        handleAPIException("IndexError", datetime_current, datetime_last_minute, callabike_exception_dict, callabike_bike_status_dict)
                    except Error as e:
                        handleAPIException("Error", datetime_current, datetime_last_minute, callabike_exception_dict, callabike_bike_status_dict)
    return callabike_bike_status_dict, callabike_new_bikes_list, callabike_bike_rides_list, callabike_exception_dict

def parseFirstFile(json_path):
    # open first JSON and write subarray 'items' to array bikes
    with open(json_path) as bike_json:
        jsonObject = json.load(bike_json)
        bike_json.close()
    bikes = jsonObject['items']

    # take rentalObject href of first bike
    href_sample_bike = str(bikes[0]['rentalObject'])
    # find out beginning and end position of bike ID within that href
    left_cut_bike = href_sample_bike.rfind("rentalobjects/")+14
    right_cut_bike = href_sample_bike.rfind("'}'")-1

    # take area href of first bike
    href_sample_station = str(bikes[0]['area'])
    # find out beginning and end position of station ID within that href
    left_cut_station = href_sample_station.rfind("areas/")+6
    right_cut_station = href_sample_station.rfind("'}'")-1

    # get number of bikes available for this minute
    bike_number = jsonObject['size']
    
    return left_cut_bike, right_cut_bike, left_cut_station, right_cut_station, bike_number

def parseSingleFile(json_path, right_cut_bike, left_cut_bike, right_cut_station, left_cut_station, 
    callabike_bike_status_dict, callabike_new_bikes_list, callabike_bike_rides_list, datetime_last_minute, datetime_current):
    # open JSON and write subarray 'items' to array bikes
    with open(json_path) as bike_json:
        jsonObject = json.load(bike_json)
        bike_json.close()
    bikes = jsonObject['items']

    for bike in bikes:
        bike_href = str(bike['rentalObject'])
        # cut bike ID out of rentalobject href
        bike_id = bike_href[:right_cut_bike][left_cut_bike:]

        station_href = str(bike['area'])
        # if area is 65D863257FDF847B1F2807E7E346B83F8C752E3F (Stadtgebiet Berlin), indicating a free-floating bike
        # comparing whole href does NOT work, as the URL changes over time!
        # if station_href == "{'href': 'https://api.deutschebahn.com:80/apiv4/v1/areas/65D863257FDF847B1F2807E7E346B83F8C752E3F'}":
        if '65D863257FDF847B1F2807E7E346B83F8C752E3F' in station_href:
            station_id = None
        else:
            # if bike is at an actual station: cut station ID out of area href
            station_id = station_href[:right_cut_station][left_cut_station:]
        coordinates = str(bike['position']['coordinates'])

        # if bike doesn't exist in database yet
        if bike_id not in callabike_bike_status_dict.keys():
            # add to set of new bikes
            callabike_new_bikes_list.append([bike_id])
        # else (= exists) if "since" timestamp of current status is NOT equal to the previous minute 
        # => bike was not previously available and therefore gone
        # AND not equal to current minute -> work-around for Call-A-Bike exception with multiple entries of same bike within same minute
        elif(callabike_bike_status_dict[bike_id][2] != datetime_last_minute and callabike_bike_status_dict[bike_id][2] != datetime_current):
            # parse bike ride details and append to list
            # current coordinates / station / datetime will be end coordinates / station / datetime of ride
            callabike_bike_rides_list = bikeRideToList(callabike_bike_rides_list, callabike_bike_status_dict, bike_id, coordinates, station_id, datetime_current)
        # update new available status in dictionary (or create new status if not yet existent)
        callabike_bike_status_dict[bike_id] = [coordinates, station_id, datetime_current]
    return callabike_bike_status_dict, callabike_bike_rides_list, callabike_new_bikes_list

def bikeRideToList(callabike_bike_rides_list, callabike_bike_status_dict, bike_id, end_coordinates, end_station, datetime_current):
    # get last saved coordinates and station from status (will be start coordinates and station)
    start_coordinates = callabike_bike_status_dict[bike_id][0]
    # start_station =  None if not callabike_bike_status_dict[bike_id][1] else callabike_bike_status_dict[bike_id][1]
    start_station =  callabike_bike_status_dict[bike_id][1]
    # bike gone since: one minute after the last time it was available
    since = callabike_bike_status_dict[bike_id][2] + datetime.timedelta(minutes=1)

    # end_station = None if not end_station else end_station

    # get ride details (start and end coordinates, start and end station)
    start_coordinates_comma_position = start_coordinates.index(", ")
    start_coordinates_Y = start_coordinates[1:start_coordinates_comma_position]
    start_coordinates_X = start_coordinates[start_coordinates_comma_position+2:-1]
    start_coordinates = 'POINT(' + start_coordinates_X + ' ' + start_coordinates_Y + ')'
    
    end_coordinates_comma_position = end_coordinates.index(", ")
    end_coordinates_Y = end_coordinates[1:end_coordinates_comma_position]
    end_coordinates_X = end_coordinates[end_coordinates_comma_position+2:-1]
    end_coordinates = 'POINT(' + end_coordinates_X + ' ' + end_coordinates_Y + ')'

    # handle "bike taken at the moment of file retrieval" exception:
    # skip rides with identical start / end coordinates AND a duration of one minute
    if not (start_coordinates==end_coordinates and since+datetime.timedelta(minutes=1)==datetime_current):
        # write ride details (+ timestamps) to list
        # bike ride tuple: bike ID, start coordinates, start station, end coordinates, end station, since, until
        callabike_bike_rides_list.append((bike_id, start_coordinates, end_coordinates, start_station, end_station, since, datetime_current))

    return callabike_bike_rides_list

def handleAPIException(error_type, datetime_current, datetime_last_minute, callabike_exception_dict, callabike_bike_status_dict):
    # convert current datetime to MySQL format
    timestamp_mysql = datetime_current.strftime('%Y-%m-%d %H:%M:%S')
    # make sure that exception with same timestamp does not exist already
    if timestamp_mysql not in callabike_exception_dict:
        # set status of all bikes that were previously (at the last minute) available to current timestamp
        # that way, the missing files do not cause a gap between status timestamps, which would result in a ride
        for bike in callabike_bike_status_dict.values():
            # if bike was available at the previous minute
            if bike[2] == datetime_last_minute:
                # manually set since-timestamp of bike to current time
                bike[2] = datetime_current
        # write error type with timestamp to exception dictionary
        callabike_exception_dict[timestamp_mysql] = error_type

def handleLastFileMissingException(callabike_bike_status_dict, callabike_exception_dict, datetime_current, datetime_last_minute):
    # convert current datetime to MySQL format
    timestamp_mysql = datetime_current.strftime('%Y-%m-%d %H:%M:%S')
    # all stations found in callabike-15.json
    callabike15_stations_list = ['E4816436351084C8665A14A791C92B9029FFE5A9', 'E53918D18D77DEC63E7617E6D576407B59A1CC64', 'E5F13D0D680098E71BCD58C24D50D2798DA105A2', 'E63983D1B9B421396BB47E084C48FF98D9F6541E', 'E7207FECDF4A6C2970B605043BD13509BC6E8BF2', 'E896DAE5C5C8641085D78F3454039BF11F88B490', 'E95B9D2D4BC6DFF1B4E4041EFA48B2E854F39F50', 'E985E95024AE08B95C5D02A99C243F3C12B21958', 'E9A4731FB1469854AF2EB04B46A20C06C7BDEFE0', 'E9C917B09D272FD073B8BAB13AFE3949D3C52D02', 'EA5B62A69DE3FFB1D78033143E67EC545149081C', 'EA7E078A544EB0EF3CE0912FEA6350BD72B9248C', 'EAB7557DE2C637133DE7FD9F59DF95B9B032FDFC', 'EB877F6C8D0310034D417C8E19856045DB312256', 'EBC6DA5A9FCFD298FD89B53DF8D3D479C77CA8A1', 'ED6733F7153177BEC3C53FC0B407F166ABAA4771', 'ED81B3D57430CD69F63A870DF80DAE082F00F2B6', 'EDA2F36F10E2351F6139CC1F7A1FAD7CE9BD7B88', 'EE2ACD8B04EDEB34DCA8F8729016AD4BAFEEF8B7', 'EEDD68FC212BB4852D306CB1507FD8AF9CA06870', 'EF978FF5850CA7680283244BBE5512E36BBBF88C', 'F1C71A67DBD7BF6E059564D42054CF0221CA1710', 'F5CB0C0F66E9AA727FD5741E82E1BECD21782E9E', 'F8C609BDD350C0D808599DE060F0C642D041B120', 'FA6605EDA10E7F1EFBA3FEA1F0F7CC226CC705A5', 'FADA02AE379131540D07A408B1ECA90C3C16C4D2', 'FADED0BBC56DA27A818D38D38947E4370BF7985C', 'FB926E835660040267689B27E176A0C5AC80AEF2', 'FF75924F15B8AADEF786B73F8DC05C5AD6223F7D', 'b180c44f-09f5-4d58-b942-6813f4b39297', 'd84aedfa-3037-43d8-b79b-bb1cc8d4bd9c', 'FDB7669045616ADB335F09EE45BCAAA5BD071A33', 'DF55D0F1B7D70B2007B4B9AB7CE751EC07555453', 'E0E1DF16874C60B2C232F94EBC8C0674D663FFE3', 'DF03F0C3EDB6CA037E09BDECB50430601FAE9275', 'DF3B9426ADBB698CB6BC513DEDF961B24FF96133', 'DF44F9023C5E9494F56F0DA04D4C0695975EA4D1', 'DEF9D70BC70DD6FDB97D15C24D172C976AED6E17', 'de1c2b90-46f9-49f3-8a16-5d39cdc04c3d', 'e61f8443-4c1f-430b-bf80-7b4350ac1957', 'f6a9894e-83a8-4404-8e1a-f4db66a82681', 'FE65FCFE5B2D6C431B7079FFE98A006F94E7E72E', 'E71F725330C12D573ECF9102452A33422581EE99', 'DF86F79CD685C1D22C87A4C7403F2D693EF53EBE', 'EB56CE583D16CF380246658016A17A1075CCCD01']
    # set timestamp to current time for previously available bikes at "callabike-15.json" stations
    for bike in callabike_bike_status_dict.values():
        # if bike was at one of the callabike-15 stations at the previous minute
        if bike[1] in callabike15_stations_list and bike[2] == datetime_last_minute:
            # as the last status is potentially missing
            # manually set since-timestamp of bike to current time
            bike[2] = datetime_current
    # write error type (last file missing) with timestamp to exception dictionary
    # callabike_exception_dict[timestamp_mysql] = "LastFileMissing"



# %%
def insertIntoDatabase(connection, cursor, callabike_new_bikes_list, callabike_bike_status_dict, callabike_bike_rides_list, callabike_exception_dict):
    
    # set up pretty printer with indentation
    pp = pprint.PrettyPrinter(indent=4)

    # get station files and return number of necessary requests
    stations_requests_no = getNewStations()
    # insert new stations from new files
    callabike_new_stations_list = parseNewStations(stations_requests_no, cursor)
    # run SQL query to insert new stations into station table
    # no station_capacity (as not provided by Call-A-Bike)
    cursor.executemany("INSERT INTO station (station_id, coordinates, name, provider) VALUES (%s,  ST_GeomFromText(%s, 4326), %s, %s) ON DUPLICATE KEY UPDATE station_id=station_id", callabike_new_stations_list)
    print('Inserting new stations done!')

    print('New bikes:')
    pp.pprint(callabike_new_bikes_list)
    cursor.executemany("INSERT INTO bike (bike_id, provider) VALUES (%s, 'callabike') ON DUPLICATE KEY UPDATE bike_id=bike_id", callabike_new_bikes_list)
    print('Inserting bikes done!')

    # insert bike ride list into bike_ride table
    # ignore (ON DUPLICATE KEY UPDATE bike_id=bike_id) if combination of bike_id and since exists already!
    # ensures idempotence as rides cannot be duplicated
    cursor.executemany("INSERT INTO bike_ride (bike_id, start_coordinates, end_coordinates, start_station_id, end_station_id, since, until) VALUES (%s, ST_GeomFromText(%s, 4326), ST_GeomFromText(%s, 4326), %s, %s, %s, %s) ON DUPLICATE KEY UPDATE bike_id=bike_id", callabike_bike_rides_list)
    print('Inserting bike rides done!')

    last_bike_status_list = lastBikeStatusToList(callabike_bike_status_dict)
    # overwrite (REPLACE) bike_last_status table with new status list
    cursor.executemany("REPLACE INTO bike_last_status (bike_id, coordinates, station_id, since) VALUES (%s, ST_GeomFromText(%s, 4326), %s, %s)", last_bike_status_list)
    print('Inserting last bike status done!')

    # convert exception dictionary to list and insert into exception table
    callabike_exception_list = [(value, key) for key, value in callabike_exception_dict.items()]
    cursor.executemany("INSERT INTO exception (provider, error_type, time) VALUES ('callabike', %s, %s) ON DUPLICATE KEY UPDATE exception_id=exception_id", callabike_exception_list)
    print('Exceptions:')
    pp.pprint(callabike_exception_list)
    print('Inserting exceptions done!')

    # commit queries and close the connection
    connection.commit()
    connection.close()

# %%
def getNewStations():
    # get current Call-A-Bike station JSON with 10km radius around mid-Berlin
    lat = "&lat=52.518611"
    lon = "&lon=13.408333"
    radius = "&radius=10000"
    limit = "&limit=100"
    url = "https://api.deutschebahn.com/flinkster-api-ng/v1/areas?providernetwork=2" + lat + lon + radius + limit
    headers = {
        'Accept': 'application/json',
        'Authorization': 'Bearer 56b6c4f18d92c4869078102e978ec8b9',
    }
    resp = requests.get(url, headers=headers)

    # get number of available stations, divide by 100 and round up to get number of necessary requests
    requests_no = math.ceil(resp.json()['size'] / 100)
    # directory path for station files
    DIR = 'json/Stations'
    # save first 100 stations
    with open(DIR+'/callabike_station_0.json', 'wb') as f:
        f.write(resp.content)
    # start counting at 1 (first one already saved) until number of necessary requests reached
    for i in range(1, requests_no):
        # scroll through stations in steps of 100 by incrementally increasing offset (starting with 100)
        offset = "&offset=" + str(i*100)
        # request json with given offset
        url = "https://api.deutschebahn.com/flinkster-api-ng/v1/areas?providernetwork=2" + lat + lon + radius + offset + limit
        resp = requests.get(url, headers=headers)
        # save JSON with numbered filename in directory
        with open(DIR+'/callabike_station_'+str(i)+'.json', 'wb') as f:
            f.write(resp.content)
    # return number of necessary requests
    return requests_no

def parseNewStations(requests_no, cursor):
    # get IDs of all existing stations and save in new set
    cursor.execute("SELECT station_id FROM station WHERE provider='callabike'")
    callabike_stations_set = {station[0] for station in cursor.fetchall()}
    # list for stations that do not exist yet
    callabike_new_stations_list = list()

    # directory path for station files
    DIR = 'json/Stations'
    # go through all gathered files
    for j in range(0, requests_no):
        try:
            # get stations of file
            with open(DIR+'/callabike_station_'+str(j)+'.json') as station_json:
                jsonObject = json.load(station_json)
                station_json.close()
            stations = jsonObject['items']
            # for every station 
            for station in stations:
                station_id =  station['uid']
                # if station doesn't exist in database yet
                if station_id not in callabike_stations_set:
                    # retrieve station data and add to call-a-bike station list
                    station_latitude = str(station['geometry']['centroid']['coordinates'][1])
                    station_longitude = str(station['geometry']['centroid']['coordinates'][0])
                    # Format for MySQL ST_GeomFromText and SRID 4326: Point(52.53153 13.38651) -> in reverse order!
                    station_coordinates = 'Point(' + station_latitude + ' ' + station_longitude + ')'
                    station_name = station['name']
                    station_provider =  'callabike'
                    # no station_capacity (as not provided by Call-A-Bike)
                    callabike_new_stations_list.append((station_id,station_coordinates,station_name,station_provider))
        except Error as e:
            print('Error occured!')
    return(callabike_new_stations_list)


# %%
def lastBikeStatusToList(callabike_bike_status_dict):
    last_bike_status_list = []
    # go through all bike status with key (bike_id) and value (bike: list with status of that bike)
    for bike_id, bike in callabike_bike_status_dict.items():
        # determine position of comma separating the coordinates in JSON array
        coordinates_space_position = bike[0].index(", ")
        # get Y coordinates -> between [ and ,
        # e.g. get 13.13 from [13.13,52.3]
        coordinates_Y = bike[0][1:coordinates_space_position]
        # get X coordinates -> between space after , and ]
        coordinates_X = bike[0][coordinates_space_position+2:-1]
        # get coordinates into MySQL POINT format
        coordinates = 'POINT(' + coordinates_X + ' ' + coordinates_Y + ')'
        # station = None if not bike[1] else bike[1]
        # write status (position, station and since) as tuple to status list
        last_bike_status_list.append((bike_id, coordinates, bike[1], str(bike[2])))
    return last_bike_status_list

if __name__ == "__main__":
    main()