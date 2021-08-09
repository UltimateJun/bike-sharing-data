# %%
# GET BIKES FROM DATABASE

from mysql.connector import connect, Error
from datetime import date
from datetime import datetime
import requests
import os, os.path, datetime, json
from json.decoder import JSONDecodeError
import pprint
import math

# set up pretty printer with indentation
pp = pprint.PrettyPrinter(indent=4)

# create MySQL connection
connection = connect(option_files='mysql.conf')
connection.autocommit = True
cursor = connection.cursor()
# get all bike IDs
cursor.execute("SELECT bike_id FROM bike WHERE provider = 'callabike'")

# set datetimes for start and end of parsing
start_datetime = datetime.datetime.strptime('2021-06-04-00-00', '%Y-%m-%d-%H-%M')
end_datetime = datetime.datetime.strptime('2021-06-18-23-59', '%Y-%m-%d-%H-%M')
# set datetime for the last minute before start
previous_minute_datetime = start_datetime - datetime.timedelta(minutes=1)

# create dictionary with status of all bikes, initialize all with empty list
callabike_bike_status_dict = {}

# only for very first setup (if bike_last_status table not filled yet)
callabike_bike_status_dict = {bike[0]:list() for bike in cursor.fetchall()} 

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

# only for very first setup (if bike_last_status table not filled yet)
# go through all bike status
for bike in callabike_bike_status_dict.values():
    if not bike: # if list within bike is empty (missing in database)
        # set status to "no_ride" (will prevent ride from being created later)
        bike[:] = ['no_ride', '', previous_minute_datetime]

# list for bikes that do not exist yet (not a set as list is required for SQL insertion)
callabike_new_bikes_list = []


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


def insertNewStations(requests_no):

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
                if(station_id not in callabike_stations_set):
                    # retrieve station data and add to call-a-bike station list
                    station_latitude = str(station['geometry']['centroid']['coordinates'][1])
                    station_longitude = str(station['geometry']['centroid']['coordinates'][0])
                    # Format for MySQL ST_GeomFromText and SRID 4326: Point(52.53153 13.38651) -> in reverse order!
                    station_coordinates = 'Point(' + station_latitude + ' ' + station_longitude + ')'
                    station_name = station['name']
                    station_provider =  'callabike'
                    # station_capacity = ''
                    callabike_new_stations_list.append(list([station_id,station_coordinates,station_name,station_provider]))
        except Error as e:
            print('Error occured!')

    # print new stations
    print('new stations: ')
    print(callabike_new_stations_list)

    # run SQL query to insert new stations into station table
    cursor.executemany("INSERT INTO station (station_id, coordinates, name, provider) VALUES (%s,  ST_GeomFromText(%s, 4326), %s, %s) ON DUPLICATE KEY UPDATE station_id=station_id", callabike_new_stations_list)
    connection.commit()


# %%
def insertLastBikeStatus(callabike_bike_status_dict):
    last_bike_status_list = []
    # go through all bike status with key (bike_id) and value (bike: list with status of that bike)
    for bike_id, bike in callabike_bike_status_dict.items():
        # only bike status that are not 'no_ride'
        if(bike[0] != 'no_ride'):
            # determine position of comma separating the coordinates in JSON array
            coordinates_space_position = bike[0].index(", ")
            # get Y coordinates -> between [ and ,
            # e.g. get 13.13 from [13.13,52.3]
            coordinates_Y = bike[0][1:coordinates_space_position]
            # get X coordinates -> between space after , and ]
            coordinates_X = bike[0][coordinates_space_position+2:-1]
            # get coordinates into MySQL POINT format
            coordinates = 'POINT(' + coordinates_X + ' ' + coordinates_Y + ')'
            station = None if not bike[1] else bike[1]
            # write status (position, station and since) as tuple to status list
            last_bike_status_list.append((bike_id, coordinates, station, str(bike[2])))

    # clear bike_last_status table and insert new status list
    cursor.execute("TRUNCATE TABLE bike_last_status")
    cursor.executemany("INSERT IGNORE INTO bike_last_status (bike_id, coordinates, station_id, since) VALUES (%s, ST_GeomFromText(%s, 4326), %s, %s)", last_bike_status_list)
    connection.commit()

def insertBikeRides(callabike_ride_list):

    ride_list = []
    for ride in callabike_ride_list:
        # get ride details (ID, start and end coordinates, start and end station)
        bike_id = ride[0]
        start_coordinates_space_position = ride[1].index(", ")
        start_coordinates_Y = ride[1][1:start_coordinates_space_position]
        start_coordinates_X = ride[1][start_coordinates_space_position+2:-1]
        start_coordinates = 'POINT(' + start_coordinates_X + ' ' + start_coordinates_Y + ')'
        
        end_coordinates_space_position = ride[2].index(", ")
        end_coordinates_Y = ride[2][1:end_coordinates_space_position]
        end_coordinates_X = ride[2][end_coordinates_space_position+2:-1]
        end_coordinates = 'POINT(' + end_coordinates_X + ' ' + end_coordinates_Y + ')'

        start_station = None if not ride[3] else ride[3]
        end_station = None if not ride[4] else ride[4]

        # write ride details (+ timestamps) to list
        ride_list.append((bike_id, start_coordinates, end_coordinates, start_station, end_station, str(ride[5]), str(ride[6])))

    # insert list into bike_ride table
    cursor.executemany("INSERT INTO bike_ride (bike_id, start_coordinates, end_coordinates, start_station_id, end_station_id, since, until) VALUES (%s, ST_GeomFromText(%s, 4326), ST_GeomFromText(%s, 4326), %s, %s, %s, %s)", ride_list)
    connection.commit()

def insertExceptions():
    # convert dictionary to list and insert into exception table
    callabike_exception_list = [(value, key) for key, value in callabike_exception_dict.items()]
    pp.pprint(callabike_exception_list)
    cursor.executemany("INSERT INTO exception (provider, error_type, time) VALUES ('callabike', %s, %s)", callabike_exception_list)
    connection.commit()

def handleAPIException(error_type):
    timestamp_mysql = date + " " + f'{hour:02d}' + ':' + f'{minute:02d}'
    # make sure that exception with same timestamp does not exist already
    if(timestamp_mysql not in callabike_exception_dict):
        # set status of all bikes that were previously (at the last minute) available to current timestamp
        # that way, the missing files do not cause a gap between status timestamps, which would result in a ride
        for bike in callabike_bike_status_dict.values():
            if(bike[2] == datetime_last_minute):
                bike[2] = datetime_current
        # write error type with timestamp to exception dictionary
        callabike_exception_dict[timestamp_mysql] = error_type

def handleNotAllFilesException(missingFiles):
    # all stations found in callabike-15.json
    callabike15_stations_list = ['E4816436351084C8665A14A791C92B9029FFE5A9', 'E53918D18D77DEC63E7617E6D576407B59A1CC64', 'E5F13D0D680098E71BCD58C24D50D2798DA105A2', 'E63983D1B9B421396BB47E084C48FF98D9F6541E', 'E7207FECDF4A6C2970B605043BD13509BC6E8BF2', 'E896DAE5C5C8641085D78F3454039BF11F88B490', 'E95B9D2D4BC6DFF1B4E4041EFA48B2E854F39F50', 'E985E95024AE08B95C5D02A99C243F3C12B21958', 'E9A4731FB1469854AF2EB04B46A20C06C7BDEFE0', 'E9C917B09D272FD073B8BAB13AFE3949D3C52D02', 'EA5B62A69DE3FFB1D78033143E67EC545149081C', 'EA7E078A544EB0EF3CE0912FEA6350BD72B9248C', 'EAB7557DE2C637133DE7FD9F59DF95B9B032FDFC', 'EB877F6C8D0310034D417C8E19856045DB312256', 'EBC6DA5A9FCFD298FD89B53DF8D3D479C77CA8A1', 'ED6733F7153177BEC3C53FC0B407F166ABAA4771', 'ED81B3D57430CD69F63A870DF80DAE082F00F2B6', 'EDA2F36F10E2351F6139CC1F7A1FAD7CE9BD7B88', 'EE2ACD8B04EDEB34DCA8F8729016AD4BAFEEF8B7', 'EEDD68FC212BB4852D306CB1507FD8AF9CA06870', 'EF978FF5850CA7680283244BBE5512E36BBBF88C', 'F1C71A67DBD7BF6E059564D42054CF0221CA1710', 'F5CB0C0F66E9AA727FD5741E82E1BECD21782E9E', 'F8C609BDD350C0D808599DE060F0C642D041B120', 'FA6605EDA10E7F1EFBA3FEA1F0F7CC226CC705A5', 'FADA02AE379131540D07A408B1ECA90C3C16C4D2', 'FADED0BBC56DA27A818D38D38947E4370BF7985C', 'FB926E835660040267689B27E176A0C5AC80AEF2', 'FF75924F15B8AADEF786B73F8DC05C5AD6223F7D', 'b180c44f-09f5-4d58-b942-6813f4b39297', 'd84aedfa-3037-43d8-b79b-bb1cc8d4bd9c', 'FDB7669045616ADB335F09EE45BCAAA5BD071A33', 'DF55D0F1B7D70B2007B4B9AB7CE751EC07555453', 'E0E1DF16874C60B2C232F94EBC8C0674D663FFE3', 'DF03F0C3EDB6CA037E09BDECB50430601FAE9275', 'DF3B9426ADBB698CB6BC513DEDF961B24FF96133', 'DF44F9023C5E9494F56F0DA04D4C0695975EA4D1', 'DEF9D70BC70DD6FDB97D15C24D172C976AED6E17', 'de1c2b90-46f9-49f3-8a16-5d39cdc04c3d', 'e61f8443-4c1f-430b-bf80-7b4350ac1957', 'f6a9894e-83a8-4404-8e1a-f4db66a82681', 'FE65FCFE5B2D6C431B7079FFE98A006F94E7E72E', 'E71F725330C12D573ECF9102452A33422581EE99', 'DF86F79CD685C1D22C87A4C7403F2D693EF53EBE', 'EB56CE583D16CF380246658016A17A1075CCCD01']
    # if only last file is missing: set timestamp to current time for previously available bikes at "callabike-15.json" stations
    if(missingFiles == "lastFile"):
        for bike in callabike_bike_status_dict.values():
            # if bike was at one of the callabike-15 stations at the previous minute
            if(bike[1] in callabike15_stations_list and bike[2] == datetime_last_minute):
                # as the last status is potentially missing
                # manually set since-timestamp of bike to current time
                bike[2] = datetime_current
    # if more than one file is missing: set timestamp to current time for all previously available bikes
    elif(missingFiles == "moreFiles"):
        # loop through all bikes
        for bike in callabike_bike_status_dict.values():
            # if bike was available at the previous minute
            if(bike[2] == datetime_last_minute):
                # manually set since-timestamp of bike to current time
                bike[2] = datetime_current

# %%
# get execution time:
# import time
# start_time = time.time()

start_day = start_datetime.day
start_month = start_datetime.month

callabike_bike_rides_list = []
callabike_exception_dict = dict()

# for all minutes within date
# for day in range(start_day, start_day+1):
for day in range(5, 6):
    date = '2021-' + f'{start_month:02d}' + '-' + f'{day:02d}'
    for hour in range(0,24):
        for minute in range(0,60):
            # format date and time strings (YYYY-MM-DD and HH-MM), append to directory path
            time_hhmm = f'{hour:02d}' + '-' + f'{minute:02d}'
            DIR = 'json/' + date + '/' + time_hhmm;
            # timestamp of batch of files currently being processed
            datetime_current = datetime.datetime.strptime(date + '-' +  time_hhmm, '%Y-%m-%d-%H-%M')
            # timestamp of previous minute
            datetime_last_minute = datetime_current - datetime.timedelta(minutes=1)
            # if given directory exists
            if(os.path.exists(DIR)):
                # CALL-A-BIKE
                # go through numbers 0-18
                # for callabike_json_number in range(0,1):
                for callabike_json_number in range(0,19):
                    # if call-a-bike JSON with given number exists
                    json_path = DIR+'/callabike-'+str(callabike_json_number)+'.json'
                    if(os.path.exists(json_path)):
                        try:
                            # open JSON and write subarray 'items' to array bikes
                            with open(json_path) as bike_json:
                                jsonObject = json.load(bike_json)
                                bike_json.close()
                            bikes = jsonObject['items']

                            # if it is the first file (callabike_0.json)
                            # href_sample_bike is updated every minute this way (in case it changes)
                            if(callabike_json_number == 0):
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

                            for bike in bikes:
                                bike_href = str(bike['rentalObject'])
                                # cut bike ID out of rentalobject href
                                bike_id = bike_href[:right_cut_bike][left_cut_bike:]

                                station_href = str(bike['area'])
                                # if area is 65D863257FDF847B1F2807E7E346B83F8C752E3F (Stadtgebiet Berlin), indicating a free-floating bike
                                # comparing whole href does NOT work, as the URL changes over time!
                                # if(station_href == "{'href': 'https://api.deutschebahn.com:80/apiv4/v1/areas/65D863257FDF847B1F2807E7E346B83F8C752E3F'}"):
                                if('65D863257FDF847B1F2807E7E346B83F8C752E3F' in station_href):
                                    station_id = ''
                                else:
                                    # if bike is at an actual station: cut station ID out of area href
                                    station_id = station_href[:right_cut_station][left_cut_station:]
                                coordinates = str(bike['position']['coordinates'])

                                # if bike doesn't exist in database yet
                                if(bike_id not in callabike_bike_status_dict.keys()):
                                    # add to set of new bikes
                                    callabike_new_bikes_list.append(list([bike_id]))
                                # else (= exists) if "since" timestamp of current status is NOT equal to the previous minute 
                                # => bike was not previously available and therefore gone
                                # AND not equal to current minute -> work-around for Call-A-Bike exception with multiple entries of same bike within same minute
                                elif(callabike_bike_status_dict[bike_id][2] != datetime_last_minute and callabike_bike_status_dict[bike_id][2] != datetime_current):
                                    # if current status is not 'no_ride' (previously not found in status database table)
                                    if(not callabike_bike_status_dict[bike_id][0] == 'no_ride'):
                                        start_coordinates = callabike_bike_status_dict[bike_id][0]
                                        start_station = callabike_bike_status_dict[bike_id][1]
                                        # bike gone since: one minute after the last time it was available
                                        since = callabike_bike_status_dict[bike_id][2] + datetime.timedelta(minutes=1)

                                        # bike ride tuple: bike ID, start coordinates, start station, end coordinates, end station, since, until
                                        callabike_bike_rides_list.append((bike_id, start_coordinates, coordinates, start_station, station_id, since, datetime_current))
                                # update new available status in dictionary (or create new status if not yet existent)
                                callabike_bike_status_dict[bike_id] = [coordinates, station_id, datetime_current]
                    
                        # catch JSON errors and print them with file path
                        except JSONDecodeError as e:
                            handleAPIException("JSONDecodeError")
                        except ValueError as e:
                            handleAPIException("ValueError")
                        except KeyError as e:
                            handleAPIException("KeyError")
                        except TypeError as e:
                            handleAPIException("TypeError")
                        except Error as e:
                            handleAPIException("Error")
                    # if callabike-0 does not exist in directory -> API exception!
                    elif callabike_json_number == 0:
                        handleAPIException("NoFile")
                        # break the loop of all files of the minute
                        break
                    # if the last file should exist given the number of bikes
                    # e.g. callabike-15 is missing even though number of bikes > 1500
                    # -> check if missing json number is 15 (> 1500 rounded down)
                    elif callabike_json_number == math.floor(bike_number/100):
                        handleNotAllFilesException("lastFile")
                        break
                    # if more than just the last file is missing
                    elif callabike_json_number < math.floor(bike_number/100):
                        handleNotAllFilesException("moreFiles")
                        break

# get station files and return number of necessary requests
stations_requests_no = getNewStations()
# insert new stations from new files
insertNewStations(stations_requests_no)
print('Inserting new stations done!')

pp.pprint(callabike_new_bikes_list)
cursor.executemany("INSERT INTO bike (bike_id, provider) VALUES (%s, 'callabike') ON DUPLICATE KEY UPDATE bike_id=bike_id", callabike_new_bikes_list)
connection.commit()
print('Inserting bikes done!')

# at the end of the day (or of the period)
insertLastBikeStatus(callabike_bike_status_dict)
print('Inserting last bike status done!')

callabike_actual_bike_rides_list = [ride for ride in callabike_bike_rides_list] # if not ride[1]=='no_ride']
insertBikeRides(callabike_actual_bike_rides_list)
print('Inserting bike rides done!')

insertExceptions()
print('Inserting exceptions done!')

# get execution time:
# print("--- %s seconds ---" % (time.time() - start_time))

# pp.pprint(callabike_bike_rides_list)
# %%
