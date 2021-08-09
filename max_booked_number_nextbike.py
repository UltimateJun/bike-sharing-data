from getpass import getpass
from datetime import datetime
import os, os.path, datetime, json
from json.decoder import JSONDecodeError
import pprint


def parseFiles():
    
    nextbike_booked = 0

    parse_month = 7
    start_day = 1
    end_day = 25

    for day in range(start_day, end_day+1):
        dateOfParse = '2021-' + f'{parse_month:02d}' + '-' + f'{day:02d}'
        print(dateOfParse + ' started at ' + datetime.datetime.now().strftime("%H:%M:%S"))
        # for all minutes of that date
        for hour in range(0,24):
            for minute in range(0,60):
                # format date and time strings (YYYY-MM-DD and HH-MM), append to directory path
                time_hhmm = f'{hour:02d}' + '-' + f'{minute:02d}'
                DIR = 'json/' + dateOfParse + '/' + time_hhmm;
                # if given directory exists
                if os.path.exists(DIR):
                    # NEXTBIKE
                    try:
                        json_path = DIR+'/nextbike.json'
                        if os.path.exists(json_path):
                            # parse bike rides and new status from given file
                             nextbike_booked, newMax = parseFile(json_path, nextbike_booked)
                             if newMax:
                                 max_datetime = datetime.datetime.strptime(dateOfParse + '-' +  time_hhmm, '%Y-%m-%d-%H-%M')
                        # if nextbike.json does not exist in directory -> API exception!
                        else:
                            print("error")
                            # break the loop of all files of the minute

                    # catch JSON errors and print them with file path
                    except JSONDecodeError as e:
                        print("error")
                    except ValueError as e:
                        print("error")
                    except KeyError as e:
                        print("error")
                    except TypeError as e:
                        print("error")
                    except IndexError as e:
                        print("error")
    print(max_datetime)
    print(nextbike_booked)

def parseFile(json_path, nextbike_booked):
    # open JSON and write subarray 'places' to array bikes
    newMax = False
    with open(json_path) as bike_json:
        jsonObject = json.load(bike_json)
        bike_json.close()
    booked = jsonObject['countries'][0]['booked_bikes']
    if booked > nextbike_booked:
        nextbike_booked = max(nextbike_booked, booked)
        newMax = True

    return nextbike_booked, newMax

if __name__ == "__main__":
    parseFiles()
