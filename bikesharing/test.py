from mysql.connector import connect, Error
from abc import ABC, abstractmethod
import datetime, os

test = input()
test2 = input()
path = test + "/" + test2
os.makedirs(path, exist_ok=True, mode=0o777)

# tag = datetime.date(2021,7,2)
# tag.replace(day = 35)
        
# # create MySQL connection and cursor
# connection = connect(option_files='mysql.conf')
# connection.autocommit = True
# cursor = connection.cursor(named_tuple=True)
# cursor.execute("DELETE FROM ride_analysis")
# cursor.execute("DELETE FROM bike_ride")
# cursor.execute("DELETE FROM exception")
# cursor.execute("DELETE FROM bike_last_status")
# cursor.execute("ALTER TABLE bike_ride AUTO_INCREMENT = 1")
# cursor.execute("ALTER TABLE exception AUTO_INCREMENT = 1")

# class DataManager(ABC):
#     @classmethod
#     def test(self):
#         lastStatusDate = cursor.fetchone().date
#         print(lastStatusDate)
#         print(lastStatusDate.date)
#         print(type(lastStatusDate))

# DataManager.test()

# lastStatusDate = datetime.date(2021,6,3)
# print(lastStatusDate)
# print(type(lastStatusDate))

# startDate = lastStatusDate + datetime.timedelta(days=1)
# parse_year = startDate.year
# month = startDate.month
# day = startDate.day

# print(parse_year)
# print(month)
# print(day)