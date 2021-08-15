
from mysql.connector import connect, Error
connection = connect(option_files='mysql.conf')
connection.autocommit = True
cursor = connection.cursor(named_tuple=True)
cursor.execute("SELECT station_id, name, ST_X(coordinates) AS lon, ST_Y(coordinates) AS lat, provider FROM station")
for station in cursor:
    print(type(station.lon))
station_list = cursor.fetchall()

print("Hallo" + ("error_type" if True else "") + " FROM b")