from sqlalchemy import *
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from mysql.connector import connect
import MySQLConf
import pprint
from dataclasses import dataclass

# create SQLAlchemy engine
engine = create_engine(URL.create(**MySQLConf.DATABASE))
Session = sessionmaker(bind=engine)
session = Session()

metadata = MetaData(bind=None)
metadata.reflect(engine, only=['bike', 'station'])
# Base = declarative_base()
Base = automap_base(metadata=metadata)
Base.prepare() #engine, reflect=True)
# bike_table = Table('bike', metadata, autoload=True, autoload_with=engine)


# @dataclass
# class Bike(Base):
#     __tablename__ = 'bike'
    
#     bike_id = Column(String(40), primary_key=True)
#     provider = Column(String(20)) 

#     def __repr__(self):
#         return self.bike_id + self.provider

    # @property
    # def id(self):
    #     return self.id
    # @id.setter
    # def id(self, value):
    #     self.id = value
    # @property
    # def provider(self):
    #     return self.provider
    # @provider.setter
    # def provider(self, value):
    #     self.provider = value

Bike = Base.classes.bike
bikes = session.query(Bike.bike_id, Bike.provider).order_by(desc(Bike.provider))#.first()
# print(bikes.keys())
print(type(bikes))
print(bikes.first().bike_id)
Station = Base.classes.station
stations = session.query(Station.station_id, Station.coordinates).first()
print(stations)

# print(bikes.first().bike_id)
# print(bikes)

# stmt = select(bike).where(bike.columns.provider == 'nextbike')
# print(stmt)
# print(type(stmt))
# conn = engine.connect()
# print(type(conn))
# result = conn.execute(stmt)