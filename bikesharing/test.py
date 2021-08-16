from abc import ABC, abstractmethod
from dataclasses import dataclass
from json.decoder import JSONDecodeError
import os, json

class Status(ABC):
    bike_id: str
    lon: str
    lat: str
    station_id: str
    name: str
    lon: float
    lat: float
    provider: str

    def test(self):
        self.lon = "test"
    
provider = "test"
print("SELECT station_id FROM station WHERE provider=" +provider + "\"")

test = [1, 2, 3, 4]
newTest = [(testel) for testel in test]
print(newTest)

try:
    with open("test.json") as bike_json:
        jsonObject = json.load(bike_json)
except JSONDecodeError:
    with open("test.json") as bike_json:
        fehler = bike_json.read()

print(fehler)
print(fehler.startswith("ERR"))