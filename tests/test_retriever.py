from json.decoder import JSONDecodeError
from bikesharing import Retriever
from datetime import date, datetime
import os, json, pytest

class TestRetrieverClass:
    # test if generated directory path is correct
    def testDirPath(self):
        storagePath = Retriever.StoragePath()
        storagePath.getPathForToday()

        # get date and format it
        today = date.today()
        today_formatted = today.strftime("%Y-%m-%d")
        # get time and format it (hh-mm)
        now = datetime.now()
        now_formatted = now.strftime("%H-%M")

        self.dirPath = "json/" + today_formatted + "/" + now_formatted + "/"

        assert storagePath.dirPath == self.dirPath
    # test if method to create directory works
    def testCreateDir(self, tmp_path):
        storagePath = Retriever.StoragePath()
        storagePath.dirPath = tmp_path / "test"
        storagePath.createDir()
        assert os.path.isdir(tmp_path / "test")
    
    # test if string writer works with a test string
    def testStringWriter(self, tmp_path):
        stringWriter = Retriever.StringWriter("test")
        stringWriterPath = tmp_path / "stringWriterTest.txt"
        stringWriter.writeFile(stringWriterPath)
        with open(stringWriterPath) as file:
            assert file.read() == "test"


    # test URL retriever with two HTTP codes (200 and 500)
    @pytest.mark.parametrize("url, httpCode", [
        # dummy links returning 200 and 500
        ("http://httpstat.us/200", "200 OK"),
        ("http://httpstat.us/500", "ERROR 500")
    ])
    def testURLRetriever(self, tmp_path, url, httpCode):
        urlRetriever = Retriever.URLRetriever(url)
        urlRetriever.openURL()
        testFilePath = tmp_path / (str(httpCode)+".txt")
        urlRetriever.storeFile(testFilePath)
        with open(testFilePath) as file:
            assert file.read() == httpCode

    # test if nextbike file retrieval works
    def testNextbike(self, tmp_path):
        testPath = str(tmp_path) + "/"
        storagePath = Retriever.StoragePath(testPath)
        storagePath.createDir()
        Retriever.NextbikeRetriever(storagePath)

        nextbikePath = tmp_path / "nextbike.json"
        with open(nextbikePath) as file:
            try:
                assert json.load(file)
            except JSONDecodeError:
                # if exception occurs, test will fail
                pytest.fail("Not a JSON file!")

    # test with callabike: not possible, as limit of 30 requests / minute would be at risk!
