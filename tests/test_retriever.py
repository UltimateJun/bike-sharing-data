from bikesharing import Retriever
from datetime import date, datetime
import os, pytest
import json
from json.decoder import JSONDecodeError

class TestRetrieverClass:
    # test if generated directory path is correct
    def testCurrentTimePath(self):
        storagePath = Retriever.StoragePath()
        path = storagePath.getPathForCurrentTime()

        # get date and format it
        today = date.today()
        today_formatted = today.strftime("%Y-%m-%d")
        # get time and format it (hh-mm)
        now = datetime.now()
        now_formatted = now.strftime("%H-%M")
        self.path = "json/" + today_formatted + "/" + now_formatted + "/"

        assert path == self.path

    # test if method to create directory works
    def testCreateDir(self, tmp_path):
        storagePath = Retriever.StoragePath()
        dirPath = tmp_path / "test"
        storagePath.createDir(dirPath)
        assert os.path.isdir(tmp_path / "test")
    
    # test if string writer works with a test string
    def testStringWriter(self):
        storagePath = Retriever.StoragePath()
        stringWriter = Retriever.StringWriter(storagePath)
        stringWriter.createDirAndFile("test", "test.txt")

        stringWriterPath = storagePath.getPathForCurrentTime() + "test.txt"
        with open(stringWriterPath) as file:
            assert file.read() == "test"
        # remove file again from folder
        # if OSError arises: do not except as it will let the test fail
        os.remove(stringWriterPath)

    # test URL retriever with two HTTP codes (200 and 500)
    @pytest.mark.parametrize("url, httpCode", [
        # dummy links returning 200 and 500
        ("http://httpstat.us/200", "200 OK"),
        ("http://httpstat.us/500", "ERROR 500")
    ])
    def testURLRetriever(self, tmp_path, url, httpCode):
        storagePath = Retriever.StoragePath()
        successWriter = Retriever.JSONWriter(storagePath)
        errorWriter = Retriever.StringWriter(storagePath)
        
        urlRetriever = Retriever.URLRetriever(successWriter, errorWriter)
        urlRetriever.retrieveURL((str(httpCode)+".txt"), url)
        testFilePath = storagePath.getPathForCurrentTime() + (str(httpCode)+".txt")
        with open(testFilePath) as file:
            assert file.read() == httpCode
        # remove file again from folder
        # if OSError arises: do not except as it will let the test fail
        os.remove(testFilePath)

    # test if nextbike file retrieval works
    def testNextbike(self, tmp_path):
        testPath = str(tmp_path) + "/"
        storagePath = Retriever.StoragePath(testPath)
        storagePath.createDir(storagePath.getPathForCurrentTime())
        
        successWriter = Retriever.JSONWriter(storagePath)
        errorWriter = Retriever.StringWriter(storagePath)
        urlRetriever = Retriever.URLRetriever(successWriter, errorWriter)
        Retriever.NextbikeRetriever(urlRetriever)

        path = storagePath.getPathForCurrentTime() + "nextbike.json"
        with open(path) as file:
            try:
                assert json.load(file)
            except JSONDecodeError:
                # if exception occurs, test will fail
                pytest.fail("Not a JSON file!")

    # test with callabike: not possible, as limit of 30 requests / minute would be at risk!
