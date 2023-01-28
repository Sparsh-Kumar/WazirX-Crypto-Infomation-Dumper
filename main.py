import sys
import json
import time
import pandas as pd
from logger import Logger
from loadenv import loadEnvironmentVariables
from request import Requests
from wazirxHelper import WazirXHelper
from pymongo import MongoClient


class SyncData(WazirXHelper):
    def __init__(self, creds, requestInstance, loggerInstance):
        super().__init__(creds, requestInstance, loggerInstance)

    def loadOneMinuteData(self, symbol):
        try:
            resultantArray = []
            if not symbol:
                raise Exception('Symbol is required.')
            kLineDataBefore1Min = json.loads(
                self.kLineDataBeforeXMin(symbol, None, 1).content
            )
            for data in kLineDataBefore1Min:
                result = {
                    'logDate': str(pd.to_datetime(data[0], unit='s')),
                    'Open': data[1],
                    'High': data[2],
                    'Low': data[3],
                    'Close': data[4],
                    'Volume': data[5],
                }
                resultantArray.append(result)
            return resultantArray
        except Exception as e:
            self.loggerInstance.logError(str(e))
            sys.exit()


def main():
    loggerInstance = Logger()
    jsonEnvContent = loadEnvironmentVariables(loggerInstance, 'wazirx.json')
    requestInstance = Requests(jsonEnvContent['baseURI'], {
        'X-API-KEY': jsonEnvContent['ApiKey']
    })
    syncData = SyncData(jsonEnvContent, requestInstance, loggerInstance)
    while True:
        cryptoInfo = syncData.loadOneMinuteData('shibinr')
        if len(cryptoInfo):
            mongoClient = MongoClient(jsonEnvContent['DbURI'])
            databaseHandle = mongoClient[jsonEnvContent['DbName']]
            collectionHandle = databaseHandle['records']
            for info in cryptoInfo:
                isAlreadyExists = collectionHandle.find_one({
                    'logDate': info['logDate'],
                })
                if not isAlreadyExists:
                    collectionHandle.insert_one(info)
            mongoClient.close()
        time.sleep(60)


if __name__ == '__main__':
    main()
