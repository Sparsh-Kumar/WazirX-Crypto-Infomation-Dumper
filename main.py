import sys
import json
from logger import Logger
from loadenv import loadEnvironmentVariables
from request import Requests
from wazirxHelper import WazirXHelper


class SyncData(WazirXHelper):
    def __init__(self, creds, requestInstance, loggerInstance):
        super().__init__(creds, requestInstance, loggerInstance)

    def loadOneMinuteData(self, symbol):
        try:
            if not symbol:
                raise Exception('Symbol is required.')
            kLineDataBefore1Min = json.loads(
                self.kLineDataBeforeXMin(symbol, None, 1).content
            )
            print(kLineDataBefore1Min)
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
    syncData.loadOneMinuteData('shibinr')


if __name__ == '__main__':
    main()
