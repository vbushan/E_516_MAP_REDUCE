import rpyc
from rpyc.utils.server import ThreadedServer
import datetime
import logging
#import time
logging.basicConfig(level=logging.DEBUG,filename='worker.log',filemode='w')


class Worker(rpyc.Service):
    def __init__(self):
        pass

    def on_connect(self, conn):

        time=datetime.datetime.now()
        logging.info(f'Master connected on {time}')

    def on_disconnect(self, conn):
        time=datetime.datetime.now()
        logging.info(f'Master disconnected on {time}')

    def exposed_execute(self, role, func, data, index):
        pass

    def exposed_add(self,a,b):
        #time.sleep(5)
        return a+b


if __name__ == "__main__":
    rpyc.core.protocol.DEFAULT_CONFIG['sync_request_timeout'] = None
    t = ThreadedServer(Worker, hostname='0.0.0.0', port=8080,protocol_config=rpyc.core.protocol.DEFAULT_CONFIG)
    t.start()
