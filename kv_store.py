import rpyc
from rpyc.utils.server import ThreadedServer
import datetime
import threading
import logging

logging.basicConfig(level=logging.DEBUG,filename='app.log',filemode='w')


class KV_SERVER(rpyc.Service):
    lock = threading.Lock()

    def __init__(self):
        super().__init__()
        self.data = dict()

    def on_connect(self, conn):
        time = datetime.datetime.now()
        logging.info(f'Worker connected on {time}')

    def on_disconnect(self, conn):
        time = datetime.datetime.now()
        logging.info(f'Worker disconnected on {time}')

    def exposed_get(self,index):

        try:
            return self.data[index]

        except Exception as e:
            logging.error(e)
            raise Exception(str(e))

    def exposed_set(self,hash_key,data):
        logging.info(f'Data in KV Store {self.data}')
        try:
            logging.info(f'Worker trying to add data {(hash_key,data)}')
            with KV_SERVER.lock:
                if hash_key in self.data:
                    self.data[hash_key]+=data
                else:
                    self.data[hash_key]=data

        except Exception as e:
            logging.error(str(e))
            raise Exception(e)

    def exposed_clear_data(self):
        try:
            with KV_SERVER.lock:
                self.data=dict()

        except Exception as e:
            logging.error(str(e))


if __name__ == "__main__":
    rpyc.core.protocol.DEFAULT_CONFIG['sync_request_timeout'] = None
    t = ThreadedServer(KV_SERVER(),
                       hostname='0.0.0.0', port=8080,protocol_config=rpyc.core.protocol.DEFAULT_CONFIG)
    t.start()