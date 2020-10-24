import rpyc
from rpyc.utils.server import ThreadedServer
import datetime
import os
import threading
import pickle
import logging
import configparser

config=configparser.ConfigParser()
config.read('config.ini')

logging.basicConfig(level=logging.DEBUG,
                    filename='kv_server.log', filemode='w')


class KV_SERVER(rpyc.Service):
    LOCK = threading.Lock()

    def __init__(self):
        super().__init__()
        self.data = dict()

    def on_connect(self, conn):
        time = datetime.datetime.now()

        logging.info(f'Worker connected on {time}.')

        self.read_data()

        print(f'Worker read previous data.')

    def read_data(self):
        try:
            if os.path.getsize('kv_data.txt') > 0:
                self.data = pickle.load(open('kv_data.txt', 'rb'))

        except Exception as e:
            logging.error(e, exc_info=True)
            raise Exception(e)

    def write_data(self):
        try:
            with KV_SERVER.LOCK:
                with open('kv_data.txt', 'wb') as file:

                    pickle.dump(self.data, file)

        except Exception as e:
            logging.error(e, exc_info=True)
            raise Exception(e)

    def on_disconnect(self, conn):
        time = datetime.datetime.now()

        logging.info(f'Worker disconnected on {time}')
        self.write_data()

    def exposed_get(self, index):
        try:
            if index in self.data:

                logging.info(f'Sending data to client')

                return self.data[index]

        except Exception as e:

            logging.error(e, exc_info=True)
            raise Exception(str(e))

    def exposed_set(self, hash_key, data):

        try:

            logging.info(f'Worker trying to add data')

            if hash_key in self.data:
                self.data[hash_key] += data
            else:
                self.data[hash_key] = data

            self.write_data()

        except Exception as e:

            logging.error(e, exc_info=True)
            raise Exception(e)

    def exposed_clear_data(self):
        try:

            self.data = dict()

        except Exception as e:
            logging.error(e, exc_info=True)
            raise Exception(e)


if __name__ == "__main__":
    rpyc.core.protocol.DEFAULT_CONFIG['sync_request_timeout'] = None
    rpyc.core.protocol.DEFAULT_CONFIG['allow_pickle'] = True
    rpyc.core.protocol.DEFAULT_CONFIG['allow_all_attrs'] = True

    t = ThreadedServer(KV_SERVER,
                       hostname=config['MAP_REDUCE']['IP'], port=int(config['MAP_REDUCE']['PORT']),
                       protocol_config=rpyc.core.protocol.DEFAULT_CONFIG)
    t.start()
