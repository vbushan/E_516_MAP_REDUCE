import rpyc
from rpyc.utils.server import ThreadedServer
import datetime
import threading
import logging
import os
import json
logging.basicConfig(level=logging.DEBUG,filename='app.log',filemode='w')




class KV_SERVER(rpyc.Service):

    def __init__(self):
        super().__init__()
        self.data = dict()

    def on_connect(self, conn):
        time = datetime.datetime.now()
        logging.info(f'Worker connected on {time}')
        self.read_data()

    def on_disconnect(self, conn):
        time = datetime.datetime.now()
        logging.info(f'Worker disconnected on {time}')
        self.write_data()

    def exposed_get(self,index):

        try:

            if index in self.data:
                logging.info(f'Sending data to client {self.data[index]}')
                return self.data[index]

        except Exception as e:
            logging.error(e)
            raise Exception(str(e))

    def exposed_set(self,hash_key,data):
        logging.info(f'Data in KV Store {self.data}')
        try:
            logging.info(f'Worker trying to add data {hash_key,data}')

            if str(hash_key) in self.data:
                self.data[str(hash_key)]+=data
            else:
                self.data[str(hash_key)]=data

        except Exception as e:
            logging.error(str(e))
            raise Exception(e)

    def write_data(self):
        try:

            with open('./kv_data.json','w', encoding="utf-8") as file:
                json.dump(self.data,file)

        except Exception as e:
            logging.error(str(e))

    def read_data(self):
        try:
            if 'kv_data.json' in os.listdir('./'):
                with open('./kv_data.json', 'r', encoding="utf-8") as file:
                    self.data = json.load(file)

        except Exception as e:
            logging.error(str(e))
            raise Exception(str(e))


if __name__ == "__main__":
    rpyc.core.protocol.DEFAULT_CONFIG['sync_request_timeout'] = None
    t = ThreadedServer(KV_SERVER,
                       hostname='0.0.0.0', port=8080,protocol_config=rpyc.core.protocol.DEFAULT_CONFIG)
    t.start()