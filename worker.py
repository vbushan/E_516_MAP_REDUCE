import rpyc
from rpyc.utils.server import ThreadedServer
import datetime
import logging
import configparser
import cmp_eng
import re
from google.cloud import storage


config=configparser.ConfigParser()
config.read('config.ini')

num_mappers=int(config['MASTER']['NUM_MAPPERS'])
num_reducers=int(config['MASTER']['NUM_REDUCERS'])

KV_SERVER_NAME=config['KV_SERVER']['NAME']
_,KV_SERVER_IP=cmp_eng.get_ip(KV_SERVER_NAME)
KV_SERVER_PORT = int(config['MAP_REDUCE']['PORT'])


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

        try:
            if role=="MAPPER":

                if func==config['MAPPER']['WORD_COUNT_MAP_FUNC']:
                    result=self.map_wc(data)

                elif func==config['MAPPER']['INVERTED_INDEX_MAP_FUNC']:
                    result=self.map_inv_ind(data)

            elif role=="REDUCER":

                if func==config['REDUCER']['WORD_COUNT_REDUCER_FUNC']:
                    result=self.red_wc(index)

                elif func==config['REDUCER']['INVERTED_INDEX_REDUCER_FUNC']:
                    result=self.red_inv_ind(index)

            return 1,result

        except Exception as e:
            logging.error(e,exc_info=True)
            raise Exception(str(e))

    def map_wc(self,data):

        result=[]
        try:
            logging.info('Starting Map task')
            for _,file in data:
                words=file.split(" ")

                for word in words:
                    if re.fullmatch(r'[a-zA-Z]+', word):
                        result+=[(word,1)]


            logging.info(f'Mapper Result {result}')
            
            logging.info(f'Connecting to KV Store...')
            rpyc.core.protocol.DEFAULT_CONFIG['allow_pickle'] = True
            rpyc.core.protocol.DEFAULT_CONFIG['sync_request_timeout'] = None
            conn = rpyc.connect(KV_SERVER_IP, KV_SERVER_PORT, config=rpyc.core.protocol.DEFAULT_CONFIG)
            kv_server = conn.root
            logging.info('Connected to KV Store')
            
            def hash_func(item):
                word=item[0]
                return sum([ord(c) for c in word])%num_reducers

            logging.info('Sending result to KV')

            store = dict()

            for item in result:
                hash_key = hash_func(item)
                if hash_key not in store:
                    store[hash_key] = [item]
                else:
                    store[hash_key] += [item]
            
            logging.info(f'Keys- {list(store.items())}')

            for key,value in store.items():
                logging.info(f'Sending data {(key,value)}')
                kv_server.set(key,value)

            logging.info('Completed Task')

            return "Completed Task"
        except Exception as e:
            logging.error(e,exc_info=True)
            raise Exception(str(e))

    def map_inv_ind(self,data):
        result=[]
        try:
            logging.info('Starting Map task')
            for index,file in data:
                words=file.split()
                for word in words:
                    if re.fullmatch(r'[a-zA-Z]+', word):
                        result+=[(word,index)]
            
            logging.info(f'Mapper result {result}')

            logging.info(f'Connecting to KV Store...')
            rpyc.core.protocol.DEFAULT_CONFIG['sync_request_timeout'] = None
            rpyc.core.protocol.DEFAULT_CONFIG['allow_pickle'] = True
            conn = rpyc.connect(KV_SERVER_IP, KV_SERVER_PORT, config=rpyc.core.protocol.DEFAULT_CONFIG)
            kv_server = conn.root
            logging.info('Connected to KV Store')

            def hash_func(item):

                word=item[0]
                return sum([ord(c) for c in word])%num_reducers

            logging.info('Sending result to KV')

            store = dict()

            for item in result:
                hash_key = hash_func(item)
                if hash_key not in store:
                    store[hash_key] = [item]
                else:
                    store[hash_key] += [item]

            for key, value in store.items():
                kv_server.set(key, value)
            
            logging.info('Completed task')
            return "Completed Task"

        except Exception as e:
            logging.error(e,exc_info=True)
            raise Exception(str(e))

    def red_wc(self,index):
        try:
            logging.info('In word count reducer function')
            rpyc.core.protocol.DEFAULT_CONFIG['sync_request_timeout'] = None
            rpyc.core.protocol.DEFAULT_CONFIG['allow_pickle'] = True
            conn = rpyc.connect(KV_SERVER_IP, KV_SERVER_PORT, config=rpyc.core.protocol.DEFAULT_CONFIG)
            kv_server = conn.root
            logging.info('Connected to KV Store')
            
            logging.info('Getting data from KV Store')
            data=kv_server.get(index)

            if data:
                logging.info(f'Data Received from KV {data}')
            
                store=dict()
                for key,value in data:
                    if key in store:
                        store[key]+=value
                    else:
                        store[key]=value

                logging.info('Completed task')
                
                logging.info('Writing Output to Cloud storage bucket')

                result = list(store.items())

                source_file = f'word_count{index}.txt'

                destination_file = f'word_count_map_red_{index}.txt'

                with open(source_file, 'a+') as file:
                    for key, value in result:
                        file.write(key + "       " + str(value) + '\n')

                storage_client = storage.Client()
                bucket_name = config['MAP_REDUCE']['OUTPUT_LOCATION']
                bucket = storage_client.bucket(bucket_name)

                blob = bucket.blob(destination_file)
                blob.upload_from_filename(source_file)

                logging.info("File {} uploaded to {}.".format(source_file, destination_file))

                return list(store.items())

        except Exception as e:
            logging.error(e,exc_info=True)
            raise Exception(e)

    def red_inv_ind(self,index):
        try:
            logging.info('In inverted index reducer function')
            rpyc.core.protocol.DEFAULT_CONFIG['sync_request_timeout'] = None
            rpyc.core.protocol.DEFAULT_CONFIG['allow_pickle'] = True
            conn = rpyc.connect(KV_SERVER_IP, KV_SERVER_PORT, config=rpyc.core.protocol.DEFAULT_CONFIG)
            kv_server = conn.root
            logging.info('Connected to KV Store')
            data = kv_server.get(index)
            
            logging.info('Received data from KV Store')
            store = dict()

            for key, value in data:
                if key in store:
                    if value not in store[key]:
                        store[key].append(value)
                else:
                    store[key] = [value]
            
            logging.info('Completed task')

            logging.info('Writing Output to Cloud storage bucket')

            result=list(store.items())

            source_file=f'inv_ind_{index}.txt'

            destination_file=f'inverted_index_map_red_{index}.txt'

            with open(source_file, 'a+') as file:
                for key, value in result:
                    file.write(key + "       " + str(sorted(value)) + '\n')

            storage_client = storage.Client()
            bucket_name=config['MAP_REDUCE']['OUTPUT_LOCATION']
            bucket = storage_client.bucket(bucket_name)

            blob = bucket.blob(destination_file)
            blob.upload_from_filename(source_file)

            logging.info("File {} uploaded to {}.".format(source_file, destination_file))

            return list(store.items())
        
        except Exception as e:
            logging.error(e,exc_info=True)
            raise Exception(e)

    def exposed_add(self,a,b):
        return a+b

if __name__ == "__main__":
    rpyc.core.protocol.DEFAULT_CONFIG['sync_request_timeout'] = None
    t = ThreadedServer(Worker,
                       hostname=config['MAP_REDUCE']['IP'], port=int(config['MAP_REDUCE']['PORT']),
                       protocol_config=rpyc.core.protocol.DEFAULT_CONFIG)
    t.start()
