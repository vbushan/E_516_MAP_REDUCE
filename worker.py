import rpyc
from rpyc.utils.server import ThreadedServer
import datetime
import logging
import configparser
import itertools
import cmp_eng
from functools import reduce

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

                if func=='map_word_count':
                    result=self.map_wc(data)

                elif func=='map_inv_ind':
                    result=self.map_inv_ind(data)

            elif role=="REDUCER":

                if func=='red_word_count':
                    result=self.red_wc(index)

                elif func=='red_inv_ind':
                    result=self.red_inv_ind(index)

            return 1,result

        except Exception as e:
            logging.error(e)
            raise Exception(str(e))

    def map_wc(self,data):

        result=[]
        try:
            for _,file in data:
                words=file.split(" ")
                result+=list(map(lambda x: (x, 1), words))
            logging.info(f'Mapper Result {result}')
            
            rpyc.core.protocol.DEFAULT_CONFIG['sync_request_timeout'] = None
            conn = rpyc.connect(KV_SERVER_IP, KV_SERVER_PORT, config=rpyc.core.protocol.DEFAULT_CONFIG)
            kv_server = conn.root
            logging.info('Connected to KV Store')
            
            def hash_func(item):

                word=item[0]
                return sum([ord(c) for c in word])%num_reducers
            
            logging.info('Sending result to KV')
            for hash_key,group in itertools.groupby(result,hash_func):
                logging.info(f'{hash_key},{group}')
                kv_server.set(hash_key,list(group))
            
            logging.info('Completed Task')
            return "Completed Task"
        except Exception as e:
            logging.error(e)
            raise Exception(str(e))

    def map_inv_ind(self,data):
        result=[]
        try:
            for index,file in data:
                words=file.split()
                result+=list(map(lambda x: (x, index), words))
            
            logging.info(f'Mapper result {result}')
            
            rpyc.core.protocol.DEFAULT_CONFIG['sync_request_timeout'] = None
            conn = rpyc.connect(KV_SERVER_IP, KV_SERVER_PORT, config=rpyc.core.protocol.DEFAULT_CONFIG)
            kv_server = conn.root

            def hash_func(item):

                word=item[0]
                return sum([ord(c) for c in word])%num_reducers
            
            logging.info('Sending result to KV')
            
            for hash_key,group in itertools.groupby(result,hash_func):
                logging.info(f'{hash_key},{group}')
                kv_server.set(hash_key,list(group))

            return "Completed Task"
        except Exception as e:
            logging.error(e)
            raise Exception(str(e))

    def red_wc(self,index):
        try:
            rpyc.core.protocol.DEFAULT_CONFIG['sync_request_timeout'] = None
            conn = rpyc.connect(KV_SERVER_IP, KV_SERVER_PORT, config=rpyc.core.protocol.DEFAULT_CONFIG)
            kv_server = conn.root

            data=kv_server.get(index)

            result=[]
            for word,group in itertools.groupby(data,lambda x:x[0]):
                result.append((word,reduce(lambda x,y:x[1]+y[1],list(group))))

            return result
        except Exception as e:
            logging.error(e)
            raise Exception(e)

    def red_inv_ind(self,index):
        try:
            rpyc.core.protocol.DEFAULT_CONFIG['sync_request_timeout'] = None
            conn = rpyc.connect(KV_SERVER_IP, KV_SERVER_PORT, config=rpyc.core.protocol.DEFAULT_CONFIG)
            kv_server = conn.root

            data = kv_server.get(index)

            result = []
            for word, group in itertools.groupby(data, lambda x: x[0]):
                temp_set=set()
                for _,file_index in list(group):
                    temp_set.add(file_index)

                result.append((word,list(temp_set)))

            return result
        
        except Exception as e:
            logging.error(e)
            raise Exception(e)

    def exposed_add(self,a,b):
        return a+b

if __name__ == "__main__":
    rpyc.core.protocol.DEFAULT_CONFIG['sync_request_timeout'] = None
    t = ThreadedServer(Worker, hostname='0.0.0.0', port=8080,protocol_config=rpyc.core.protocol.DEFAULT_CONFIG)
    t.start()
