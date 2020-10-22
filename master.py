import rpyc
from rpyc.utils.server import ThreadedServer
import datetime
import configparser
import concurrent.futures
import traceback
from worker_trigger import start_worker_instance
import cmp_eng

config=configparser.ConfigParser()
config.read('config.ini')

class Master(rpyc.Service):
    def __init__(self):
        super().__init__()
        self.cluster = dict()
        self.num_mappers=int(config['MASTER']['NUM_MAPPERS'])
        self.num_reducers=int(config['MASTER']['NUM_REDUCERS'])


    def on_connect(self, conn):
        time=datetime.datetime.now()
        print('Client connected on',time)

    def on_disconnect(self, conn):
        time=datetime.datetime.now()
        print('Client disconnected on',time)

    def exposed_init_cluster(self):
        with concurrent.futures.ProcessPoolExecutor() as executor:
            mapper_names = [config['MAPPER']['NAME'] +
                            str(i) for i in range(1, self.num_mappers+1)]

            mappers = executor.map(self.spawn_worker, mapper_names)

            reducer_names = [config['REDUCER']['NAME'] +
                             str(i) for i in range(1, self.num_reducers+1)]

            reducers = executor.map(self.spawn_worker, reducer_names)

        print('Number of mappers created',len(mappers))
        print('Number of reducers created',len(reducers))

    def exposed_destroy_cluster(self):
        pass

    def exposed_run_map_reduce(self,in_loc,map_func,red_func,out_loc):
        pass

    def spawn_worker(self,worker_name):
        try:

            return (worker_name,start_worker_instance(worker_name))
        except Exception as e:
            traceback.print_exc()



if __name__=="__main__":
    rpyc.core.protocol.DEFAULT_CONFIG['sync_request_timeout'] = None
    t=ThreadedServer(Master,
    hostname='0.0.0.0',port=8080,protocol_config=rpyc.core.protocol.DEFAULT_CONFIG)
    t.start()