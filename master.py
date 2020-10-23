import rpyc
from rpyc.utils.server import ThreadedServer
import datetime
import configparser
import concurrent.futures
import traceback
from worker_trigger import start_worker_instance
import cmp_eng
import logging
import os

logging.basicConfig(level=logging.DEBUG,filename='app.log',filemode='w')

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
        mappers=[]
        reducers=[]
        with concurrent.futures.ProcessPoolExecutor() as executor:
            mapper_names = [config['MAPPER']['NAME'] +
                            str(i) for i in range(1, self.num_mappers+1)]

            for name,IP in zip(mapper_names,executor.map(self.spawn_worker, mapper_names)):
                mappers.append((name,IP))

            reducer_names = [config['REDUCER']['NAME'] +
                             str(i) for i in range(1, self.num_reducers+1)]

            for name,IP in zip(reducer_names,executor.map(self.spawn_worker,reducer_names)):
                reducers.append((name,IP))
        


        self.mappers=mappers
        self.reducers=reducers
    
        return (self.mappers,self.reducers)


    def exposed_destroy_cluster(self):
        try:
            mapper_names=[name for name,_ in self.mappers]
            reducer_names=[name for name,_ in self.reducers]

            with concurrent.futures.ProcessPoolExecutor() as executor:
                executor.map(cmp_eng.delete_instance,mapper_names)
                executor.map(cmp_eng.delete_instance,reducer_names)

            return 1

        except Exception as e:
            traceback.print_exc()
            return 0

    def exposed_run_map_reduce(self,in_loc,map_func,red_func,out_loc):
        try:
            self.map_func=map_func
            self.red_func=red_func
            self.out_loc=out_loc

            book_names=list(os.listdir(in_loc))
            map_in_files=[[] for i in range(self.num_mappers)]
            for i in range(len(book_names)):
                index=i%self.num_mappers
                map_in_files[index].append((i+1,book_names[i]))

            mapper_input=[[] for i in range(self.num_mappers)]
            
            for i in range(len(map_in_files)):
                for file_index,file in map_in_files[i]:
                    mapper_input[i].append((file_index,open(in_loc+file,'r',encoding='utf-8').read()))

            mapper_ips=[ip for _,ip in self.mappers ]
            reducer_ips=[ip for _,ip in self.reducers ]

            logging.info(f'Worker IPs- {(mapper_ips,reducer_ips)}')

            logging.info('Starting mapper tasks')

            with concurrent.futures.ProcessPoolExecutor() as executor:
                mapper_responses=executor.map(self.run_mapper,zip(mapper_ips,mapper_input,range(1,self.num_mappers+1)))

            for response in mapper_responses:
                if not response:
                    raise Exception("Mapper task incomplete")

            logging.info('Completed mapper tasks')

            logging.info('Starting reducer tasks')

            with concurrent.futures.ProcessPoolExecutor() as executor:
                reducer_responses=executor.map(self.run_reducer,zip(reducer_ips,range(1,self.num_reducers+1)))

            for response in reducer_responses:
                if not response:
                    raise Exception("Reducer task incomplete")

            logging.info('Completed reducer tasks')

            return 1
        except Exception as e:
            traceback.print_exc()
            logging.error('Error in map reduce task- \n'+str(e))
            return 0

    def run_mapper(self,mapper_inp):
        try:

            mapper_ip=mapper_inp[0]
            inp_data=mapper_inp[1]
            mapper_index=mapper_inp[2]

            rpyc.core.protocol.DEFAULT_CONFIG['sync_request_timeout'] = None
            conn=rpyc.connect(mapper_ip[0],int(config['MAP_REDUCE']['PORT']),config=rpyc.core.protocol.DEFAULT_CONFIG)
            worker=conn.root

            
            logging.debug(f'Mapper {mapper_ip} performing task 1')
            result = worker.add(2, 3)

            logging.debug(f"{mapper_ip} task 1 result- {result}")

            if result!=5:
                raise Exception('Incorrect Result')

            logging.info(f'Mapper {mapper_ip} performing task 2')


            result,_=worker.execute('MAPPER',self.map_func,inp_data,mapper_index)

            logging.debug(f"{mapper_ip} task 2 result- {result}")

            if result!=1:
                raise Exception(f'Something went wrong in mapper- {mapper_ip}')

            conn.close()
            return 1

        except Exception as e:
            traceback.print_exc()
            logging.error('Error in map task- \n' + str(e))
            raise Exception(str(e))

    def run_reducer(self,reducer_inp):
        try:

            reducer_ip=reducer_inp[0]
            reducer_index=reducer_inp[1]

            rpyc.core.protocol.DEFAULT_CONFIG['sync_request_timeout'] = None
            conn = rpyc.connect(reducer_ip[0], int(config['MAP_REDUCE']['PORT']), config=rpyc.core.protocol.DEFAULT_CONFIG)
            worker = conn.root

            logging.debug(f"Reducer {reducer_ip} performing task 1")
            result = worker.add(2, 3)
            logging.debug(f"{reducer_ip} task 1 result- {result}")

            if result != 5:
                raise Exception('Incorrect Result')


            status,result=worker.execute('REDUCER',self.red_func,None,reducer_index)

            if status==1:
                with open(self.out_loc+f'-{reducer_index}.txt','w') as file:
                    file.write()
            
            else:
                raise Exception('Something went wrong in the mapper')
            conn.close()
            return 1

        except Exception as e:
            traceback.print_exc()
            logging.error('Error in reduce task- \n' + str(e))
            raise Exception(str(e))

    def spawn_worker(self,worker_name):
        try:

            return start_worker_instance(worker_name)
        except Exception as e:
            traceback.print_exc()


if __name__=="__main__":
    rpyc.core.protocol.DEFAULT_CONFIG['sync_request_timeout'] = None
    t=ThreadedServer(Master,
    hostname='0.0.0.0',port=8080,protocol_config=rpyc.core.protocol.DEFAULT_CONFIG)
    t.start()