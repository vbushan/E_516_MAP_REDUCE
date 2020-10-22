import rpyc
from rpyc.utils.server import ThreadedServer
import datetime
#from worker_functions import mapper_wc, mapper_inv_ind, reducer_wc, reducer_inv_ind


class Worker(rpyc.Service):
    def __init__(self):
        pass

    def on_connect(self, conn):
        time=datetime.datetime.now()
        print('Master connected on',time)

    def on_disconnect(self, conn):
        time=datetime.datetime.now()
        print('Master disconnected on', time)

    def exposed_execute(self, role, func, data, index):
        pass

    def exposed_add(self,a,b):
        return a+b


if __name__ == "__main__":

    t = ThreadedServer(Worker, hostname='0.0.0.0', port=8080)
    t.start()
