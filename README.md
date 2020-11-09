# E 516: Distributed Map Reduce

1.  I have used the Google Client APIs in python to interface with Compute Engine and Google Cloud Storage.
    The APIs can be installed using the following commands:

         pip install --upgrade google-cloud-storage
         pip install --upgrade google-api-python-client

2.  All the instance connections is achieved through an rpc-based infrastructure. I have used the RPyC framework in python to achieve this.
    Make sure you install RPyC library via the following command before running the scripts-

         pip install rpyc

3.  The Master and KV Severs are long-running servers which listen to TCP connections. So, to run the servers, execute the make_file.py file.

4.  You can run run the make_file using the command python3 make_file.py.make_file

5.  I make extensive use of features included in Python versions 3.7+. So, make sure your python version is >=3.7.make_file

6.  After running the make_file, run the client.py file to run the map_reduce job.

