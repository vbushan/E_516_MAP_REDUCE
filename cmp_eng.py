import googleapiclient.discovery
import configparser
import time
#import logging

#logging.basicConfig(level=logging.DEBUG,filename='worker-trigger.log',filemode='w')


config = configparser.ConfigParser()
config.read('config.ini')

project = config['MAP_REDUCE']['PROJECT_ID']
zone = config['MAP_REDUCE']['ZONE']
image=config['TEMPLATE']['IMAGE_NAME']

compute=googleapiclient.discovery.build('compute', 'v1')

def list_instances():
    result = compute.instances().list(project=project, zone=zone).execute()
    return result['items'] if 'items' in result else None


def create_instance(name,startup_script):
    image_response = compute.images().get(project=project, image=image).execute()

    source_disk_image = image_response['selfLink']

    # Configure the machine
    machine_type = "zones/%s/machineTypes/n1-standard-1" % zone

    config = {
        'name': name,
        'machineType': machine_type,

        # Specify the boot disk and the image to use as a source.
        'disks': [
            {
                'boot': True,
                'autoDelete': True,
                'initializeParams': {
                    'sourceImage': source_disk_image,
                }
            }
        ],

        # Specify a network interface with NAT to access the public
        # internet.
        'networkInterfaces': [{
            'network': 'global/networks/default',
            'accessConfigs': [
                {'type': 'ONE_TO_ONE_NAT', 'name': 'External NAT'}
            ]
        }],

        # Allow the instance to access cloud storage and logging.
        'serviceAccounts': [{
            'email': 'default',
            'scopes': [
                'https://www.googleapis.com/auth/devstorage.read_write',
                'https://www.googleapis.com/auth/logging.write',
                'https://www.googleapis.com/auth/cloud-platform'
            ]
        }],


        'metadata': {
            'items': [{
                # Startup script is automatically executed by the
                # instance upon startup.
                'key': 'startup-script',
                'value': startup_script
            }]
        }

    }

    return compute.instances().insert(
        project=project,
        zone=zone,
        body=config).execute()

def wait_for_operation(operation):

    print('Waiting for operation to finish...')

    while True:
        result = compute.zoneOperations().get(
            project=project,
            zone=zone,
            operation=operation).execute()

        if result['status'] == 'DONE':
            print("done.")
            if 'error' in result:
                raise Exception(result['error'])
            return result

        time.sleep(1)

def delete_instance(name):

    operation= compute.instances().delete(
        project=project,
        zone=zone,
        instance=name).execute()
    
    result=wait_for_operation(operation['name'])
    while True:
        if result['status'] == 'DONE':
            print('Deleted instance',name)
            break

def get_ip(name):

    instance = compute.instances().get(
        project=project, zone=zone, instance=name).execute()

    int_ip = instance['networkInterfaces'][0]['networkIP']
    ext_ip = instance['networkInterfaces'][0]['accessConfigs'][0]['natIP']

    return int_ip,ext_ip


