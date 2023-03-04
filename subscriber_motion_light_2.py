"""
Author: Benazir de la Rosa
Contact: benazir.delarosa@gmail.com
"""

from subscriber_mqtt import create_subscriber
import yaml

if __name__ == '__main__':

    with open('config.yml', 'r') as file:
        config = yaml.safe_load(file)

    # Credentials Per Device
    device_name = config['tcs_hackaton']['device_name']['Motion_Light'][1]
    path_to_save_data = config['tcs_hackaton']['paths']['Motion_light_2_collection']
    topic = config['tcs_hackaton']['topic_type'][7]
    username = config['tcs_hackaton']['username'][0]
    password = config['tcs_hackaton']['password'][0]
    hostname = config['tcs_hackaton']['hostname'][0]
    attribute_to_measure = "Motion Detected"

    subscribe_to_emulate_device = create_subscriber(device_name,path_to_save_data,topic,username,password,hostname)
    subscribe_to_emulate_device.main()      