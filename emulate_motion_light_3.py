"""
Author: Benazir de la Rosa
Contact: benazir.delarosa@gmail.com
"""

from emulate_device_mqtt import emulate_electronic_devices
import yaml

if __name__ == '__main__':
    
    with open('config.yml', 'r') as file:
        config = yaml.safe_load(file)

    #Authenticator Credentials Per Device
    username = config['tcs_hackaton']['username'][0]
    password = config['tcs_hackaton']['password'][0]
    hostname = config['tcs_hackaton']['hostname'][0]
    device_name = config['tcs_hackaton']['device_name']['Motion_Light'][2]
    topic = config['tcs_hackaton']['topic_type'][8]
    path_to_watch = config['tcs_hackaton']['paths']['Motion_light_3_send']
    attribute_to_measure = "Motion Detected"

    emulate_device = emulate_electronic_devices(username, password, hostname, device_name, topic, path_to_watch, attribute_to_measure)
    emulate_device.main()