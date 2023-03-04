"""
Author: Benazir de la Rosa
Contact: benazir.delarosa@gmail.com
"""

import ssl
from paho import mqtt
import paho.mqtt.client as paho
import paho.mqtt.publish as publish
import os
import sys
import ast
from pathlib import Path
import yaml

class emulate_electronic_devices(object):
    """
    This class emulates electronic devices for Hackathon chanllenge.
    The main purpose of the class is to mimic what a regular electronic
    device would do. Send signals to MQTT broker. In this case the class
    reads files with signals from the mimic device.
    
    Parameters
    ----------
    :username: username to access MQTT broker (String Type). 
    :password: passoword assign to username from MQTT broker  (String Type). 
    :hostname: hostname to access MQTT broker (String Type).
    :device_name: name of the device to be emulated by the computer (String Type).
    :topic: topic which is going to handle the electronic device (String Type).
    :path_to_watch: path within OS system in the computer where the files that 
                    mimic electronic device output signals are gonig to be dropped.
    :attribute_to_measure: attribute collected from the electronic device (String Type)
    
    Functions
    ---------

    :send_message_to_topic:

        The purpose of this function is to send message with the state 
        from the electronic device to the subscriber.

        Parameters
        ----------
        :topic: topic which is going to handle the electronic device (String Type).
        :payload: message from attribute (String Type).
        :username: username to access MQTT broker (String Type). 
        :password: passoword assign to username from MQTT broker  (String Type).
        :hostname: hostname to access MQTT broker (String Type).
        
        Returns
        -------
        None 

    :read_info_from_file:

        Reads information send in log format from the electronic 
        device.

        Parameters
        ----------
        :path_to_file: absolute path where the new file is created (String Type).

        Returns
        -------
        :Lines[0]: text with information (String Type). 

    :encode_sparkplug_b:

        This function prepares the information from the emulated 
        electronic device to be encoded to sparkplug b format. 

        Parameters
        ----------
        :param string_text: text from emulated device (String Type).
        :object_type: type of device to be monitored (String Type).
        :read_attribute: attribute within the emited info to be send (String Type).

        Returns
        -------
        :sparkplugb_encoded_info: returns sparkplugb encoded info.

    :watch_for_object:

        This function is responsible for reading emited information from the 
        emulated electronic device and send it back to the MQTT broker in
        Sparkplugb encoded format.

        Parameters
        ----------
        :username: username to access MQTT broker (String Type). 
        :password: passoword assign to username from MQTT broker  (String Type). 
        :hostname: hostname to access MQTT broker (String Type).
        :device_name: name of the device to be emulated by the computer (String Type).
        :topic: topic which is going to handle the electronic device (String Type).
        :path_to_watch: path within OS system in the computer where the files that 
                        mimic electronic device output signals are gonig to be dropped.
        :attribute_to_measure: attribute collected from the electronic device (String Type).

        Returns
        -------
        None

    :main:
        This functions runs the process for emulating the electronic device
        and send encoded sparkplugb data to its corresponding MQTT broker.

        Parameters
        ----------
        None
        
        Returns
        -------
        None
    """

    def __init__(self, username, password, hostname, device_name, topic, path_to_watch, attribute_to_measure):
        self.username = username 
        self.password = password
        self.hostname = hostname
        self.device_name = device_name
        self.topic = topic
        self.path_to_watch = path_to_watch
        self.attribute_to_measure = attribute_to_measure
        pass

    @staticmethod
    def send_message_to_topic(topic,payload,username,password, hostname):
        """
        The purpose of this function is to send message with the state 
        from the electronic device to the subscriber.

        Parameters
        ----------
        :topic: topic which is going to handle the electronic device (String Type).
        :payload: message from attribute (String Type).
        :username: username to access MQTT broker (String Type). 
        :password: passoword assign to username from MQTT broker  (String Type).
        :hostname: hostname to access MQTT broker (String Type).

        Returns
        -------
        None 
        """
        # create a set of 2 test messages that will be published at the same time
        msgs = [{'topic':topic,'payload':payload},("encyclopedia/fridge_1_","test 2",0,False)]
        # use TLS for secure connection with HiveMQ Cloud
        sslSettings = ssl.SSLContext(mqtt.client.ssl.PROTOCOL_TLS)
        # put in your cluster credentials and hostname
        auth = {'username':username,'password':password}
        publish.multiple(msgs,hostname=hostname, port=8883, auth=auth,
                        tls=sslSettings, protocol=paho.MQTTv31)
        pass

    @staticmethod
    def read_info_from_file(path_to_file):
        """
        Reads information send in log format from the electronic 
        device.

        Parameters
        ----------
        :path_to_file: absolute path where the new file is created (String Type).

        Returns
        :Lines[0]: text with information (String Type). 
        """
        file = open(path_to_file,'r')
        lines = file.readlines()
        return lines[0]

    @staticmethod
    def encode_sparkplug_b(string_text,object_type, read_attribute):
        """
        This function prepares the information from the emulated 
        electronic device to be encoded to sparkplug b format. 

        Parameters
        ----------
        :param string_text: text from emulated device (String Type).
        :object_type: type of device to be monitored (String Type).
        :read_attribute: attribute within the emited info to be send (String Type).

        Returns
        -------
        :sparkplugb_encoded_info: returns sparkplugb encoded info.
        """
        dict_string = ast.literal_eval(string_text)
        message = {
            "metrics":[{
                "name":object_type,
                "alias":22,
                "datatype":9,
                "isNull":False,
                "floatValue":1
            }],
            "seq":-1
        }
        return dict_string[read_attribute]

    @staticmethod
    def watch_for_object(path_to_watch, device_name, attribute_to_measure, topic, username, password, hostname):
        """
        This function is responsible for reading emited information from the 
        emulated electronic device and send it back to the MQTT broker in
        Sparkplugb encoded format.

        Parameters
        ----------
        :username: username to access MQTT broker (String Type). 
        :password: passoword assign to username from MQTT broker  (String Type). 
        :hostname: hostname to access MQTT broker (String Type).
        :device_name: name of the device to be emulated by the computer (String Type).
        :topic: topic which is going to handle the electronic device (String Type).
        :path_to_watch: path within OS system in the computer where the files that 
                        mimic electronic device output signals are gonig to be dropped.
        :attribute_to_measure: attribute collected from the electronic device (String Type)

        Returns
        -------
        None
        """
        old = os.listdir(path_to_watch)
        while True:
            new = os.listdir(path_to_watch)
            if len(new) > len(old):
                newfile = list(set(new)-set(old))
                old = new
                extension = os.path.splitext(path_to_watch + "/" + newfile[0])[1]
                if extension == ".log":
                    try:
                        file_path = path_to_watch + newfile[0]
                        print("I got a file path ok: " + file_path)
                        os.chmod(file_path , 0o777)
                        print("we are able to change permit mode")
                        contents = Path(file_path).read_text()
                        if '\n' in contents:
                            contents = contents.split('\n')[0]
                        print("I was able to read content: " + contents)
                        msgs = emulate_electronic_devices.encode_sparkplug_b(contents, device_name,attribute_to_measure)
                        emulate_electronic_devices.send_message_to_topic(topic,msgs,username,password,hostname)
                    except:
                        print("error reading file: " + file_path)
                else:
                    continue
            else:
                continue
    
    def main(self):
        """
        This functions runs the process for emulating the electronic device
        and send encoded sparkplugb data to its corresponding MQTT broker.

        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        emulate_electronic_devices.watch_for_object(self.path_to_watch, self.device_name, self.attribute_to_measure, self.topic, self.username, self.password, self.hostname)
        pass

