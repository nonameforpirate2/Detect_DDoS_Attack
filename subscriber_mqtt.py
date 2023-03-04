"""
Author: Benazir de la Rosa
Contact: benazir.delarosa@gmail.com
"""

import ssl
import paho.mqtt.client as paho
import paho.mqtt.subscribe as subscribe
from paho import mqtt
import datetime
import pandas as pd

class create_subscriber(object):
    """
    The main purpose fo this class is to create a subscriber
    for an emulated object within the local computer. The
    subscriber connects to a specific topic with the help of
    the MQTT broker. 

    Parameters
    ----------
    :username: username to access MQTT broker (String Type). 
    :password: passoword assign to username from MQTT broker  (String Type). 
    :hostname: hostname to access MQTT broker (String Type).
    :device_name: name of the device to be emulated by the computer (String Type).
    :topic: topic which is going to handle the electronic device (String Type).
    :path_to_save_data: path within OS system in the computer where the info 
                        read by the subscriber will be saved (String Type). 

    Functions
    ---------

    :decode_sparkplug_b:
        Prints a mqtt message to stdout ( used as callback for subscribe )

        Parameters
        ----------
        :client: the client itself
        :userdata: userdata is set when initiating the client, here it is userdata=None
        :message: the message with topic and payload.
            
        Returns
        -------
        :topic: topic where we are connected (String Type).
        :payload: decoded sparkplug b info (String Type).

    :main:
        This functions creates the communication channel between
        local computer and MQTT broker to read messages from 
        emulated devices
        
        Parameters
        ----------
        None

        Returns
        -------
        None
    """

    global device_name
    global path_to_save_data

    def __init__(self, device_name, path_to_save_data, topic, username, password, hostname):
        self.device_name = device_name
        create_subscriber.device_name = device_name
        create_subscriber.path_to_save_data = path_to_save_data
        self.path_to_save_data = path_to_save_data
        self.topic = topic
        self.username = username
        self.password = password
        self.hostname = hostname
        pass

    @staticmethod
    def decode_sparkplug_b(client, userdata, message):
        """
            Prints a mqtt message to stdout ( used as callback for subscribe )

            Parameters
            ----------
            :client: the client itself
            :userdata: userdata is set when initiating the client, here it is userdata=None
            :message: the message with topic and payload.

            Returns
            -------
            :topic: topic where we are connected (String Type).
            :payload: decoded sparkplug b info (String Type).
        """
        path_to_save_data = create_subscriber.path_to_save_data
        device_name = create_subscriber.device_name
        df_msg = pd.DataFrame({str(message.topic):[float(message.payload)],'date':datetime.datetime.now()})
        name_ = path_to_save_data + '\\' + str(device_name) + '_' + str(datetime.datetime.now()).replace(':','_') + '.csv'
        df_msg.to_csv(name_, sep='\t')
        print("new message from: " + device_name)
        print("%s : %s" % (message.topic, message.payload))
        return message.topic, message.payload
    
    def main(self):
        """
        This functions creates the communication channel between
        local computer and MQTT broker to read messages from 
        emulated devices
        
        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        sslSettings = ssl.SSLContext(mqtt.client.ssl.PROTOCOL_TLS)
        auth = {'username': self.username, 'password': self.password}
        subscribe.callback(create_subscriber.decode_sparkplug_b, self.topic, hostname=self.hostname, port=8883, auth=auth,
                    tls=sslSettings, protocol=paho.MQTTv31)    