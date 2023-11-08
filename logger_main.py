#!/usr/bin/env python
import serial  # Serial communications
import time  # Timing utilities
from datetime import datetime, timedelta
import json  # To send JSON object to MQTT broker
import paho.mqtt.client as mqtt  # MQTT publishing
import psycopg2

# Set the time constants
rec_time = time.gmtime()
timestamp = time.strftime("%Y/%m/%d %H:%M:%S GMT", rec_time)
prev_minute = rec_time[4]
# Read the settings from the settings file
settings_file = open("./settings.txt")
# e.g. "/dev/ttyUSB0"
port = settings_file.readline().rstrip("\n")
print(port)
# path for data files
# e.g. "/home/logger/datacpc3775/"
datapath = settings_file.readline().rstrip("\n")
print(datapath)
prev_file_name = datapath + time.strftime("%Y%m%d.txt", rec_time)
flags = settings_file.readline().rstrip().split(",")
print(timestamp + flags[0])

current_LOG_name = datapath + time.strftime("%Y%m%d.LOG", rec_time)
current_file = open(current_LOG_name, "a")
current_file.write(timestamp + " Logging starts\n")
current_file.write(timestamp + " " + port + "\n")
current_file.write(timestamp + " " + datapath + "\n")
current_file.write(timestamp + " " + flags[0] + "\n")
current_file.flush()
current_file.close()

# MQTT server address
mqtt_server = settings_file.readline().rstrip("\n")
# MQTT topic to publish
mqtt_topic = settings_file.readline().rstrip("\n")
# Start the MQTT client
client = mqtt.Client()
client.connect(mqtt_server, 1883)
# Close the settings file
settings_file.close()
# Database connection
connection = psycopg2.connect(
    user = "your_username",
    password = "your_password",
    host = "localhost",
    port = "5432",
    database = "your_database"
)

while True:
    try:
        while time.gmtime().tm_sec > 0:
            time.sleep(0.05)
            time.gmtime().tm_sec
        # Set the time for the record
        rec_time = time.gmtime()
        timestamp = time.strftime("%Y/%m/%dT%H:%M:%S GMT", rec_time)
        print("Setting up Serial Port")
        ser = serial.Serial(
            port,
            9600,
            parity=serial.PARITY_EVEN,
            bytesize=serial.SEVENBITS,
            stopbits=2,
            timeout=3,
        )
        ser.reset_input_buffer()
        ser.reset_output_buffer()
        buff = ser.readline().strip()
        while len(buff) > 0:
            buff = ser.readline().strip()
            print(buff)
        print(ser.write(b"C\r\n"))
        concentration = ser.readline().strip().decode("UTF-8")
        print(concentration)
        print(ser.write(b"#\r\n"))
        devstatus = ser.readline().strip().decode("UTF-8")
        print(devstatus)
        ser.close()
        json_line = '{"Timestamp":"' + timestamp + '"'
        json_line = json_line + ',"PMnow":' + concentration
        json_line = json_line + ',"DevStatus":' + devstatus
        json_line = json_line + "}"
        # Make the line pretty for the file
        file_line = timestamp + "," + concentration + "," + devstatus
        print(json_line)
        # Save it to the appropriate file
        current_file_name = datapath + time.strftime("%Y%m%d.txt", rec_time)
        current_file = open(current_file_name, "a")
        current_file.write(file_line + "\n")
        current_file.flush()
        current_file.close()
        # Send concentration only data to mqtt_server
        # Start the MQTT client
        client = mqtt.Client()
        client.connect(mqtt_server, 1883)
        print("Sending an update!")
        client.publish(mqtt_topic, json_line)
        # Send data to database -- NOT READY FOR USE!!!!
        # Establish a connection to the database
        try:
            connection = psycopg2.connect(
                user = "your_username",
                password = "your_password",
                host = "localhost",
                port = "5432",
                database = "your_database"
            )

            cursor = connection.cursor()

            # Create a new record
            postgres_insert_query = """ INSERT INTO table_name (column1, column2) VALUES (%s,%s)"""
            record_to_insert = ('value1', 'value2')
            cursor.execute(postgres_insert_query, record_to_insert)

            connection.commit()
            count = cursor.rowcount
            print(count, "Record inserted successfully into table")

        except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to insert record into table", error)

        finally:
            # Close the database connection
            if(connection):
                cursor.close()
                connection.close()
                print("PostgreSQL connection is closed")
    except:
        print("ERROR")
        current_LOG_name = datapath + time.strftime("%Y%m%d.LOG", rec_time)
        current_file = open(current_LOG_name, "a")
        current_file.write(
            timestamp + " Something unexpected happened and data wasn't logged\n"
        )
        current_file.flush()
        current_file.close()
