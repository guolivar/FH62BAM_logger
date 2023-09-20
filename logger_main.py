#!/usr/bin/env python

# Is it better to run it as a CRON job once every minute instead of an infinite loop?
# 	Potentially better error handling and failure recovery
# 	Need to be very careful with PATHS and users.


# Load the libraries
import pdb  # Debugger
import serial  # Serial communications
import time  # Timing utilities
import subprocess  # Shell utilities ... compressing data files
import os, sys  # OS utils to keep working directory
import json  # To send JSON object to MQTT broker
import paho.mqtt.client as mqtt  # MQTT publishing
import boto3  # AWS client
from botocore.config import Config  # Configure AWS client

# Functions


# Manual, more flexible, "serial readline"
def Serial_Readline(_ser, _eol):
    leneol = len(_eol)
    _bline = bytearray()
    while True:
        c = _ser.read(1)
        _bline += c
        if _bline[-leneol:] == _eol:
            break
    ## Parse the data line
    _line = _bline.decode("utf-8").rstrip()
    return _line


##############################################################
# AWS bits
aws_secrets = open("./secret_aws.txt")
aws_cred = aws_secrets.readline().split(";")

my_S3_config = Config(
    region_name="ap-southeast-2",
    signature_version="v4",
    retries={"max_attempts": 10, "mode": "standard"},
)

clientS3 = boto3.client(
    "s3",
    aws_access_key_id=aws_cred[0],
    aws_secret_access_key=aws_cred[1],
    config=my_S3_config,
)
# Let's use Amazon S3
s3 = boto3.resource("s3")
##############################################################
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
# Hacks to work with custom end of line
eol = b"\r\n"
# Start at the beginning of the minute
while time.gmtime().tm_sec > 0:
    time.sleep(0.05)
    time.gmtime().tm_sec
# breakpoint()
# Start the logging
while True:
    try:
        # Set the time for the record
        rec_time = time.gmtime()
        # Set the time for the next record (add seconds to current time)
        rec_time_s = int(time.time()) + 60
        timestamp = time.strftime("%Y/%m/%dT%H:%M:%S GMT", rec_time)
        print("Setting up Serial Port")
        # Open the serial port and clean the I/O buffer
        ser = serial.Serial(
            port,
            9600,
            parity=serial.PARITY_EVEN,
            bytesize=serial.SEVENBITS,
            rtscts=1,
            stopbits=2,
        )
        ser.flushInput()
        ser.flushOutput()
        # Set concentration to ERROR
        concentration = -999
        # Request current reading from the instrument
        # breakpoint()
        print("Request concentration")
        ser.write(b"C\r\n")
        print("Concentration requested")
        # breakpoint()
        c_read = Serial_Readline(ser, eol)
        json_line = '{\\"PMnow\\":' + c_read
        file_line = c_read
        concentration = eval(file_line)
        # Request current 30min reading from the instrument
        print("Request 30min avg")
        ser.write(b"H\r\n")
        c_read = Serial_Readline(ser, eol)
        file_line = file_line + "," + c_read
        json_line = json_line + ',\\"PM30min\\":' + eval(c_read)
        # Request current air flow rate
        ser.write(b"J2\r\n")
        c_read = Serial_Readline(ser, eol)
        file_line = file_line + "," + c_read
        json_line = json_line + ',\\"Airflow\\":' + eval(c_read)
        # Request current T1 (sampling head T)
        ser.write(b"JB\r\n")
        c_read = Serial_Readline(ser, eol)
        file_line = file_line + "," + c_read
        json_line = json_line + ',\\"SamplT\\":' + eval(c_read)
        # Request current T2 (sampling chamber)
        ser.write(b"JC\r\n")
        c_read = Serial_Readline(ser, eol)
        file_line = file_line + "," + c_read
        json_line = json_line + ',\\"ChambT\\":' + eval(c_read)
        # Request current T3 (inside monitor)
        ser.write(b"JD\r\n")
        c_read = Serial_Readline(ser, eol)
        file_line = file_line + "," + c_read
        json_line = json_line + ',\\"MonitT\\":' + eval(c_read)
        # Request current T4 (sampling tube)
        ser.write(b"JE\r\n")
        c_read = Serial_Readline(ser, eol)
        file_line = file_line + "," + c_read
        json_line = json_line + ',\\"SampTubeT\\":' + eval(c_read)
        # Request current operating flow
        ser.write(b"JI\r\n")
        c_read = Serial_Readline(ser, eol)
        file_line = file_line + "," + c_read
        json_line = json_line + ',\\"CurrFlow\\":' + eval(c_read)
        # Request device status
        ser.write(b"#\r\n")
        c_read = Serial_Readline(ser, eol)
        file_line = file_line + "," + c_read
        json_line = json_line + ',\\"DevStatus\\":' + eval(c_read) + "}"
        # Make the line pretty for the file
        file_line = timestamp + "," + file_line
        print(file_line)
        # Save it to the appropriate file
        current_file_name = datapath + time.strftime("%Y%m%d.txt", rec_time)
        current_file = open(current_file_name, "a")
        current_file.write(file_line + "\n")
        current_file.flush()
        current_file.close()
        file_line = ""
        # Send concentration only data to mqtt_server
        print("Sending an update!")
        client.publish(mqtt_topic, json_line)
        ## Compress data if required
        # Is it the last minute of the day?
        if current_file_name != prev_file_name:
            gzfile = prev_file_name + ".gz"
            if sys.platform.startswith("linux"):
                subprocess.call(["gzip", prev_file_name])
            elif sys.platform.startswith("win"):
                subprocess.call(["7za", "a", "-tgzip", gzfile, prev_file_name])
            # Upload a new file
            data = open(gzfile, "rb")
            s3.Bucket("hbrc").put_object(Key="BAM/" + gzfile, Body=data)
            prev_file_name = current_file_name
        ser.close()
    except:
        current_LOG_name = datapath + time.strftime("%Y%m%d.LOG", rec_time)
        current_file = open(current_LOG_name, "a")
        current_file.write(
            timestamp + " Something unexpected happened and data wasn't logged\n"
        )
        current_file.flush()
        current_file.close()
    # Wait until the next sample time
    while int(time.time()) <= (rec_time_s):
        # wait a few miliseconds
        time.sleep(0.05)
print("I'm done now")
