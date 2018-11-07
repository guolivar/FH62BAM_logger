#!/usr/bin/env python
# Load the libraries
import serial # Serial communications
import time # Timing utilities
import subprocess # Shell utilities ... compressing data files
import httplib, urllib   # http and url libs used for HTTP POSTs
import os,sys           # OS utils to keep working directory

# Change working directory to the script's path
os.chdir(os.path.dirname(sys.argv[0]))

# Set the time constants
rec_time=time.gmtime()
timestamp = time.strftime("%Y/%m/%d %H:%M:%S GMT",rec_time)
prev_minute=rec_time[4]
# Read the settings from the settings file
settings_file = open("./settings.txt")
# e.g. "/dev/ttyUSB0"
port = settings_file.readline().rstrip('\n')
print(port)
# path for data files
# e.g. "/home/logger/datacpc3775/"
datapath = settings_file.readline().rstrip('\n')
print(datapath)
prev_file_name = datapath+time.strftime("%Y%m%d.txt",rec_time)
flags = settings_file.readline().rstrip().split(',')
print(timestamp + flags[0])

current_LOG_name = datapath + time.strftime("%Y%m%d.LOG", rec_time)
current_file = open(current_LOG_name, "a")
current_file.write(timestamp + " Logging starts\n")
current_file.write(timestamp + " " + port + "\n")
current_file.write(timestamp + " " + datapath + "\n")
current_file.write(timestamp + " " + flags[0] + "\n")
current_file.flush()
current_file.close()

# Phant address
phant_server = settings_file.readline().rstrip('\n')
# Phant publicKey
publickey = settings_file.readline().rstrip('\n')
# Phant privateKey
privatekey = settings_file.readline().rstrip('\n')

# Close the settings file
settings_file.close()

print('Setting up Serial Port')
# Open the serial port and clean the I/O buffer
ser = serial.Serial(port,9600,parity = serial.PARITY_EVEN,bytesize = serial.SEVENBITS, rtscts=1, stopbits=2)
ser.flushInput()
ser.flushOutput()
# Start the logging
while True:
	# Set the time for the record
	rec_time=time.gmtime()
	# Set the time for the next record (add seconds to current time)
	rec_time_s = int(time.time()) + 60
	timestamp = time.strftime("%Y/%m/%d %H:%M:%S GMT",rec_time)
	# Set concentration to ERROR
	concentration = -999
	# Request current reading from the instrument
	ser.write('C\r')
	file_line = ser.readline().rstrip()
	concentration = eval(file_line)
	# Request current 30min reading from the instrument
	ser.write('H\r')
	file_line = file_line + ',' + ser.readline().rstrip()
	# Request current air flow rate
	ser.write('J2\r')
	file_line = file_line + ',' + ser.readline().rstrip()
	# Request current T1 (sampling head T)
	ser.write('JB\r')
	file_line = file_line + ',' + ser.readline().rstrip()
	# Request current T2 (sampling chamber)
	ser.write('JC\r')
	file_line = file_line + ',' + ser.readline().rstrip()
	# Request current T3 (inside monitor)
	ser.write('JD\r')
	file_line = file_line + ',' + ser.readline().rstrip()
	# Request current T4 (sampling tube)
	ser.write('JE\r')
	file_line = file_line + ',' + ser.readline().rstrip()
	# Request current operating flow
	ser.write('JI\r')
	file_line = file_line + ',' + ser.readline().rstrip()
	# Request device status
	ser.write('#\r')
	file_line = file_line + ',' + ser.readline().rstrip()
	# Make the line pretty for the file
	file_line = timestamp + ',' + file_line
	print(file_line)
	# Save it to the appropriate file
	current_file_name = datapath+time.strftime("%Y%m%d.txt",rec_time)
	current_file = open(current_file_name,"a")
	current_file.write(file_line+"\n")
	current_file.flush()
	current_file.close()
	file_line = ""

	#Send concentration only data to Phant phant_server
	print("Sending an update!")
    # Our first job is to create the data set. Should turn into
    # something like "light=1234&switch=0&name=raspberrypi"
    # fields = ["co", "no2", "co_t", "no2_t", "serial_co", "serial_no2"]
    data = {} # Create empty set, then fill in with our three fields:
    # Field 0, co
    data[fields[0]] = concentration

    # Next, we need to encode that data into a url format:
    params = urllib.urlencode(data)

    # Now we need to set up our headers:
    headers = {} # start with an empty set
    # These are static, should be there every time:
    headers["Content-Type"] = "application/x-www-form-urlencoded"
    headers["Connection"] = "close"
    headers["Content-Length"] = len(params) # length of data
    headers["Phant-Private-Key"] = privatekey # private key header

    # This is very breakable so we try to catch the upload errors
    try:
        # Now we initiate a connection, and post the data
        c = httplib.HTTPConnection(phant_server,8080)
        # Here's the magic, our reqeust format is POST, we want
        # to send the data to phant.server/input/PUBLIC_KEY.txt
        # and include both our data (params) and headers
        print(params)
        print(headers)
        c.request("POST", "/input/" + publickey + ".txt", params, headers)
        r = c.getresponse() # Get the server's response and print it
        print r.status, r.reason
    except:
        print("Connection error. No data upload. Nothing to se here, move along")
        current_LOG_name = datapath + time.strftime("%Y%m%d.LOG", rec_time)
        current_file = open(current_LOG_name, "a")
        current_file.write(timestamp + " Connection error\n")
        current_file.flush()
        current_file.close()
	## Compress data if required
	# Is it the last minute of the day?
	if flags[1]==1:
		if current_file_name != prev_file_name:
			gzfile = prev_file_name + ".gz"
			if sys.platform.startswith('linux'):
				subprocess.call(["gzip",prev_file_name])
			elif sys.platform.startswith('win'):
				subprocess.call(["7za","a","-tgzip", gzfile, prev_file_name])
		prev_file_name = current_file_name
	# Wait until the next minute
	while int(time.time())<=(rec_time_s):
		#wait a few miliseconds
		time.sleep(0.05)
print('I\'m done now')
ser.close()
