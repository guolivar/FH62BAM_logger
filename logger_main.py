#!/usr/bin/env python
# Load the libraries
import serial # Serial communications
import time # Timing utilities
import subprocess # Shell utilities ... compressing data files
import sys # System info to select compression utility
# Hard restart every 3600 seconds
while True:
	ix=1 # Index for the 3600 iterations before restart
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
	# psql connection string
	# e.g "user=datauser password=l3tme1n host=penap-data.dyndns.org dbname=didactic port=5432"
	db_conn = settings_file.readline().rstrip('\n')
	# ID values for the parameters and site (DATA, ERROR, SITE)
	# e.g. "408,409,2" == CPCdata,CPCerror,QueenStreet
	params = settings_file.readline().rstrip('\n').split(",")
	# Close the settings file
	settings_file.close()
	print('Setting up Serial Port')
	# Open the serial port and clean the I/O buffer
	ser = serial.Serial(port,9600,parity = serial.PARITY_EVEN,bytesize = serial.SEVENBITS, rtscts=1, stopbits=2)
	ser.flushInput()
	ser.flushOutput()
	# Start the logging
	while (ix<=600):
		# Set the time for the record
		rec_time=time.gmtime()
		# Set the time for the next record
		rec_time_s = int(time.time()) + 60
		timestamp = time.strftime("%Y/%m/%d %H:%M:%S GMT",rec_time)
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
		ix=ix+1
	print('I\'ll restart now')
	ser.close()
