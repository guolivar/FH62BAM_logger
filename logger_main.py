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
	# Set the minute averaging variable
	min_concentration=0
	n_concentration = 0
	# Set the pre/post SQL statement values
	insert_statement = """INSERT INTO data.fixedmeasurements (parameterid,value,siteid,recordtime) VALUES (%s,%s,%s,timestamptz %s);"""
	insert_statement_file = """INSERT INTO data.fixedmeasurements (parameterid,value,siteid,recordtime) VALUES (%s,'%s',%s,timestamptz '%s');\n"""
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
	# Hacks to work with custom end of line
	eol = b'\r'
	leneol = len(eol)
	bline = bytearray()
	print('Setting up Serial Port')
	# Open the serial port and clean the I/O buffer
	ser = serial.Serial(port,9600,parity = serial.PARITY_EVEN,bytesize = serial.SEVENBITS, rtscts=1, stopbits=2)
	#ser.flushInput()
	#ser.flushOutput()
	print('Writing #')
	ser.write('#\r')
	file_line = ser.readline().rstrip()
	print(file_line)
	# Start the logging
	while (ix<=3600):
		# Set the time for the record
		rec_time_s = int(time.time())
		rec_time=time.gmtime()
		timestamp = time.strftime("%Y/%m/%d %H:%M:%S GMT",rec_time)
		# Change the counters' display to show the time
		#ser.write('WLogging\r')
		#dump_me = ser.readline()

		# Request display concentration from the instrument
		ser.write('C\r')
		## Get data from the instrument
		#while True:
			#c = ser.read(1)
			#bline += c
			#if bline[-leneol:] == eol:
				#break
		# Parse the data line
		#file_line = bline.decode("utf-8")[:-leneol]
		#concentration = eval(bline.decode("utf-8")[:-leneol])
		file_line = ser.readline().rstrip()
		print(file_line)
		#concentration = eval(file_line)
		# Make the line pretty for the file
		file_line = timestamp+','+file_line
		# Save it to the appropriate file
		current_file_name = datapath+time.strftime("%Y%m%d.txt",rec_time)
		current_file = open(current_file_name,"a")
		current_file.write(file_line+"\n")
		current_file.flush()
		current_file.close()
		file_line = ""
		bline = bytearray()
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
		# Wait until the next second
		while int(time.time())<=rec_time_s:
			#wait a few miliseconds
			time.sleep(0.05)
		ix=ix+1
	print('I\'ll restart now')
	ser.close()
