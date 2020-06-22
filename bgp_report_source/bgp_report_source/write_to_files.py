import csv
import commands
import json
import time
import copy
from elasticsearch import Elasticsearch, helpers

# Writes data to a CSV file or returns an error
def write_to_csv(flaps_dict, top_talker_sources, file_path, sensor_name_map, start_time):
    
	print 'Writing data to CSV...',
	err_msg = '\t[FAILED] (Reason: {})'

	# Set the date for the rows of data being written
	date = start_time[0:10]

	# Open the CSV file for writing or return error
	try:
		csv_file = open(file_path+"Analysis.csv", "a")
		writer = csv.writer(csv_file, delimiter=',')
	except IOError, e:
		print err_msg.format(e)
		return ('write_to_csv', e)

    
	# Create a row of data for each source
	for source in top_talker_sources:

		# Get the sensor name, IP address, and CIDR /24 netblock for the source
		sensor   = source[0]
		ip       = source[1]
		netblock = ip[:ip.find('x')] + '0/24'
		bits     = source[2]
		flaps    = len(flaps_dict[netblock])

		# Correct the sensor name if it's in the map
		if sensor in sensor_name_map:
			sensor = sensor_name_map[sensor]
		
		# Get the WHOIS description for the netblock
		try:
			cmd = 'whois -h whois.radb.net {} | grep descr:'.format(netblock)
			description = commands.getoutput(cmd).split("\n")[0].split(":")[1].strip(" ")
		except IndexError as e:
			# Set default description when no description could be indexed
			description = 'NOT FOUND IN RADb'

		# Set the row array to write for the source
		row = [date, sensor, ip, bits, flaps, description]

		# Write the row to the CSV file or return error
		try:
			writer.writerow(row)
		except IOError, e:
			print err_msg.format(e)
			return ('write_to_csv', e)

	# Write an empty row to the end for formatting and close the CSV file
	try:
		writer.writerow([])
		csv_file.close()
	except IOError, e:
		print err_msg.format(e)
		return ('write_to_csv', e)

	print '\t\t[COMPLETE]'
	return(1, 0)


# Writes data to a JSON file or returns an error
def write_to_json(flaps_dict, top_talker_sources, file_path, sensor_name_map, start_time, event_type):
	
	print 'Writing data to JSON...',
	err_msg = '\t[FAILED] (Reason: {})'

	# Set the date for the rows of data being written
	date = start_time[0:10]

	# Open the JSON file for writing
	try:
		# Clear the file contents
		open(file_path+"Analysis.json", 'w').close()
		json_file = open(file_path+"Analysis.json", "w")
	except IOError, e:
		print err_msg.format(e)
		return ('write_to_json', e)

	# An array to store all output to be written
	output = []

	for source in top_talker_sources:

		# Get the sensor name, IP address, CIDR /24 netblock, flap count, and bits for the source
		sensor   = source[0]
		ip       = source[1]
		netblock = ip[:ip.find('x')] + '0/24'
		bits	 = int(source[2])
		flaps    = len(flaps_dict[netblock])

		# Correct the sensor name if it's in the map
		if sensor in sensor_name_map:
			sensor = sensor_name_map[sensor]

		# We only add data for sensors that have flaps
		if flaps > 0:

			# Get the WHOIS description for the netblock
			try:
				cmd = "whois -h whois.radb.net {} | grep descr:".format(netblock)
				description = commands.getoutput(cmd).split("\n")[0].split(":")[1].strip(" ")
			except IndexError as e:
				description = 'NOT FOUND IN RADb'

			# Create an object for the sensor data
			sensor_data = {\
				'Date':           date,\
				'Event_Type':     event_type,\
				'Sensor':         sensor,\
				'Prefix':         ip,\
				'DataSentInbits': bits,\
				'Events':         flaps,\
				'Events_Time':    flaps_dict[netblock],\
				'Organization':   description\
			}

			# Add the sensor data to the output
			output.append(sensor_data)
	
	# Write the output to the JSON file
	try:
		json_file.seek(0)
		json_file.write(json.dumps(output))
	except IOError, e:
		print err_msg.format(e)
		return ('write_to_json', e)

	# Return the output array
	print '\t[COMPLETE]'
	return (output, 0)


def write_to_json_events(flaps_dict, top_talker_sources, file_path, start_time, event_type):

	print 'Writing data to JSON...',
	err_msg = '\t[FAILED] (Reason: {})'

	# Set the date for the rows of data being written
	date = start_time[0:10]

	# Open the JSON file for writing
	try:
		# Clear the file contents
		open(file_path+"events_Analysis.json", 'w').close() # to clear contents of the file
		json_file = open(file_path+"events_Analysis.json", "w")
	except IOError, e:
		print err_msg.format(e)
		return ('write_to_json_events', e)

	# An array to store all output to be written
	output = []
	
	for source in top_talker_sources:

		# Get the IP address, CIDR /24 netblock, flap count, and bits for the source
		ip = source[0]
		netblock = ip[:ip.find("x")]+"0/24"
		flaps = len(flaps_dict[netblock])
		bits = int(source[1])

		# We only add data for events that have flaps
		if flaps > 0:

			# Get the WHOIS description for the netblock
			try:
				cmd = "whois -h whois.radb.net {} | grep descr:".format(netblock)
				description = commands.getoutput(cmd).split("\n")[0].split(":")[1].strip(" ")
			except IndexError as e:
				description = "NOT FOUND IN RADb"

			# Create an object for the sensor data
			event_data = {
				"Event_Type":     event_type,
				"Date":           date,
				"Prefix":         ip,
				"DataSentInbits": bits,
				"Events":         flaps,
				"Events_Time":    flaps_dict[netblock],
				"Organization":   description
			}

			# Add the sensor data to the output
			output.append(event_data)

	# Write the output to the JSON file
	try:
		json_file.seek(0)
		json_file.write(json.dumps(output))
	except IOError, e:
		print err_msg.format(e)
		return ('write_to_json_events', e)

	# Return the output array
	print '\t[COMPLETE]'
	return (output, 0)


# Writes data to ES database and returns (status, error)
def write_to_db(START_TIME, json_dump, es_instance, bgp_index, document):

	print 'Writing data to ES...',
	err_msg = '\t\t[FAILED] (Reason: {})'

	# Query for ES to get data for the time range
	query = {'query': {'term': {'Date':START_TIME[0:10]}}}

	# Get the ES object and number of data entries in ES for the time period
	try:
		es_object = Elasticsearch([es_instance])
		hits = es_object.search(index=bgp_index, body=query, scroll='1m')["hits"]["total"]
	except Exception, e:
		print err_msg.format(e)
		return ('write_to_db', e)

	# Check if the time period has any data entries
	if hits == 0:

		data = [{"_index":bgp_index, "_type":document, "_source":entry} for entry in json_dump]
		
		# Remove "Events_Time" from data entries
		for d in data:
			if "Events_Time" in d["_source"]:
				del d["_source"]["Events_Time"]

		helpers.bulk(es_object, data)

		# Output the result and return
		print '\t\t[COMPLETE]'
		return (1, 0)

	else:
		# Output the result and return
		print '\t\t[COMPLETE] (Data already exists)'
		return (1, 0)


def write_to_db_drill_down(START_TIME, json_dump, es_instance, bgp_index, document):

	print 'Writing drill-down to ES...',
	err_msg = '[FAILED] (Reason: {})'

	# Query for ES to get data for the time range
	query = { "query": { "term": {"Date":START_TIME[0:10]}}}
	
	# Get the ES object and number of data entries in ES for the time period
	try:
		es_object = Elasticsearch([es_instance])
		hits = es_object.search(index=bgp_index, body=query, scroll='1m')["hits"]["total"]
	except Exception, e:
		print err_msg.format(e)
		return ('write_to_db_drilldown', e)

	# Check if the time period has any data entries
	if hits == 0:

		output = []

		for entry in json_dump:

			for event in entry["Events_Time"]:
					
				# Get the peer name from WHOIS description
				try:
					cmd = 'whois -h whois.radb.net {} | grep descr:'.format(event[1])
					peer_name = commands.getoutput(cmd).split("\n")[0].split(":")[1].strip(" ")
				except IndexError as e:
					peer_name = "NOT FOUND IN RADb"
					
				# Set the data for the event
				event_data = {\
					'Prefix':        entry['Prefix'],\
					'Organization':  entry['Organization'],\
					'Date':          entry['Date'],\
					'Sensor':        entry['Sensor'],\
					'Peer_id':       event[1],\
					'Peer_name':     peer_name,\
					'Prefix_length': event[2],\
					'Event_Type':    entry['Event_Type'],\
					'timestamp':     time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(event[3]))\
				}

				# Add the event data to the output
				output.append({"_index":bgp_index, "_type":document, "_source":event_data})
		
		# Add output to ES
		try:
			helpers.bulk(es_object, output)
		except Exception, e:
			print err_msg.format(e)
			return ('write_to_db_drill_down', e)

	# Output the result and return
		print '\t[COMPLETE]'
	else:
		print "\t[COMPLETE] (Data already exists)"

	return (1, 0)

