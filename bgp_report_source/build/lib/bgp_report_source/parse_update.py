"""This module parses the archived file and extracts events information from it."""
import time
from mrtparse import Reader, BGP4MP_ST, MRT_T

def print_bgp4mp(message, ip_stability, ipset):
	if message.subtype == BGP4MP_ST['BGP4MP_MESSAGE']\
	or message.subtype == BGP4MP_ST['BGP4MP_MESSAGE_AS4']\
	or message.subtype == BGP4MP_ST['BGP4MP_MESSAGE_LOCAL']\
	or message.subtype == BGP4MP_ST['BGP4MP_MESSAGE_AS4_LOCAL']:
		print_bgp_msg(message.bgp.msg, ip_stability, ipset)

def print_bgp_msg(msg, ip_stability, ipset):
	for withdrawn in msg.withdrawn:
		ip_address = str(withdrawn.prefix)+"/"+str(withdrawn.plen)
		if ip_address in ipset:
			ip_stability[ip_address] = ip_stability[ip_address]+1

def parse(document, sensor_top_talkers, events_top_talkers):

	""" Parse the document and extract events information for top_talkers_sources
	    events = 0 represnts events_bgp_data and events = 1 represents sensor_bgp_data
	"""
	ipset = set()
	ip_stability = {}
	document = Reader(document)
	for line in sensor_top_talkers:
		ip_address = line[1].replace("x", "0/24")
		ip_stability[ip_address] = 0
                ipset.add(ip_address)

	for line in events_top_talkers:
                ip_address = line[0].replace("x", "0/24")
                ip_stability[ip_address] = 0
                ipset.add(ip_address)

	start_time = time.time()
	try:
		for message in document:
			message = message.mrt
			if message.type == MRT_T['BGP4MP']\
			or message.type == MRT_T['BGP4MP_ET']:
				print_bgp4mp(message, ip_stability, ipset)
	except IOError as file_read_exception:
		print file_read_exception
		print "skipping the file "
	print "total time to read - ", time.time()-start_time
	#Separate events and sensor data - 
	sensor_parsed_file = {}
	events_parsed_file = {}
	for line in sensor_top_talkers:
		ip_address = line[1].replace("x", "0/24")
		sensor_parsed_file[ip_address] = ip_stability[ip_address]

	for line in events_top_talkers:
		ip_address = line[0].replace("x", "0/24")
		events_parsed_file[ip_address] = ip_stability[ip_address]

	return (sensor_parsed_file, events_parsed_file)
