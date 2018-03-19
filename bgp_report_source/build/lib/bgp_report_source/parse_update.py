"""This module parses the archived file and extracts events information from it."""
import time
import sys
#sys.stdout = open('a.out', 'w')
#f = open('a.out', 'a')
from mrtparse import Reader, BGP4MP_ST, MRT_T

def print_bgp4mp(message, ip_stability):
	if message.subtype == BGP4MP_ST['BGP4MP_MESSAGE']\
	or message.subtype == BGP4MP_ST['BGP4MP_MESSAGE_AS4']\
	or message.subtype == BGP4MP_ST['BGP4MP_MESSAGE_LOCAL']\
	or message.subtype == BGP4MP_ST['BGP4MP_MESSAGE_AS4_LOCAL']:
		#print message.__dict__.keys()
		#print_bgp_msg(message.bgp.msg, ip_stability, ipset, message.ts)
		print_bgp_msg(message, ip_stability, message.ts)

#def print_bgp_msg(msg, ip_stability, ipset, ts):
def print_bgp_msg(message, ip_stability, ts):
	msg = message.bgp.msg
	for withdrawn in msg.withdrawn:
		ip_address = str(withdrawn.prefix)+"/"+str(withdrawn.plen)
		if ip_address in ip_stability:
			#ip_stability[ip_address]+= 1
			print "Peer ip - ",message.bgp.peer_ip
			ip_stability[ip_address].append([message.bgp.peer_ip,ts])

def parse(document, sensor_top_talkers, events_top_talkers):

	""" Parse the document and extract events information for top_talkers_sources
	    events = 0 represnts events_bgp_data and events = 1 represents sensor_bgp_data
	"""
	ip_stability = {}
	document = Reader(document)
	for line in sensor_top_talkers:
		ip_address = line[1].replace("x", "0/24")
		ip_stability[ip_address] = []
                #ip_time[ip_address] = []

	for line in events_top_talkers:
                ip_address = line[0].replace("x", "0/24")
                ip_stability[ip_address] = []
                #ip_time[ip_address] = []

	start_time = time.time()
	try:
		for message in document:
			message = message.mrt
			if message.type == MRT_T['BGP4MP']\
			or message.type == MRT_T['BGP4MP_ET']:
				print_bgp4mp(message, ip_stability)
	except IOError as file_read_exception:
		print file_read_exception
		print "skipping the file "
	print "total time to read - ", time.time()-start_time
	print "Ip stability - "
	print ip_stability
	#Separate events and sensor data - 
	sensor_parsed_file = {}
	#sensor_parsed_file_time = {}
	events_parsed_file = {}
	#events_parsed_file_time = {}
	for line in sensor_top_talkers:
		ip_address = line[1].replace("x", "0/24")
		sensor_parsed_file[ip_address] = ip_stability[ip_address]
		#sensor_parsed_file_time[ip_address] = ip_time[ip_address]

	for line in events_top_talkers:
		ip_address = line[0].replace("x", "0/24")
		events_parsed_file[ip_address] = ip_stability[ip_address]
		#events_parsed_file_time[ip_address] = ip_time[ip_address]
	return (sensor_parsed_file, events_parsed_file)
