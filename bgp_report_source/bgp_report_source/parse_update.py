"""This module parses the archived file and extracts events information from it."""
import time
import sys
from mrtparse import Reader, BGP4MP_ST, MRT_T

def print_bgp4mp(message, ip_stability_a, ip_stability_w):
	if message.subtype == BGP4MP_ST['BGP4MP_MESSAGE']\
	or message.subtype == BGP4MP_ST['BGP4MP_MESSAGE_AS4']\
	or message.subtype == BGP4MP_ST['BGP4MP_MESSAGE_LOCAL']\
	or message.subtype == BGP4MP_ST['BGP4MP_MESSAGE_AS4_LOCAL']:
		print_bgp_msg(message, ip_stability_a, ip_stability_w, message.ts)

def print_bgp_msg(message, ip_stability_a, ip_stability_w, ts):
	msg = message.bgp.msg

	for announcement in msg.nlri:
		ip_address = str(announcement.prefix)+"/"+str(announcement.plen)
		if ip_address in ip_stability_a:
			ip_stability_a[ip_address].append(["A", message.bgp.peer_ip, announcement.plen, ts])
	for withdrawn in msg.withdrawn:
		ip_address = str(withdrawn.prefix)+"/"+str(withdrawn.plen)
		if ip_address in ip_stability_w:
			ip_stability_w[ip_address].append(["W", message.bgp.peer_ip, withdrawn.plen, ts])

def parse(bzfile, sensor_top_talkers, events_top_talkers):

	""" Parse the document and extract events information for top_talkers_sources
	    events = 0 represnts events_bgp_data and events = 1 represents sensor_bgp_data
	"""
	ip_stability_w = {}
	ip_stability_a = {}
	errors = 0

	document = Reader(bzfile)
	#print "Read mrt document: {}".format(document)

	for line in sensor_top_talkers:
		ip_address = line[1].replace("x", "0/24")
		ip_stability_w[ip_address] = []
		ip_stability_a[ip_address] = []

	for line in events_top_talkers:
		ip_address = line[0].replace("x", "0/24")
		ip_stability_w[ip_address] = []
		ip_stability_a[ip_address] = []

	try:
		for message in document:
			#print "Got message from document: {}".format(message)
			mrt = message.mrt
			#print "Got the MRT message: {}".format(mrt)
			if mrt.type == MRT_T['BGP4MP'] or mrt.type == MRT_T['BGP4MP_ET']:
				print_bgp4mp(mrt, ip_stability_a, ip_stability_w)

	except IOError as file_read_exception:
		errors = file_read_exception

	#Separate events and sensor data - 
	sensor_parsed_file_a = {}
	sensor_parsed_file_w = {}
	events_parsed_file_a = {}
	events_parsed_file_w = {}
	#events_parsed_file_time = {}
	for line in sensor_top_talkers:
		ip_address = line[1].replace("x", "0/24")
		sensor_parsed_file_a[ip_address] = ip_stability_a[ip_address]
		sensor_parsed_file_w[ip_address] = ip_stability_w[ip_address]

	for line in events_top_talkers:
		ip_address = line[0].replace("x", "0/24")
		events_parsed_file_a[ip_address] = ip_stability_a[ip_address]
		events_parsed_file_w[ip_address] = ip_stability_w[ip_address]
	
	return (sensor_parsed_file_a, sensor_parsed_file_w, events_parsed_file_a, events_parsed_file_w, errors)
