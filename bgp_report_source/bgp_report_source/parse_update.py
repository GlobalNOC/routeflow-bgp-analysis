"""This module parses the archived file and extracts events information from it."""
import time
import sys
import copy
from mrtparse import Reader, BGP4MP_ST, MRT_T

# Reformats IPs ending in ".x" as /24 CIDR netblocks
def get_cidr(ip):
	return ip.replace('x', '0/24')

# Checks mrt message types and subtypes for what we want
def check_types(message):
	# Check whether the type is wrong
	if message.type != MRT_T['BGP4MP'] and message.type != MRT_T['BGP4MP_ET']:
		return False

	# Check whether the subtype is correct
	if message.subtype == BGP4MP_ST['BGP4MP_MESSAGE']\
	or message.subtype == BGP4MP_ST['BGP4MP_MESSAGE_AS4']\
	or message.subtype == BGP4MP_ST['BGP4MP_MESSAGE_LOCAL']\
	or message.subtype == BGP4MP_ST['BGP4MP_MESSAGE_AS4_LOCAL']:
		return True
	else:
		return False

# Parses the BGP information from the top talkers data
def parse(bzfile, sensor_top_talkers, events_top_talkers):

	# Set default errors
	errors = ''

	# Read the bz2 document with mrtparse
	document = Reader(bzfile)

	# Initialize IP dictionaries for announcement and withdrawn sensor data
	sensor_a = {get_cidr(entry[1]): [] for entry in sensor_top_talkers}
	sensor_w = copy.deepcopy(sensor_a)
	
	# Initialize IP dictionaries for announcement and withdrawn events data
	events_a = {get_cidr(entry[0]): [] for entry in events_top_talkers}
	events_w = copy.deepcopy(events_a)

	try:
		for message in document:

			mrt = message.mrt
			
			# Check whether the types and subtypes are correct
			if check_types(mrt):

				# Check announcements
				for announcement in mrt.bgp.msg.nlri:

					# Get the IP from the prefix and prefix length
					ip = str(announcement.prefix)+'/'+str(announcement.plen)
					
					# Check if the IP is in the sensor top talkers
					if ip in sensor_a:
						sensor_a[ip].append(['A', mrt.bgp.peer_ip, announcement.plen, mrt.ts])
					
					# Check if the IP is in the events top talkers
					if ip in events_a:
						events_a[ip].append(['A', mrt.bgp.peer_ip, announcement.plen, mrt.ts])

				# Check withdrawn
				for withdrawn in mrt.bgp.msg.withdrawn:

					# Get the IP from the prefix and prefix length
					ip = str(withdrawn.prefix)+"/"+str(withdrawn.plen)
					
					# Check if the IP is in the sensor top talkers
					if ip in sensor_w:
						sensor_w[ip].append(['W', mrt.bgp.peer_ip, withdrawn.plen, mrt.ts])

					if ip in events_w:
						events_w[ip].append(['W', mrt.bgp.peer_ip, withdrawn.plen, mrt.ts])

	except Exception, e:
		errors = e

	return ((sensor_a, sensor_w, events_a, events_w), errors)
