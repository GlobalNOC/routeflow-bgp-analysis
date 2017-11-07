from optparse import OptionParser
from datetime import *
from mrtparse import *
import ast
import time
import sys

def prline(line):
	print('    ' * self.indt + line)

def print_bgp4mp(m,ipStability,ipset):
        if ( m.subtype == BGP4MP_ST['BGP4MP_MESSAGE'] or m.subtype == BGP4MP_ST['BGP4MP_MESSAGE_AS4'] or m.subtype == BGP4MP_ST['BGP4MP_MESSAGE_LOCAL'] or m.subtype == BGP4MP_ST['BGP4MP_MESSAGE_AS4_LOCAL']):
		print_bgp_msg(m.bgp.msg, m.subtype, m,ipStability,ipset)

def print_bgp_msg(msg,subtype, m,ipStability,ipset):
	for withdrawn in msg.withdrawn:
		ip = str(withdrawn.prefix)+"/"+str(withdrawn.plen)
		if ip in ipset:
			ipStability[ip] = ipStability[ip]+1
            

def parse(d,topTalker_sources):
	ipset = set()
	ipStability = {}
	d = Reader(d)
	for line in topTalker_sources:
                ip = line[0].replace("x","0/24")
                ipStability[ip]=0
                ipset.add(ip)
        st = time.time()
	try:
        	for m in d:
                	m = m.mrt
                	if (m.type == MRT_T['BGP4MP'] or m.type == MRT_T['BGP4MP_ET']):
                		print_bgp4mp(m,ipStability,ipset)
	except IOError as IOE:
		print IOE
		print "skipping the file "
    	print "total time to read - ",time.time()-st
        return ipStability
