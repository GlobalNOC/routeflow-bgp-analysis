from optparse import OptionParser
from datetime import *
from mrtparse import *
import commands
import ast
import time
import sys

def prline(line):
	print('    ' * self.indt + line)

def print_bgp4mp(m):
	global indt
	indt = 0
	indt += 1
        if ( m.subtype == BGP4MP_ST['BGP4MP_MESSAGE'] or m.subtype == BGP4MP_ST['BGP4MP_MESSAGE_AS4'] or m.subtype == BGP4MP_ST['BGP4MP_MESSAGE_LOCAL'] or m.subtype == BGP4MP_ST['BGP4MP_MESSAGE_AS4_LOCAL']):
		print_bgp_msg(m.bgp.msg, m.subtype, m)

def print_bgp_msg(msg,subtype, m):
	global indt
	global ipset
	global ipStability
	indt = 0
	for withdrawn in msg.withdrawn:
		ip = str(withdrawn.prefix)+"/"+str(withdrawn.plen)
		if ip in ipset:
			ipStability[ip] = ipStability[ip]+1
            

def parse(d,topTalker_sources):
	global ipset
	ipset = set()
	global ipStability
	ipStability = {}
	global ipFile
	print "Type of d before Reader is - ",type(d)
	d = Reader(d)
	for line in topTalker_sources:
                ip = line[0].replace("x","0/24")
                print "IP is --  ",ip
                ipStability[ip]=0
                ipset.add(ip)
   	print "ipset - ",ipset
        st = time.time()
        print "Type of d is - ",type(d)
        for m in d:
        	#print "Reading m for thread - ",threadCounter
                m = m.mrt
                if (m.type == MRT_T['BGP4MP'] or m.type == MRT_T['BGP4MP_ET']):
                	print_bgp4mp(m)
    	print "total time to read - ",time.time()-st
        print "ipStability --- "        
        print ipStability
        return ipStability
