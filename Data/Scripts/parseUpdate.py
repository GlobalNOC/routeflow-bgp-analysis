from optparse import OptionParser
from datetime import *
from mrtparse import *
import commands
import ast
import time
myASnumber = 0
indt = 0
ipFile = open("ipFile","r")
target2 = open("FinalStability","a")
ipset = set()


ipStability = {}

def prline(line):
	global indt
	print('    ' * indt + line)

def print_bgp4mp(m):
	global indt
	indt = 0
	indt += 1
	if ( m.subtype == BGP4MP_ST['BGP4MP_MESSAGE'] or m.subtype == BGP4MP_ST['BGP4MP_MESSAGE_AS4'] or m.subtype == BGP4MP_ST['BGP4MP_MESSAGE_LOCAL'] or m.subtype == BGP4MP_ST['BGP4MP_MESSAGE_AS4_LOCAL']):
		print_bgp_msg(m.bgp.msg, m.subtype, m)

def print_bgp_msg(msg, subtype, m):
	global indt
	global ipset
	global ipStability
	global ip_to_asn
	indt = 0
	for withdrawn in msg.withdrawn:
		ip = str(withdrawn.prefix)+"/"+str(withdrawn.plen)
		if ip in ipset:
			ipStability[ip] = ipStability[ip]+1
            

def main():
	global ipset
	global ipStability
	global ipFile
	d = Reader(sys.argv[1])
	for line in ipFile:
		lineDetail = line.split(",")
		ip = lineDetail[0].rstrip(" ").replace("x","0/24")
		print "IP is --  ",ip
		ipStability[ip]=0
		ipset.add(ip)
	print "ipset - ",ipset
	st = time.time()
	for m in d:
		m = m.mrt
		if ( m.type == MRT_T['BGP4MP'] or m.type == MRT_T['BGP4MP_ET']):
			print_bgp4mp(m)
	print "total time to read - ",time.time()-st
	print "ipStability --- "
	print ipStability
	target2.write(str(ipStability)+",")
	target2.close()

if __name__ == '__main__':
	print "main entered"
	main()
