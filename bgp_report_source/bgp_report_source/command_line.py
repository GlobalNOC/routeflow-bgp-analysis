import bgp_report_source
import time
import sys
def main():
	s = time.time()
	#No arguments specified, take the default datetime
	if len(sys.argv) == 1:
		bgp_report_source.main()
	#Two arguments specified, pass the two datetimes to main
	elif len(sys.argv) == 3:
		bgp_report_source.main(sys.argv[1], sys.argv[2])
	#Three arguments specified, third is the location of config file - 
	elif len(sys.argv) == 4:
		bgp_report_source.main(sys.argv[1], sys.argv[2], sys.argv[3])
	else:
		print "Incorrect number of arguments"
	print "Total time compute ",time.time()-s
