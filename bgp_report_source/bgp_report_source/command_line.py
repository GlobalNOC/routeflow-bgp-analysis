import bgp_report_source
import time
import sys
import os
def main():
	s = time.time()
	pwd = os.getcwd()
	#No arguments specified, take the default datetime
	if len(sys.argv) == 1:
		bgp_report_source.main(pwd+"/")
	#If only one argimet is passed, passed the config file location - 
	elif len(sys.argv) == 2:
		bgp_report_source.main(sys.argv[1])
	#Two arguments specified, pass the two datetimes to main
	elif len(sys.argv) == 3:
		bgp_report_source.main(pwd+"/", sys.argv[1], sys.argv[2])
	#Three arguments specified, passed config file location and datetime in order
	elif len(sys.argv) == 4:
		bgp_report_source.main(sys.argv[1], sys.argv[2], sys.argv[3])
	else:
		print "ERROR: Incorrect number of arguments"

	print "\nBGP Report finished in {} seconds\n".format(round(time.time()-s, 2))

if __name__ == "__main__":
	main()
