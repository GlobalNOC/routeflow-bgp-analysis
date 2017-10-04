import ast
import csv
import commands
import sys
import json
Date = (sys.argv[1])[0:10]
asn_vs_flaps = open("FinalStability","r")
flaps = asn_vs_flaps.read();
flapsDict = {}
tempListOfDict = ast.literal_eval("["+flaps[:-1]+"]")

for val in tempListOfDict:
	for key,value in val.iteritems():
		if key in flapsDict.keys():
			flapsDict[key] = flapsDict[key] + value
		else:
			flapsDict[key] = value
print "flapdict ---- "
print flapsDict
# Write data sent to file - 
csvFile = open("Analysis.csv","a")
file_to_write_sent = csv.writer(csvFile, delimiter=',')
#heading = ["ASN Num", "Prefix", "DataTransferred", "Flaps", "TotalPrefixInAS"]
#file_to_write_sent.writerow(heading)

topTalkerFile = open("ipFile","r")
for line2 in topTalkerFile:
	listToWrite = []
	listToWrite.append(Date)
	for w in line2.split(","):
		listToWrite.append(w.strip())
	ip = listToWrite[1]
	ip = ip[:ip.find("x")]+"0/24"
	listToWrite.append(flapsDict[ip])
	cmd = "whois -h whois.radb.net "+ip+" | grep descr:"
	try: #When no records were found for particular IP, ignore them
		descr = commands.getoutput(cmd).split("\n")[0].split(":")[1].strip(" ")
		asn = commands.getoutput("whois -h whois.radb.net "+ip+" | grep origin:").split("\n")[0].split(":")[1].strip(" ")
		asn_number = asn[asn.find("AS")+2:]
		print asn_number
		listToWrite.append(descr+"  |ASN - "+asn_number)
	except:
		listToWrite.append("NOT FOUND IN RADb")
	print listToWrite
	file_to_write_sent.writerow(listToWrite)
file_to_write_sent.writerow([])
csvFile.close()

