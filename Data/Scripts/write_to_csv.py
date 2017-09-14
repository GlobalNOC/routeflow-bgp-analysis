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


print "Sent Data"
# Write data sent to file - 
     
csvFile = open("Analysis_sent.csv","a")
file_to_write_sent = csv.writer(csvFile, delimiter=',')
#heading = ["ASN Num", "Prefix", "DataTransferred", "Flaps", "TotalPrefixInAS"]
#file_to_write_sent.writerow(heading)

topTalkerFile = open("asnInfoFile_sent","r")
for line2 in topTalkerFile:
	listToWrite = []
	listToWrite.append(Date)
	for w in line2.split(" "):
		listToWrite.append(w.strip())
	listToWrite.append(flapsDict[listToWrite[1]])
	cmd = "whois -h whois.radb.net -- '-i origin AS"+listToWrite[1]+ "' | grep route:"
	asn_totalPrefixes = len(commands.getoutput(cmd).split("\n"))
	descr = commands.getoutput("whois -h whois.radb.net -- '-i origin AS"+listToWrite[1]+ "' | grep descr:").split("\n")[0]+" | ASN : "+str(listToWrite[1])
	listToWrite.append(descr)
	listToWrite.append(asn_totalPrefixes)
	print listToWrite
	file_to_write_sent.writerow(listToWrite)
file_to_write_sent.writerow([])


print "------" *10
print "Received Data - "
# Write data received to file - 

csvFile = open("Analysis_recv.csv","a")
file_to_write_recv = csv.writer(csvFile, delimiter=',')

topTalkerFile = open("asnInfoFile_recv","r")
for line2 in topTalkerFile:
        listToWrite = []
        listToWrite.append(Date)
        for w in line2.split(" "):
                listToWrite.append(w.strip())
        listToWrite.append(flapsDict[listToWrite[1]])
        cmd = "whois -h whois.radb.net -- '-i origin AS"+listToWrite[1]+ "' | grep route:"
        asn_totalPrefixes = len(commands.getoutput(cmd).split("\n"))
        descr = commands.getoutput("whois -h whois.radb.net -- '-i origin AS"+listToWrite[1]+ "' | grep descr:").split("\n")[0]+" | ASN : "+str(listToWrite[1])
        listToWrite.append(descr)
        listToWrite.append(asn_totalPrefixes)
        print listToWrite
        file_to_write_recv.writerow(listToWrite)
file_to_write_recv.writerow([])



print "------"*10
print "Total data --- "
#Write total data to file - 


csvFile = open("Analysis_total.csv","a")
file_to_write_total = csv.writer(csvFile, delimiter=',')

topTalkerFile = open("asnInfoFile_total","r")
for line2 in topTalkerFile:
        listToWrite = []
        listToWrite.append(Date)
        for w in line2.split(" "):
                listToWrite.append(w.strip())
        listToWrite.append(flapsDict[listToWrite[1]])
        cmd = "whois -h whois.radb.net -- '-i origin AS"+listToWrite[1]+ "' | grep route:"
        asn_totalPrefixes = len(commands.getoutput(cmd).split("\n"))
        descr = commands.getoutput("whois -h whois.radb.net -- '-i origin AS"+listToWrite[1]+ "' | grep descr:").split("\n")[0]+" | ASN : "+str(listToWrite[1])
        listToWrite.append(descr)
        listToWrite.append(asn_totalPrefixes)
        print listToWrite
        file_to_write_total.writerow(listToWrite)
file_to_write_total.writerow([])

