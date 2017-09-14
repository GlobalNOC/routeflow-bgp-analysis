import ast
import csv
import commands
import sys
import json
Date = (sys.argv[1])[0:10]
asn_vs_flaps = open("FinalStability","r")
flaps = asn_vs_flaps.read()
flapsDict = {}
tempListOfDict = ast.literal_eval("["+flaps[:-1]+"]")

for val in tempListOfDict:
	for key,value in val.iteritems():
    		if key in flapsDict.keys():
      			flapsDict[key] = flapsDict[key] + value
    		else:
      			flapsDict[key] = value

# Write data sent to file -
print "Sent Data - "


open("Analysis_sent.json",'w').close() # to clear contents of the file  
file_to_write_sent = open("Analysis_sent.json","r+")

list_file = []
topTalkerFile = open("asnInfoFile_sent","r")
for line2 in topTalkerFile:
	listToWrite = {"Date":"","ASN":"","Prefix":"","DataSentInbits":"","Events":"","Organization":"","PrefixesOwnedByAS":""}
	
	listToWrite["Date"] = Date
	w = line2.rstrip('\n').split(" ")
	listToWrite["ASN"] = w[0]
	listToWrite["Prefix"] = w[1]
	listToWrite["DataSentInbits"] = int(w[2])
	listToWrite["Events"] = flapsDict[listToWrite["ASN"]]
  	cmd = "whois -h whois.radb.net -- '-i origin AS"+listToWrite["ASN"]+ "' | grep route:"
  	asn_totalPrefixes = len(commands.getoutput(cmd).split("\n"))
  	descr = commands.getoutput("whois -h whois.radb.net -- '-i origin AS"+listToWrite["ASN"]+ "' | grep descr:").split("\n")[0]+" | ASN : "+str(listToWrite["ASN"])
	if descr.find("descr")>=0:
		descr = descr[descr.find("descr")+6:]
	listToWrite["Organization"] = descr.lstrip(" ").rstrip(" ")
	listToWrite["PrefixesOwnedByAS"] = asn_totalPrefixes
  	print listToWrite
  	list_file.append(listToWrite)
file_to_write_sent.seek(0)
file_to_write_sent.write(json.dumps(list_file))
file_to_write_sent.close()

print "----"*12
print "received Data - "
# Write data received to file -


open("Analysis_recv.json",'w').close() # to clear contents of the file
file_to_write_recv = open("Analysis_recv.json","r+")

list_file = []
topTalkerFile = open("asnInfoFile_recv","r")
for line2 in topTalkerFile:
        listToWrite = {"Date":"","ASN":"","Prefix":"","DataReceivedInbits":"","Events":"","Organization":"","PrefixesOwnedByAS":""}

        listToWrite["Date"] = Date
        w = line2.rstrip('\n').split(" ")
        listToWrite["ASN"] = w[0]
        listToWrite["Prefix"] = w[1]
        listToWrite["DataReceivedInbits"] = int(w[2])
        listToWrite["Events"] = flapsDict[listToWrite["ASN"]]
        cmd = "whois -h whois.radb.net -- '-i origin AS"+listToWrite["ASN"]+ "' | grep route:"
        asn_totalPrefixes = len(commands.getoutput(cmd).split("\n"))
        descr = commands.getoutput("whois -h whois.radb.net -- '-i origin AS"+listToWrite["ASN"]+ "' | grep descr:").split("\n")[0]+" | ASN : "+str(listToWrite["ASN"])
        if descr.find("descr")>=0:
                descr = descr[descr.find("descr")+6:]
        listToWrite["Organization"] = descr.lstrip(" ").rstrip(" ")
        listToWrite["PrefixesOwnedByAS"] = asn_totalPrefixes
        print listToWrite
        list_file.append(listToWrite)
file_to_write_recv.seek(0)
file_to_write_recv.write(json.dumps(list_file))
file_to_write_recv.close()


print "----"*12
print "Total Data - "
# Write data received to file -


open("Analysis_total.json",'w').close() # to clear contents of the file
file_to_write_total = open("Analysis_total.json","r+")

list_file = []
topTalkerFile = open("asnInfoFile_total","r")
for line2 in topTalkerFile:
        listToWrite = {"Date":"","ASN":"","Prefix":"","TotalDataInbits":"","Events":"","Organization":"","PrefixesOwnedByAS":""}

        listToWrite["Date"] = Date
        w = line2.rstrip('\n').split(" ")
        listToWrite["ASN"] = w[0]
        listToWrite["Prefix"] = w[1]
        listToWrite["TotalDataInbits"] = int(w[2])
        listToWrite["Events"] = flapsDict[listToWrite["ASN"]]
        cmd = "whois -h whois.radb.net -- '-i origin AS"+listToWrite["ASN"]+ "' | grep route:"
        asn_totalPrefixes = len(commands.getoutput(cmd).split("\n"))
        descr = commands.getoutput("whois -h whois.radb.net -- '-i origin AS"+listToWrite["ASN"]+ "' | grep descr:").split("\n")[0]+" | ASN : "+str(listToWrite["ASN"])
        if descr.find("descr")>=0:
                descr = descr[descr.find("descr")+6:]
        listToWrite["Organization"] = descr.lstrip(" ").rstrip(" ")
        listToWrite["PrefixesOwnedByAS"] = asn_totalPrefixes
        print listToWrite
        list_file.append(listToWrite)
file_to_write_total.seek(0)
file_to_write_total.write(json.dumps(list_file))
file_to_write_total.close()

