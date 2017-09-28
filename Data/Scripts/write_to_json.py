import commands
import sys
import json
import ast
import csv
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


open("Analysis.json",'a').close() # to clear contents of the file  
file_to_write_sent = open("Analysis.json","a")

list_file = []
topTalkerFile = open("ipFile","r")
for line2 in topTalkerFile:
        listToWrite = {"Date":"","Prefix":"","DataSentInbits":"","Events":"","Organization":""}
        
        listToWrite["Date"] = Date
        w = line2.rstrip('\n').split(",")
        listToWrite["Prefix"] = w[0]
        listToWrite["DataSentInbits"] = int(w[1])
        ip = w[0]
        ip = ip[:ip.find("x")]+"0/24"
        listToWrite["Events"] = flapsDict[ip]
        cmd = "whois -h whois.radb.net "+ip+" | grep descr:"
        descr = commands.getoutput(cmd).split("\n")[0].split(":")[1].strip(" ")
        asn = commands.getoutput("whois -h whois.radb.net "+ip+" | grep origin:").split("\n")[0].split(":")[1].strip(" ")
        asn_number = asn[asn.find("AS")+2:]
        listToWrite["Organization"] = descr+"  |ASN - "+asn_number
        print listToWrite
        list_file.append(listToWrite)
file_to_write_sent.seek(0)
file_to_write_sent.write(json.dumps(list_file))
file_to_write_sent.close()
