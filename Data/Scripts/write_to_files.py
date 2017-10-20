import ast
import csv
import commands
import sys
import json

def write_to_csv(flapsDict, topTalker_sources, START_TIME):
	csvFile = open("Analysis.csv","a")
        file_to_write = csv.writer(csvFile, delimiter=',')
	Date = START_TIME[0:10]
	for line2 in topTalker_sources:
                listToWrite = []
                listToWrite.append(Date)
                listToWrite.append(line2[0])
                listToWrite.append(line2[1])
                ip = listToWrite[1]
                ip = ip[:ip.find("x")]+"0/24"
                listToWrite.append(flapsDict[ip])
                cmd = "whois -h whois.radb.net "+ip+" | grep descr:"
                print "ip is -- ",ip
                try: #When no records were found for particular IP, ignore them
                        descr = commands.getoutput(cmd).split("\n")[0].split(":")[1].strip(" ")
                        asn = commands.getoutput("whois -h whois.radb.net "+ip+" | grep origin:").split("\n")[0].split(":")[1].strip(" ")
                        asn_number = asn[asn.find("AS")+2:]
                        print asn_number
                        listToWrite.append(descr+"  |ASN - "+asn_number)
                except Exception as e:
			print ("write to cs exception ",e)
                        listToWrite.append("NOT FOUND IN RADb")
                print listToWrite
                file_to_write.writerow(listToWrite)
        file_to_write.writerow([])
        csvFile.close()

def write_to_json(flapsDict, topTalker_sources, START_TIME):
	open("Analysis.json",'w').close() # to clear contents of the file  
        file_to_write = open("Analysis.json","w")
	Date = START_TIME[0:10]
        list_file = []
        for line2 in topTalker_sources:
                listToWrite = {"Date":"","Prefix":"","DataSentInbits":"","Events":"","Organization":""}
                listToWrite["Date"] = Date
                listToWrite["Prefix"] = line2[0]
                listToWrite["DataSentInbits"] = int(line2[1])
                ip = line2[0]
                ip = ip[:ip.find("x")]+"0/24"
                listToWrite["Events"] = flapsDict[ip]
                cmd = "whois -h whois.radb.net "+ip+" | grep descr:"
                try:
                        descr = commands.getoutput(cmd).split("\n")[0].split(":")[1].strip(" ")
                        asn = commands.getoutput("whois -h whois.radb.net "+ip+" | grep origin:").split("\n")[0].split(":")[1].strip(" ")
                        asn_number = asn[asn.find("AS")+2:]
                        listToWrite["Organization"] = descr+"  |ASN - "+asn_number
                except Exception as e:
			print "write to json exception ",e
                        listToWrite["Organization"] = "NOT FOUND IN RADb"
                print listToWrite
                list_file.append(listToWrite)
        file_to_write.seek(0)
        file_to_write.write(json.dumps(list_file))
        file_to_write.close()
'''
flapsDict = {u'128.117.212.0/24': 0, u'200.136.80.0/24': 1, u'192.170.227.0/24': 0, u'190.103.184.0/24': 0, u'128.117.181.0/24': 0, u'128.55.205.0/24': 0, u'128.117.140.0/24': 0, u'129.57.199.0/24': 0, u'17.253.27.0/24': 0, u'140.172.138.0/24': 0}
topTalker_sources = [(u'200.136.80.x', 325054380539904), (u'17.253.27.x', 203692342345728), (u'128.117.140.x', 162477296451584), (u'128.117.181.x', 129070168406080), (u'140.172.138.x', 73488065585152), (u'128.55.205.x', 62324857983336), (u'128.117.212.x', 62149955936256), (u'192.170.227.x', 56938831506088), (u'190.103.184.x', 52844537118720), (u'129.57.199.x', 43267863216128)]
START_TIME = "2017-10-18"
write_to_csv(flapsDict, topTalker_sources, START_TIME)
'''
