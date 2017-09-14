import commands
import sys
import pandas as pd
import json
import operator
asnFile_sent = open("asnFile_sent","w")
asnInfo_sent = open("asnInfoFile_sent","w")

asnFile_recv = open("asnFile_recv","w")
asnInfo_recv = open("asnInfoFile_recv","w")

asnFile_total = open("asnFile_total","w")
asnInfo_total = open("asnInfoFile_total","w")

def getMax_sent(dataDict):
	returnList = []
	tup1 = []
	sortedDict = sorted(dataDict.items(), key=operator.itemgetter(1), reverse=True)
	count =1
	for ip,values in sortedDict:
    		if count > 10:
      			break
		myIp = ip.replace("x","1")
		cmd = "whois -h whois.radb.net "+myIp+" | grep 'origin' "
		ASN = commands.getoutput(cmd)
		if(ASN =="" or len(myIp) == 0):
			continue
		else:
			myAsn = ASN.split("\n")[0].split("AS")[1]
		if myAsn not in tup1:
			tup1.append(myAsn)
			newList = [myAsn , ip, values]
			asnInfo_sent.write(str(newList[0]) + " " + str(newList[1]) + " "+str(newList[2])+"\n")
			returnList.append(newList)
			count +=1
	asnInfo_sent.close()
	return tup1


def getMax_recv(dataDict):
        returnList = []
        tup1 = []
        sortedDict = sorted(dataDict.items(), key=operator.itemgetter(1), reverse=True)
        count =1
        for ip,values in sortedDict:
                if count > 10:
                        break
                myIp = ip.replace("x","1")
                cmd = "whois -h whois.radb.net "+myIp+" | grep 'origin' "
                ASN = commands.getoutput(cmd)
                if(ASN =="" or len(myIp) == 0):
                        continue
                else:
                        myAsn = ASN.split("\n")[0].split("AS")[1]
                if myAsn not in tup1:
                        tup1.append(myAsn)
                        newList = [myAsn , ip, values]
                        asnInfo_recv.write(str(newList[0]) + " " + str(newList[1]) + " "+str(newList[2])+"\n")
                        returnList.append(newList)
                        count +=1
        asnInfo_recv.close()
        return tup1


def getMax_total(dataDict):
        returnList = []
        tup1 = []
        sortedDict = sorted(dataDict.items(), key=operator.itemgetter(1), reverse=True)
        count =1
        for ip,values in sortedDict:
                if count > 10:
                        break
                myIp = ip.replace("x","1")
                cmd = "whois -h whois.radb.net "+myIp+" | grep 'origin' "
                ASN = commands.getoutput(cmd)
                if(ASN =="" or len(myIp) == 0):
                        continue
                else:
                        myAsn = ASN.split("\n")[0].split("AS")[1]
                if myAsn not in tup1:
                        tup1.append(myAsn)
                        newList = [myAsn , ip, values]
                        asnInfo_total.write(str(newList[0]) + " " + str(newList[1]) + " "+str(newList[2])+"\n")
                        returnList.append(newList)
                        count +=1
        asnInfo_total.close()
        return tup1

def topTalk(filename):
	with open(filename) as data_file:
		data = json.load(data_file)

	ids = set()
	output = {}
 	i=0
	for entry in data["hits"]["hits"]:
		if entry["_index"] == ".kibana":
			continue
		inner = {}
		uniqueId = entry["_id"]
		src_ipAddress = entry["_source"]["meta"]["src_ip"] 
		dst_ipAddress = entry["_source"]["meta"]["dst_ip"]
		bits_sent = (entry["_source"]["values"]["num_bits"])
		if uniqueId in ids:
			print "Duplicate flow ID found "
    		else:
      			ids.add(uniqueId)
		i+=1
		if(output.has_key(src_ipAddress)):
			if(output.get(src_ipAddress).has_key(dst_ipAddress)):
				inner[dst_ipAddress] = bits_sent + output.get(src_ipAddress).get(dst_ipAddress)
				output[src_ipAddress][dst_ipAddress] = inner[dst_ipAddress]
			else:
				inner[dst_ipAddress] = bits_sent
				output[src_ipAddress][dst_ipAddress] = inner[dst_ipAddress]
		else:
			inner[dst_ipAddress] = bits_sent
			output[src_ipAddress] = inner
	print output

# To find the src dst pair with max bits exchanged
	maxDataExchanged = -1
	finalSrc = ""
	finalDst = ""
	srcDict = {} 
	dstDict = {} #srcDict will hold scr + data sent and dstDict will hold dst + data recieved
	for src, values in output.iteritems():
		maxSrc=0
		for dst, dataInMb in values.iteritems():
			#print "-- src " + src + " dst " + dst + " value " + str(dataInMb) + "\n"
			maxSrc = maxSrc + dataInMb
			if(dst in dstDict):
				dstDict[dst] = dstDict[dst]+dataInMb
			else:
				dstDict[dst] = dataInMb
			if dataInMb > maxDataExchanged:
				finalSrc = src
				finalDst = dst
				maxDataExchanged = dataInMb
		srcDict[src] = maxSrc

	# Write file with ASN sent data
	finalList = getMax_sent(srcDict)
	print "finalist_sent"
	print(finalList)
	asnFile_sent.write(str(finalList))
	asnFile_sent.close()

	# Write file with ASN receive data
	finalList = getMax_recv(dstDict)
	print "finalList_recev"
	print finalList
	asnFile_recv.write(str(finalList))
        asnFile_recv.close()


	# Write file with ASN total  data -> sent + receive
        totalDict = {k:v for k,v in dstDict.items()}
        for key,values in srcDict.items():
        	if key in totalDict:
                	totalDict[key]+=values
        	else:
                	totalDict[key]=0
        
	finalList = getMax_total(totalDict)
        print "finalList_total"
        print finalList
        asnFile_total.write(str(finalList))
        asnFile_total.close()

if __name__ == "__main__":
	topTalk("nflow")

