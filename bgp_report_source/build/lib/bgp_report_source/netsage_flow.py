"""This module queries datasage db for extracting routeflow data """
from elasticsearch import Elasticsearch

def get_sensor_flow_entries(start, end, es_instance):
	"""Returns nflow data group by sensor_id for given start and end time perioed
	Args:
		start (int) : Unix time
		end (int) : Unix time
		es_instance : netsage db instance
	Return:
		list : list dictionaries
	"""
	es_object = Elasticsearch([es_instance])
	#Get top 10 IP group by total data bits sent -
	#Count of distinct sensor ids - 
	query = {"query":{"range":{"start": {"gte":start, "lte":end, "format":"epoch_millis"}}},\
		"size":0, "aggs":{"count_sensor_id":{"cardinality":{"field":"meta.sensor_id.keyword"}}}}
	number_of_sensors = es_object.search(body=query, scroll='1m')["aggregations"]["count_sensor_id"]["value"]

	#final query group by sensor ids - 
	query = {"query":{"range":{"start":{"gte":start, "lte":end, "format":"epoch_millis"}}},\
		"size":0, "aggs":{"group_by_sensor_id":{"terms":{"size":number_of_sensors, "field":"meta.sensor_id.keyword"},\
		"aggs":{"group_by_src_ip":{"terms":{"field":"meta.src_ip.keyword"},\
		"aggs":{"total_bits":{"sum":{"field":"values.num_bits"}}}}}}}}

        return es_object.search(body=query, scroll='1m')["aggregations"]["group_by_sensor_id"]["buckets"]

def get_events_flow_entries(start, end, es_instance):
        """Returns nflow data - Top 10 IP group by total data bits sent -
        Args:
                start (int) : Unix time
                end (int) : Unix time
                es_instance : netsage db instance
        Return:
                list : list dictionaries
        """
        es_object = Elasticsearch([es_instance])
        ##Get top 10 IP group by total data bits sent -
        query = {"query":{"range": {"start": {"gte":start, "lte":end, "format":"epoch_millis"}}},\
                "aggs":{"group_by_src_ip":{"terms":{"field":"meta.src_ip.keyword"}, "aggs":{"total_bits":\
                {"sum":{"field":"values.num_bits"}}}}}}

        return es_object.search(body=query, scroll='1m')["aggregations"]["group_by_src_ip"]["buckets"]

