#!/usr/bin/python3
import os
import csv
import time
import multiprocessing
from pprint import pprint
from influxdb import InfluxDBClient

host = '127.0.0.1'
username = 'admin'
password = 'VMware1!'

client = InfluxDBClient(host=host, port=8086, username=username, password=password)
client.switch_database('stats')

date = os.popen("date +%s").read().split('\n')
time = ((int(date[0])) * 1000000000 - 10000000000)
hn = os.popen("hostname").read().split('\n')
df = os.popen("df | tail -n +2 | sed -e \'s/%//g\' | sed -e \'s/[[:space:]]\+/,/g\'").readlines()
df = [i.rstrip('\n') for i in df]
pr={}
for row in csv.reader(df):
     pr['%s' % row[5]] = {'size': row[1], 'used': row[2], 'Avail': row[3], 'pct_use': row[4], 'fs': row[0]}

for p in pr.keys():
    for x,y in pr[p].items():
        if 'fs' in x:
            pr[p][x] = str(y)
        else:
            pr[p][x] = int(y)

invalid = {"fs"}
def without_keys(d, keys):
    return {x: d[x] for x in d if x not in keys}
fs = []
for i in pr.keys():
	fs.append(i)
influx_data = []
for i in fs:
	influx_data.append(
		{
			"measurement": "df",
			"tags": {
				"hostname" : hn[0],
				"mount point" : i,
				"fs" : pr[i]['fs']
			},
			"time": time,
			"fields": without_keys(pr[i], invalid)
			}
			)
#print (influx_data)
client.write_points(influx_data)
