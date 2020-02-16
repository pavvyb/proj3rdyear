from influxdb import InfluxDBClient
from datetime import datetime
import os

client = InfluxDBClient(host='localhost', port=8086)
client.create_database('labtest')
dbaselist = client.get_list_database()
print("Databases:", dbaselist)
client.switch_database('labtest')

fields = ["CPUsec","CPUper","MEMk", "MEMper", "MAXMEMk", "MAXMEMper", "VCPUS", "NETS", "NETTX", "NETRX", "VBDS", "VBD_OO", "VBD_RD", "VBD_WR", "VBD_RSECT", "VBD_WSECT", "SSID"]

json_body = [
    {
        "measurement": "brushEvents",
        "tags": {
            "user": "load",
            "brushId": "6c89f539-71c6-490d-a28d-6c5d84c0ee2f"
        },
        "time": "2018-03-28T8:01:00Z",
        "fields": {
            "duration": 127
        }
    },
    {
        "measurement": "brushEvents",
        "tags": {
            "user": "load",
            "brushId": "6c89f539-71c6-490d-a28d-6c5d84c0ee2f"
        },
        "time": "2018-03-29T8:04:00Z",
        "fields": {
            "duration": 132
        }
    },
    {
        "measurement": "brushEvents",
        "tags": {
            "user": "load",
            "brushId": "6c89f539-71c6-490d-a28d-6c5d84c0ee2f"
        },
        "time": "2018-03-30T8:02:00Z",
        "fields": {
            "duration": 129
        }
    }
]

pathh = "/home/popufey/Desktop/lab/xentop.1581649624"

client.write_points(json_body)
result = client.query('SELECT "duration" FROM "labtest"."autogen"."brushEvents" WHERE time > now() - 10000d GROUP BY "user"')
print("Result: {0}".format(result))

def convertTime(unixformat):
    standardform = datetime.utcfromtimestamp(int(unixformat.split(".")[1])).strftime('%Y-%m-%d %H:%M:%S')
    # optional
    tempArr = standardform.split(" ")
    standardform = tempArr[0] + "T" + tempArr[1] + "Z"
    return standardform

def readFile(path):
    answ = []
    f = open(path, "r")
    fname = os.path.basename(f.name)
    for x in f:
        tmp = x.split(" ")
        tmp2 = []
        for i in tmp:
            if (i != ''):
                tmp2.append(i)
        tmp2[-1] = tmp2[-1][:-1]
        if (tmp2[0]=='load'):
            answ = tmp2
    fname = convertTime(fname)
    answ.append(fname)
    return answ

def getData(inp):
    return inp[2:]

def extractData(path):
    dataArr = getData(readFile(path))
    return dataArr

toInsert = extractData(pathh)