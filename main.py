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

print("to insert")
print(toInsert)

def prepareObject(unprepared):
    dataRes = []
    dataRes.append(
        {
            "measurement": "devices",
            "tags": {
                "user": "load",
                "brushId": unprepared[16]
            },
            "time": unprepared[-1],
            "fields": {
                "CPUsec": unprepared[0],
                "CPUper": unprepared[1],
                "MEMk": unprepared[2],
                "MEMper": unprepared[3],
                "MAXMEMk": unprepared[4],
                "MAXMEMper": unprepared[5],
                "VCPUS": unprepared[6],
                "NETS": unprepared[7],
                "NETTX": unprepared[8],
                "NETRX": unprepared[9],
                "VBDS": unprepared[10],
                "VBD_OO": unprepared[11],
                "VBD_RD": unprepared[12],
                "VBD_WR": unprepared[13],
                "VBD_RSECT": unprepared[14],
                "VBD_WSECT": unprepared[15],
                "SSID": unprepared[16]
            }
        }
    )
    # measurement = "devices"
    # tags = {"user": "load",
    #         "brushId": "6c89f539-71c6-490d-a28d-6c5d84c0ee2f"}
    # time = unprepared[-1]
    # fields = {}
    # fields["CPUsec"] = unprepared[0]
    # fields["CPUper"] = unprepared[1]
    # fields["MEMk"] = unprepared[2]
    # fields["MEMper"] = unprepared[3]
    # fields["MAXMEMk"] = unprepared[4]
    # fields["MAXMEMper"] = unprepared[5]
    # fields["VCPUS"] = unprepared[6]
    # fields["NETS"] = unprepared[7]
    # fields["NETTX"] = unprepared[8]
    # fields["NETRX"] = unprepared[9]
    # fields["VBDS"] = unprepared[10]
    # fields["VBD_OO"] = unprepared[11]
    # fields["VBD_RD"] = unprepared[12]
    # fields["VBD_WR"] = unprepared[13]
    # fields["VBD_RSECT"] = unprepared[14]
    # fields["VBD_WSECT"] = unprepared[15]
    # fields["SSID"] = unprepared[16]
    # print("FIELDS")
    # print(fields)
    # resultObject = {}
    # resultObject["measurement"] = measurement
    # resultObject["tags"] = tags
    # resultObject["time"] = time
    # resultObject["fields"] = fields
    return dataRes

ressss = prepareObject(toInsert)


client.write_points(ressss)
print("DATA INSERTED TO DB")
result = client.query('SELECT * FROM "labtest"."autogen"."devices" WHERE time > now() - 100000d GROUP BY "user"')
print("Result: {0}".format(result))