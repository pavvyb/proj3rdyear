from influxdb import InfluxDBClient
from datetime import datetime
from sparklines import sparklines
import os
from os import listdir
from os.path import isfile, join

client = InfluxDBClient(host='localhost', port=8086)
client.create_database('labtest')
dbaselist = client.get_list_database()
print("Databases:", dbaselist)
client.switch_database('labtest')

# fields = ["CPUsec","CPUper","MEMk", "MEMper", "MAXMEMk", "MAXMEMper", "VCPUS", "NETS", "NETTX", "NETRX", "VBDS", "VBD_OO", "VBD_RD", "VBD_WR", "VBD_RSECT", "VBD_WSECT", "SSID"]
#
# json_body = [
#     {
#         "measurement": "brushEvents",
#         "tags": {
#             "user": "load",
#             "brushId": "6c89f539-71c6-490d-a28d-6c5d84c0ee2f"
#         },
#         "time": "2018-03-28T8:01:00Z",
#         "fields": {
#             "duration": 127
#         }
#     },
#     {
#         "measurement": "brushEvents",
#         "tags": {
#             "user": "load",
#             "brushId": "6c89f539-71c6-490d-a28d-6c5d84c0ee2f"
#         },
#         "time": "2018-03-29T8:04:00Z",
#         "fields": {
#             "duration": 132
#         }
#     },
#     {
#         "measurement": "brushEvents",
#         "tags": {
#             "user": "load",
#             "brushId": "6c89f539-71c6-490d-a28d-6c5d84c0ee2f"
#         },
#         "time": "2018-03-30T8:02:00Z",
#         "fields": {
#             "duration": 129
#         }
#     }
# ]

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
    return dataRes

rootFolder = "/home/popufey/Desktop/lab/fastio/"
def getAllFilepaths(mypath):
    onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]
    paths = []
    qqq = [i for i in onlyfiles if i.startswith('xentop')]
    for i in qqq:
        elll = mypath + i
        paths.append(elll)
    return paths

allFilepaths = getAllFilepaths(rootFolder)
print("FILES", allFilepaths)

metricArray = []

# Extracting data from all filepaths in the directory
for i in allFilepaths:
    toInsert = extractData(i)
    print("to insert")
    print(toInsert)
    metricArray.append(toInsert)
    ressss = prepareObject(toInsert)
    client.write_points(ressss)
    print("DATA INSERTED TO DB")

# Arrays for all metrics
metric1 = []
metric2 = []
metric3 = []
metric4 = []
metric5 = []
metric6 = []
metric7 = []
metric8 = []
metric9 = []
metric10 = []
metric11 = []
metric12 = []
metric13 = []
metric14 = []
metric15 = []
metric16 = []
metric17 = []

# Insering data to metrics arrays
for i in metricArray:
    metric1.append(float(i[0]))
    metric2.append(float(i[1]))
    metric3.append(float(i[2]))
    metric4.append(float(i[3]))
    metric5.append(float(i[4]))
    metric6.append(float(i[5]))
    metric7.append(float(i[6]))
    metric8.append(float(i[7]))
    metric9.append(float(i[8]))
    metric10.append(float(i[9]))
    metric11.append(float(i[10]))
    metric12.append(float(i[11]))
    metric13.append(float(i[12]))
    metric14.append(float(i[13]))
    metric15.append(float(i[14]))
    metric16.append(float(i[15]))
    metric17.append(float(i[16]))

# Final arrays for metrics
finalmetric1 = ["CPUsec", sparklines(metric1)]
finalmetric2 = ["CPUper", sparklines(metric2)]
finalmetric3 = ["MEMk", sparklines(metric3)]
finalmetric4 = ["MEMper", sparklines(metric4)]
finalmetric5 = ["MAXMEMk", sparklines(metric5)]
finalmetric6 = ["MAXMEMper", sparklines(metric6)]
finalmetric7 = ["VCPUS", sparklines(metric7)]
finalmetric8 = ["NETS", sparklines(metric8)]
finalmetric9 = ["NETTX", sparklines(metric9)]
finalmetric10 = ["NETRX", sparklines(metric10)]
finalmetric11 = ["VBDS", sparklines(metric11)]
finalmetric12 = ["VBD_OO", sparklines(metric12)]
finalmetric13 = ["VBD_RD", sparklines(metric13)]
finalmetric14 = ["VBD_WR", sparklines(metric14)]
finalmetric15 = ["VBD_RSECT", sparklines(metric15)]
finalmetric16 = ["VBD_WSECT", sparklines(metric16)]
finalmetric17 = ["SSID", sparklines(metric17)]

# result = client.query('SELECT * FROM "labtest"."autogen"."devices" WHERE time > now() - 100000d GROUP BY "user"')
# print("Result: {0}".format(result))

# Array of pairs [metric, sparkline]
metricArray = []
metricArray.append(finalmetric1)
metricArray.append(finalmetric2)
metricArray.append(finalmetric3)
metricArray.append(finalmetric4)
metricArray.append(finalmetric5)
metricArray.append(finalmetric6)
metricArray.append(finalmetric7)
metricArray.append(finalmetric8)
metricArray.append(finalmetric9)
metricArray.append(finalmetric10)
metricArray.append(finalmetric11)
metricArray.append(finalmetric12)
metricArray.append(finalmetric13)
metricArray.append(finalmetric14)
metricArray.append(finalmetric15)
metricArray.append(finalmetric16)
metricArray.append(finalmetric17)

x = ["Metric 1", sparklines([1, 2, 3, 4, 5.0, 9, 3, 2, 1, 434, 341, 262, 133, 3, 2, 1, 1,2, 1, 1, 16, 22, 4, 42])]
y = ["Metric 2", sparklines([133, 242, 421, 423, 542, 434, 341, 262, 133, 3, 2, 1, 1, 16, 22, 4, 42, 1, 2, 3, 4, 5.0, 9, 3, 2, 1])]
z = ["Metric 3", sparklines([1, 16, 22, 4, 42, 12, 3, 133, 242, 421, 423, 542, 434, 341, 262, 133, 3, 2, 1, 1, 16, 22, 4,  12, 3, 133, 242, 421, 42, 12, 3, 2, 1])]
metrics = [x,y,z]


print("Testing the sparklines for an existing metrics")
for item in metricArray:
    print(item[0], item[1][0])