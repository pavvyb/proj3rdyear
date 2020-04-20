from influxdb import InfluxDBClient
from datetime import datetime
from sparklines import sparklines
import os
from os import listdir
from os.path import isfile, join

client = InfluxDBClient(host='localhost', port=8086)
client.create_database('newtest')
dbaselist = client.get_list_database()
print("Databases:", dbaselist)
client.switch_database('newtest')

pathh = "/home/popufey/Desktop/proj3rdyear/xentop.1581649624"

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

rootFolder = "/home/popufey/Desktop/proj3rdyear/fastio/"
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
    print(ressss)
    client.write_points(ressss)
    print("DATA INSERTED TO DB")


x = ["Metric 1", sparklines([1, 2, 3, 4, 5.0, 9, 3, 2, 1, 434, 341, 262, 133, 3, 2, 1, 1,2, 1, 1, 16, 22, 4, 42])]
y = ["Metric 2", sparklines([133, 242, 421, 423, 542, 434, 341, 262, 133, 3, 2, 1, 1, 16, 22, 4, 42, 1, 2, 3, 4, 5.0, 9, 3, 2, 1])]
z = ["Metric 3", sparklines([1, 16, 22, 4, 42, 12, 3, 133, 242, 421, 423, 542, 434, 341, 262, 133, 3, 2, 1, 1, 16, 22, 4,  12, 3, 133, 242, 421, 42, 12, 3, 2, 1])]
metrics = [x,y,z]

def findMin(inpArr):
    mini = inpArr[0]
    for i in inpArr:
        if (i < mini):
            mini = i
    return mini

def correctMin(inpArr):
    mii = findMin(inpArr)
    for i in inpArr:
        i = i - mii
    return inpArr
def makeSparksNew(inpArr):
    outpArr = []

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
    for i in inpArr:
        metric1.append(float(i['CPUsec']))
        metric2.append(float(i["CPUper"]))
        metric3.append(float(i['MEMk']))
        metric4.append(float(i['MEMper']))
        metric5.append(float(i['MAXMEMk']))
        metric6.append(float(i['MAXMEMper']))
        metric7.append(float(i['VCPUS']))
        metric8.append(float(i['NETS']))
        metric9.append(float(i['NETTX']))
        metric10.append(float(i['NETRX']))
        metric11.append(float(i['VBDS']))
        metric12.append(float(i['VBD_OO']))
        metric13.append(float(i['VBD_RD']))
        metric14.append(float(i['VBD_WR']))
        metric15.append(float(i['VBD_RSECT']))
        metric16.append(float(i['VBD_WSECT']))
        metric17.append(float(i['SSID']))

    # Correctiong minimals
    metric1 = correctMin(metric1)
    metric2 = correctMin(metric2)
    metric3 = correctMin(metric3)
    metric4 = correctMin(metric4)
    metric5 = correctMin(metric5)
    metric6 = correctMin(metric6)
    metric7 = correctMin(metric7)
    metric8 = correctMin(metric8)
    metric9 = correctMin(metric9)
    metric10 = correctMin(metric10)
    metric11 = correctMin(metric11)
    metric12 = correctMin(metric12)
    metric13 = correctMin(metric13)
    metric14 = correctMin(metric14)
    metric15 = correctMin(metric15)
    metric16 = correctMin(metric16)
    metric17 = correctMin(metric17)


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

    # Array of pairs [metric, sparkline]
    outpArr = []
    outpArr.append(finalmetric1)
    outpArr.append(finalmetric2)
    outpArr.append(finalmetric3)
    outpArr.append(finalmetric4)
    outpArr.append(finalmetric5)
    outpArr.append(finalmetric6)
    outpArr.append(finalmetric7)
    outpArr.append(finalmetric8)
    outpArr.append(finalmetric9)
    outpArr.append(finalmetric10)
    outpArr.append(finalmetric11)
    outpArr.append(finalmetric12)
    outpArr.append(finalmetric13)
    outpArr.append(finalmetric14)
    outpArr.append(finalmetric15)
    outpArr.append(finalmetric16)
    outpArr.append(finalmetric17)

    return outpArr

while (True):
    print('Specify the start date:')
    startdate = input('Enter the date in the format yyyy-mm-dd (example: 2015-08-17):\n')
    ques = ''
    ques = input('Do you want to specify the time? [y/n]')
    if (ques=='y'):
        starttime = input('Enter the time in the format hh:mm:ss (example: 23:48:00):')
    else:
        starttime = '1:11:11'
    print('Specify the finish date:')
    enddate = input('Enter the date in the format yyyy-mm-dd (example: 2015-08-18):\n')
    ques = ''
    ques = input('Do you want to specify the time? [y/n]')
    if (ques == 'y'):
        endtime = input('Enter the time in the format hh:mm:ss (example: 00:54:00):')
    else:
        endtime = '1:11:11'
    finalstart = startdate + 'T' + starttime + 'Z'
    finalend = enddate + 'T' + endtime + 'Z'
    finalend = "'"+finalend+"'"
    finalstart = "'"+finalstart+"'"
    requestbd = 'SELECT * FROM "newtest"."autogen"."devices" WHERE time >= '+ finalstart + ' AND time <= ' + finalend + 'ORDER BY time ASC'
    # result = client.query('SELECT * FROM "newtest"."autogen"."devices" WHERE time > now() - 100000d GROUP BY "user"')
    result = client.query(requestbd)
    resultset = []
    print(requestbd)
    for qus in result:
        for qq in qus:
            resultset.append(qq)
    # for finn in resultset:
    #     print(finn)
    answerArray = makeSparksNew(resultset)


    print("The Sparklines for this timeframe:")
    for item in answerArray:
        print(item[0], item[1][0])
