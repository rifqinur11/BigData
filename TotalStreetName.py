from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TotalStreetName")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    streetName = str(fields[2])
    count = 1
    return (streetName, count)
    
lines = sc.textFile("F:/Matakuliah/Semester 6/Big Data/Bicycle_Parking__Public_.csv")
rdd = lines.map(parseLine)
totalBystreetName = rdd.reduceByKey(lambda x, y: x + 1).sortByKey()

for result in totalBystreetName.collect():
    print result
