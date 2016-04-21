from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TotalSpace")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    space = int(fields[4])
    street = str(fields[2])
    return (street,space)
    
lines = sc.textFile("F:/Matakuliah/Semester 6/Big Data/Bicycle_Parking__Public_.csv")
rdd = lines.map(parseLine)
TotalSpace = rdd.map(lambda x: (x)).reduceByKey(lambda x, y: x + y)
results = TotalSpace.collect()

for result in results:
    print result