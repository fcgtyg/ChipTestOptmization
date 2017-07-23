from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext

conf = SparkConf()
sc = SparkContext("local[3]", "NetworkWordCount", conf=conf)  # 2 threads, app name
ssc = StreamingContext(sc, 5)  # sc, time interval for batch update.

"""
conf.set("spark.streaming.blockInterval", "200ms")
print conf.get("spark.rdd.compress")
print conf.getAll()
"""

ssc.checkpoint("C:/SparkCheckpoints/")
sc.setLogLevel("OFF")
counter = sc.accumulator(0)
init1DTable = sc.parallelize([(1, 0), (2, 0), (3, 0), (4, 0), (5, 0), (6, 0), (7, 0), (8, 0), (9, 0), (10, 0)])

rows = [{}]


def createRows(ifAccu=False):
    if ifAccu:
        for i in range(1, 11):
            row = "Test" + str(i)
            if i != 1:
                dependency = rows[0]["Test" + str(i - 1)]
                rows[0][row] = sc.accumulator(dependency.value + 0)
            else:
                rows[0][row] = sc.accumulator(0)
    else:
        for i in range(1, 11):
            row = "Test" + str(i)
            rows[0][row] = 0


nums = ssc.socketTextStream("localhost", 12345).repartition(2)  # stream data from TCP; source, port

"""
#For Stateful app

def updateFunc(new_values, last_sum):
    return sum(new_values) + (last_sum or 0)
"""


def reduceMap(num):
    global counter
    counter += 1
    numInt = int(num)
    rtn = []
    for i in range(numInt, 11):
        rtn.append((i, 1))
    return rtn


tests = nums.flatMap(reduceMap).reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, 25, 5)

tests.pprint(10)
# wordCounts = tests.reduceByKey((lambda x, y: x + y))

# Print the first ten elements of each RDD generated in this DStream to the console
# wordCounts.pprint(10)

ssc.start()  # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate