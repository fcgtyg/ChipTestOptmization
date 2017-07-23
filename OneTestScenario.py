def SparkApp():
    from pyspark import SparkContext, SparkConf
    from pyspark.streaming import StreamingContext

    conf = SparkConf()
    sc = SparkContext("local[3]", "NetworkWordCount", conf=conf) # 2 threads, app name
    ssc = StreamingContext(sc, 5) # sc, time interval for batch update.

    ssc.checkpoint("C:/SparkCheckpoints/")
    sc.setLogLevel("OFF")

    nums = ssc.socketTextStream("localhost", 12345).repartition(2) # stream data from TCP; source, port

    def reduceMap(num):
        numInt = int(num)
        rtn = []
        for i in range(numInt, 11):
            rtn.append((i,1))
        return rtn


    tests = nums.flatMap(reduceMap).reduceByKeyAndWindow(lambda x,y: x+y, lambda x,y: x-y, 25, 5)

    tests.pprint(10)

    ssc.start()
    ssc.awaitTermination()
