from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint


def reduceMap(num):
    numInt = int(num)
    rtn = []
    for i in range(numInt, 11):
        rtn.append((i, 1))
    return rtn


def SparkApp(model=None, Context=None, streamingContext=None):
    if Context is None:
        Context = SparkContext("local[3]", "Predict")
    if streamingContext is None:
        streamingContext = StreamingContext(Context, 5) # sc, time interval for batch update.

    streamingContext.checkpoint("C:/SparkCheckpoints/")

    nums = streamingContext.socketTextStream("localhost", 12345).repartition(2) # stream data from TCP; source, port

    tests = nums.flatMap(reduceMap).reduceByKeyAndWindow(lambda x,y: x+y, lambda x,y: x-y, 25, 5)

    tests.pprint(10)

    if model is not None:
        predict(model, tests)

    #model.predictOnValues(tests.map(lambda x: (x.label, x.feature)))
    streamingContext.start()
    streamingContext.awaitTermination()

def predict(model, dstream):
    model.predictOnValues(dstream.map(lambda x: LabeledPoint(x[0], Vectors.dense([x[1], 0, 0]))))
    dstream.pprint(10)