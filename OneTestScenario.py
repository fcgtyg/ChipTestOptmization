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

    model.setInitialWeights([0])

    streamingContext.checkpoint("C:/SparkCheckpoints/")

    nums = streamingContext.socketTextStream("localhost", 12345) # stream data from TCP; source, port

    tests = nums.flatMap(reduceMap)\
        .reduceByKeyAndWindow(lambda x,y: x+y, lambda x,y: x-y, 25, 5)

    if model is not None:
        train(model, tests)
    tests.pprint(10)

    streamingContext.start()
    streamingContext.awaitTermination()

def train(model, dstream):
    model.trainOn(
        dstream.map(lambda x: LabeledPoint(x[0], Vectors.dense([x[1]]))))
    dstream.map(lambda x: model.latestModel()).pprint(1)