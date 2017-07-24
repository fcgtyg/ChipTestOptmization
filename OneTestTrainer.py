from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint


def reduceMap(num):
    numInt = int(num)
    rtn = []
    for i in range(numInt, 11):
        rtn.append((i, 1))
    return rtn


def train(model, Context=None, streamingContext=None):
    if Context is None:
        Context=SparkContext("local[3]", "Train")
    if streamingContext is None:
        streamingContext = StreamingContext(Context, 5) # sc, time interval for batch update.

    train = streamingContext.socketTextStream("localhost", 12344)
    train = train.flatMap(reduceMap)\
        .reduceByKey(lambda x,y: x+y)\
        .map(lambda x: LabeledPoint(x[0], Vectors.dense([x[1]])))\
        .cache()

    model.setInitialWeights([0])
    model.trainOn(train)

    train.pprint(10)

    streamingContext.start()
    streamingContext.awaitTermination(60)