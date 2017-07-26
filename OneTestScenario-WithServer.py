import threading, time
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.mllib.regression import StreamingLinearRegressionWithSGD

sc = SparkContext("local[5]", "Tester")
sc.setLogLevel("OFF")
model = StreamingLinearRegressionWithSGD(stepSize=0.01)


class SparkThread(threading.Thread):
    global sc, model
    def __init__(self):
        threading.Thread.__init__(self)
        pass

    def run(self):
        """
        from OneTestTrainer import train
        ssc = StreamingContext(sc, 5)
        train(model=model, Context=sc, streamingContext=ssc)
        ssc.stop(stopSparkContext=False)
        """
        ssc = StreamingContext(sc, 5)
        from OneTestScenario import SparkApp
        SparkApp(model=model, Context=sc, streamingContext=ssc)
        print "\nSpark thread ended.\n"


class ServerThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        pass

    def run(self):
        import RandomNumberServer
        RandomNumberServer.start()
        print "\nServer thread stopped.\n"


# NOT USED IN THIS COMMIT.
class TrainingServer(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        import RandomNumberTrainer
        RandomNumberTrainer.start()
        print "\nTrainer server stopped.\n"

threadSpark = SparkThread()
threadServer = ServerThread()
#threadTrainer = TrainingServer()

threadServer.start()
#threadTrainer.start()
threadSpark.start()

