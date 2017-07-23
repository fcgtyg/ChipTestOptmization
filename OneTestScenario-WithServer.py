import threading
from sys import exit
class SparkThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        pass
    def run(self):
        from OneTestScenario import SparkApp
        SparkApp()


class ServerThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        pass
    def run(self):
        import RandomNumberServer
        RandomNumberServer.start()

threadSpark = SparkThread()
threadServer = ServerThread()

print "Hello"

threadSpark.start()
threadServer.start()
