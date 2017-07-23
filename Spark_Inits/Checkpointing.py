from pyspark import SparkContext
from pyspark.streaming import StreamingContext

def createContext(DOSYA_YOLU):
    sc = SparkContext("local[2]", "CheckpointingSample")
    ssc = StreamingContext(sc, 5)

    """
    Transformations
    Actions
    Outputs
    """
    ssc.checkpoint(DOSYA_YOLU)

YEDEKLER = "D:/SparkCheckpoints/"
context = StreamingContext.getOrCreate("DOSYA_YOLU", lambda: createContext(YEDEKLER))

"""
...
"""

context.start()  # Start the computation
context.awaitTermination()  # Wait for the computation to terminate