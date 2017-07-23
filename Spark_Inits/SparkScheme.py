from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext("local[*]", "AppName") # Local cores (number can be specified) and application name.
ssc = StreamingContext(sc, 1) # Batch interval, 1 seconds for default.

"""
    1) Define the input sources by creating input DStreams.
    2) Define the streaming computations by applying transformation and output operations to DStreams.
"""



ssc.start()
ssc.awaitTermination()

#ssc.stop() #for manual stopping