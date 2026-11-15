from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext("local[2]", "WordCount")
ssc = StreamingContext(sc, 5)
lines = ssc.socketTextStream("localhost", 9999)
words = lines.flatMap(lambda l: l.split(" "))
pairs = words.map(lambda w: (w, 1))
counts = pairs.reduceByKey(lambda a, b: a + b)
counts.pprint()
longw = words.filter(lambda w: len(w) > 5)
longw.pprint()
ssc.start()
ssc.awaitTermination()
