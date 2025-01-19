from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.flume import FlumeUtils

def process_line(line):
    # Example transformation logic, modify as per your need
    return "Processed: " + line

if __name__ == "__main__":
    sc = SparkContext(appName="StreamingFlumeLogAggregator")
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, 1)  # Batch interval of 1 second

    # Create a Flume stream
    flumeStream = FlumeUtils.createStream(ssc, "localhost", 9092)

    # Extract the body of the Flume events
    lines = flumeStream.map(lambda x: x[1])

    # Apply your custom processing logic
    processed_lines = lines.map(process_line)

    # Print the results to the console
    processed_lines.pprint()

    # Start the streaming context
    ssc.start()
    ssc.awaitTermination()
