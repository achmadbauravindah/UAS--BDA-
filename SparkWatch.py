from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.flume import FlumeUtils

import os

def read_full_file(file_path):
    """Reads and returns the full content of the file."""
    if os.path.exists(file_path):
        with open(file_path, 'r') as f:
            return f.read()
    return "File not found or empty."

if __name__ == "__main__":
    sc = SparkContext(appName="FlumeFileMonitor")
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, 1)  # Batch interval of 1 second

    # Create a Flume stream
    flumeStream = FlumeUtils.createStream(ssc, "localhost", 9092)

    # Path to the dataset file
    dataset_path = "/home/maria_dev/dataset.txt"

    # Process each Flume event and trigger reading the file
    def process_event(event):
        # Triggered when a change occurs
        print("File changed. Current content:")
        print(read_full_file(dataset_path))

    # Apply processing to each RDD in the stream
    full_file_contents = flumeStream.map(lambda event: process_event(event))

    # Print the full file contents to the terminal
    full_file_contents.pprint()

    # Start the streaming context
    ssc.start()
    ssc.awaitTermination()