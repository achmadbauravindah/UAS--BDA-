from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == "__main__":
    # Create Spark context and streaming context
    sc = SparkContext(appName="FileStreamingExample")
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, 1)  # Batch interval of 1 second

    # Monitor the directory where the dataset file is stored
    monitored_dir = "/path/to/monitored_directory"
    
    # Create a DStream from the text files in the monitored directory
    lines = ssc.textFileStream(monitored_dir)

    # Print the lines in the terminal
    lines.pprint()

    # Start the streaming context
    ssc.start()
    ssc.awaitTermination()