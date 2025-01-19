from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.flume import FlumeUtils
from textblob import TextBlob
import os

def read_full_file(file_path):
    """Reads and returns the full content of the file."""
    if os.path.exists(file_path):
        with open(file_path, 'r', encoding='utf-8') as f:
            return f.read()
    return None

def analyze_sentiment(content):
    """Performs sentiment analysis on the given content."""
    reviews = [review.strip() for review in content.split("\n") if review.strip()]
    results = {"Positive": 0, "Neutral": 0, "Negative": 0}

    for review in reviews:
        sentiment_score = TextBlob(review).sentiment.polarity
        if sentiment_score > 0.1:
            results["Positive"] += 1
        elif sentiment_score < -0.1:
            results["Negative"] += 1
        else:
            results["Neutral"] += 1

    # Calculate percentages
    total = sum(results.values())
    percentages = {
        sentiment: str(round(count / total * 100, 2)) + "% (" + str(count) + "/" + str(total) + ")"
        for sentiment, count in results.items()
    }
    return percentages

def process_event(event):
    """Processes each Flume event."""
    dataset_path = "/home/maria_dev/reviews_swiss_aircraft.txt"  # Path to the dataset
    content = read_full_file(dataset_path)
    if content:
        sentiment_results = analyze_sentiment(content)
        # Combine results into a readable string
        return "Sentiment Analysis Results: " + str(sentiment_results)
    else:
        return "No content available for sentiment analysis."

if __name__ == "__main__":
    sc = SparkContext(appName="FlumeSentimentAnalysis")
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, 1)  # Batch interval of 1 second

    # Create a Flume stream
    flumeStream = FlumeUtils.createStream(ssc, "localhost", 9092)

    # Process each event in the stream
    processed_stream = flumeStream.map(lambda event: process_event(event))
    
    # Display the results in the terminal
    processed_stream.pprint()

    # Start the streaming context
    ssc.start()
    ssc.awaitTermination()
