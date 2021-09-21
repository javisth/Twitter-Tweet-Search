import csv
import logging
import threading
import concurrent.futures
import time
from typing import List, Tuple

#def thread_function(name):
    #logging.info("Thread %s: starting", name)
    #time.sleep(2)
    #logging.info("Thread %s: finishing", name)

def find(L, target):
    start = 0
    end = len(L) - 1

    while start <= end:
        middle = (start + end)// 2
        if L[middle] == target:
            return middle
        elif L[middle] < target:
            start = middle + 1
        else:
            end = middle - 1
    return -1


class TweetIndex:
    def __init__(self):
        self.list_of_tweets = []

    def process_tweets(self, list_of_timestamps_and_tweets: List[Tuple[int, str]]) -> None:
        """
        process_tweets processes a list of tweets and initializes any data structures needed for
        searching over them.
        :param list_of_timestamps_and_tweets: A list of tuples consisting of a timestamp and a tweet.
        """
        for row in list_of_timestamps_and_tweets:
            timestamp = int(row[0])
            tweet = str(row[1])
            self.list_of_tweets.append((tweet, timestamp))

    def search(self, query: str) -> Tuple[str, int]:
        """
        search looks for the most recent tweet (highest timestamp) that contains all words in list_of_words. You may
        assume that all timestamps are unique.
        
        NOTE: If you are doing the improved search quality optional extension, you do not have to return the result with the highest timestamp.
        You can just return your "optimal" result.
        :param list_of_words: list of words that must be in the tweet
        :return: a tuple of the text of the most recent tweet that contains all words in list_of_words, as well as the
        associated timestamp. If no such tweet exists, return empty string and -1
        """
        list_of_words = query.split(" ")
        result_tweet, result_timestamp = "", -1
        for tweet, timestamp in self.list_of_tweets:
            words_in_tweet = tweet.split(" ")
            words_in_tweet.sort()
            tweet_contains_query = True
            for word in list_of_words:
                if find(words_in_tweet, word) == -1:
                    tweet_contains_query = False
                    break     
            if tweet_contains_query and timestamp > result_timestamp:
                result_tweet, result_timestamp = tweet, timestamp
        return result_tweet, result_timestamp

if __name__ == "__main__":
    #format = "%(asctime)s: %(message)s"
    #logging.basicConfig(format=format, level=logging.INFO, datefmt="%H:%M:%S")
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        executor.map(TweetIndex, range(3))

    tweet_csv_filename = "../data/small.csv" 
    list_of_tweets = []
    with open(tweet_csv_filename, "r") as f:
        csv_reader = csv.reader(f, delimiter=",")
        for i, row in enumerate(csv_reader):
            if i == 0:
                # header
                continue
            timestamp = int(row[0])
            tweet = str(row[1])
            list_of_tweets.append((timestamp, tweet))

    ti = TweetIndex()
    ti.process_tweets(list_of_tweets)
    assert ti.search("hello") == ('hello this is also neeva', 15)
    assert ti.search("hello me") == ('hello not me', 14)
    assert ti.search("hello bye") == ('hello bye', 3)
    assert ti.search("hello this bob") == ('hello neeva this is bob', 11)
    assert ti.search("notinanytweets") == ('', -1)
    print("Success!")

