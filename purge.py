import click
import twitter
import time
import csv
from itertools import islice, chain
import datetime
import email

def rfc822(timestamp):
    return datetime.datetime.fromtimestamp(email.utils.mktime_tz(email.utils.parsedate_tz(timestamp)))


def iter_statuses(api):
    max_id = None
    total_statuses = 0
    while True:
        statuses = api.GetUserTimeline(count=500, max_id=max_id)
        time.sleep(1)
        print("Requesting statuses with max_id {0}".format(max_id))
        if len(statuses) == 0:
            break
        for status in statuses:
            yield (total_statuses, status)
            total_statuses += 1
            max_id = status.id


def batch(iterable, size):
    sourceiter = iter(iterable)
    while True:
        batchiter = islice(sourceiter, size)
        yield chain([batchiter.next()], batchiter)

def read_tweet_ids(infile):
    r = csv.DictReader(infile)
    return [row["tweet_id"] for row in r]


def iter_tweets(tweet_ids, api):
    for id_batch in batch(tweet_ids, 100):
        statuses = api.GetStatuses(list(id_batch))
        for status in statuses:
            yield status

@click.command()
@click.argument('infile', type=click.File('rb'))
@click.option('--consumer_key', envvar='CONSUMER_KEY')
@click.option('--consumer_secret', envvar='CONSUMER_KEY')
@click.option('--access_token', envvar='ACCESS_TOKEN')
@click.option('--access_token_secret', envvar='ACCESS_TOKEN_SECRET')
@click.option('--min_favs', default=5)
@click.option('--older_than_days', default=30)
def purge(infile, consumer_key, consumer_secret, access_token, access_token_secret, min_favs, older_than_days):
    """ Deletes tweets given the CSV file produced from an account's "archive". I had
        trouble getting all my tweets through the timeline API, so I had to resort to
        this. The CSV dump gives me the ids of the statuses/tweets. I look them up
        individually via the API and then choose which to delete.
    """
    api = twitter.Api(consumer_key=consumer_key,
                        consumer_secret=consumer_secret,
                        access_token_key=access_token,
                        access_token_secret=access_token_secret)
    tweet_ids = read_tweet_ids(infile)
    today = datetime.datetime.now()
    for (i, tweet) in enumerate(iter_tweets(tweet_ids, api)):
        created_at = rfc822(tweet.created_at)
        delta = today - created_at
        is_old = delta.days > older_than_days
        is_liked = tweet.favorite_count > min_favs or tweet.retweet_count > min_favs
        is_rt = tweet.retweeted_status is not None
        if is_old and (is_rt or not is_liked):
            print("--------- {0}".format(i))
            print("WILL DELETE")
            print(tweet.text)
            api.DestroyStatus(tweet.id)
            time.sleep(3)
        else:
            print("--------- {0}".format(i))
            print("WILL KEEP")
            print(tweet.text)
        
    
if __name__ == '__main__':
    purge(auto_envvar_prefix='TWITTER')