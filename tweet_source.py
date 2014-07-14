#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function, division
import argparse
import json
import os
import sys
import time

from dateutil import parser as date_parser
import requests
import twitter

BATCH_INTERVAL_SECS = 1
DEFAULT_API_ENDPOINT = 'localhost:8001'

TWITTER_ENV_CREDENTIALS = {
    'consumer_key': 'TWITTER_CONSUMER_KEY',
    'consumer_secret': 'TWITTER_CONSUMER_SECRET',
    'access_token_key': 'TWITTER_ACCESS_TOKEN_KEY',
    'access_token_secret': 'TWITTER_ACCESS_TOKEN_SECRET'
}

FEATURES = [
    {'name': 'timestamp', 'type': 'real'},
    {'name': 'language', 'type': 'categorical'},
    {'name': 'tweet', 'type': 'text'}
]


def create_source(endpoint, token, name):
    headers = {'content-type': 'application/json'}
    payload = {'token': token,
               'name': name,
               'private': False,
               'frozen': False,
               'features': FEATURES}
    resp = requests.post('http://%s/api/sources/create' % endpoint,
                         data=json.dumps(payload), headers=headers)
    print('[%d]  %s' % (resp.status_code, resp.json()), file=sys.stderr)
    if resp.status_code != 200:
        return None
    return resp.json()['source_id']


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Pushes tweets to a reinfer.io source.')
    _arg = parser.add_argument
    _arg('--endpoint', type=str, action='store', metavar='HOST:PORT',
         default=DEFAULT_API_ENDPOINT,
         help='API endpoint (default=%s)' %  DEFAULT_API_ENDPOINT)
    _arg('--new', action='store_true', default=False,
         help='Create a new source where to push tweets')
    _arg('--token', action='store', required=True, help='API token')
    _arg('source', type=str, action='store', metavar='source',
         help='Base64 encoded source id or the source name if '
         '--new was used')
    args = parser.parse_args()

    credentials = {}
    for k, v in TWITTER_ENV_CREDENTIALS.items():
        credentials[k] = os.environ[v]

    api = twitter.Api(**credentials)
    stream_sample = api.GetStreamSample()

    source_id = create_source(args.endpoint, args.token, args.source) \
                if args.new else args.source
    if source_id is None:
        sys.exit(1)

    headers = {'content-type': 'application/json'}
    records = []
    endpoint = 'http://%s/api/sources/put' % args.endpoint
    start = time.time()
    for tweet in stream_sample:
        if 'text' in tweet:
            timestamp = date_parser.parse(tweet['created_at']).strftime('%s')
            records.append([timestamp, tweet['lang'], tweet['text']])
        end = time.time()
        if end - start > BATCH_INTERVAL_SECS and len(records) > 0:
            start = end
            payload = {'token': args.token,
                       'source_id': source_id,
                       'records': records}
            resp = requests.post(
                endpoint, data=json.dumps(payload), headers=headers)
            if resp.status_code != 200:
                print('API responded with: %s' % resp.json())
            else:
                print('[source_id=%s] Pushed %d tweets' %
                      (source_id, len(records)))
                records = []
