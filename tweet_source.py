#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function, division
import argparse
import json
import os

from dateutil import parser as date_parser
import requests
import twitter


DEFAULT_API_ENDPOINT = 'localhost:8001'
TWITTER_ENV_CREDENTIALS = {
    'consumer_key': 'TWITTER_CONSUMER_KEY',
    'consumer_secret': 'TWITTER_CONSUMER_SECRET',
    'access_token_key': 'TWITTER_ACCESS_TOKEN_KEY',
    'access_token_secret': 'TWITTER_ACCESS_TOKEN_SECRET'
}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Pushes tweets to a reinfer.io source.')
    _arg = parser.add_argument
    _arg('--api', type=str, action='store', metavar='HOST:PORT',
         default=DEFAULT_API_ENDPOINT,
         help='API endpoint (default=%s)' %  DEFAULT_API_ENDPOINT)
    _arg('source_id', type=str, action='store', metavar='SID',
         help='Base64 encoded source id')
    args = parser.parse_args()

    credentials = {}
    for k, v in TWITTER_ENV_CREDENTIALS.items():
        credentials[k] = os.environ[v]

    api = twitter.Api(**credentials)
    stream_sample = api.GetStreamSample()

    headers = {'content-type': 'application/json'}
    for tweet in stream_sample:
        if 'text' in tweet:
            timestamp = date_parser.parse(tweet['created_at']).strftime('%s')
            payload = {'source_id': args.source_id,
                       'records': [[timestamp, tweet['lang'], tweet['text']]]}
            resp = requests.post('http://%s/api/sources/put' % args.api,
                                 data=json.dumps(payload), headers=headers)
            print(resp.json())
            assert resp.status_code == 200
