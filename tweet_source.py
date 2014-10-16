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
DEFAULT_API_ENDPOINT = 'http://localhost:8001'

TWITTER_ENV_CREDENTIALS = {
    'consumer_key': 'TWITTER_CONSUMER_KEY',
    'consumer_secret': 'TWITTER_CONSUMER_SECRET',
    'access_token_key': 'TWITTER_ACCESS_TOKEN_KEY',
    'access_token_secret': 'TWITTER_ACCESS_TOKEN_SECRET'
}

FEATURES = [
    {'name': 'timestamp', 'type': 'numerical'},
    {'name': 'language', 'type': 'categorical'},
    {'name': 'tweet', 'type': 'text'}
]


def create_dataset(endpoint, token, name):
    headers = {'content-type': 'application/json'}
    payload = {'token': token,
               'name': name,
               'private': False,
               'frozen': False,
               'features': FEATURES}
    resp = requests.post('%s/api/datasets/create' % endpoint,
                         data=json.dumps(payload), headers=headers)
    print(resp)
    print('[%d]  %s' % (resp.status_code, resp.json()), file=sys.stderr)
    if resp.status_code != 200:
        return None
    return resp.json()['dataset_id']


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Pushes tweets to a reinfer.io dataset.')
    _arg = parser.add_argument
    _arg('--endpoint', type=str, action='store', metavar='HOST:PORT',
         default=DEFAULT_API_ENDPOINT,
         help='API endpoint (default=%s)' %  DEFAULT_API_ENDPOINT)
    _arg('--new', action='store_true', default=False,
         help='Create the dataset')
    _arg('--token', action='store', required=True, help='API token')
    _arg('dataset', type=str, action='store', metavar='dataset',
         help='Base64 encoded dataset id or username/dataset-name')
    args = parser.parse_args()

    credentials = {}
    for k, v in TWITTER_ENV_CREDENTIALS.items():
        credentials[k] = os.environ[v]

    api = twitter.Api(**credentials)
    stream_sample = api.GetStreamSample()

    dataset_id = None
    if args.new:
        dataset_id = create_dataset(args.endpoint, args.token, args.dataset)
    elif '/' in args.dataset:
        response = requests.get(args.endpoint + '/api/resolve/%s' % args.dataset)
        dataset_id = response.json()['dataset']['id']
    else:
        dataset_id = args.dataset
    assert dataset_id is not None

    headers = {'content-type': 'application/json'}
    records = []
    endpoint = '%s/api/datasets/put' % args.endpoint
    start = time.time()
    for tweet in stream_sample:
        if 'text' in tweet:
            timestamp = date_parser.parse(tweet['created_at']).strftime('%s')
            timestamp = int(timestamp) % (24 * 3600)
            records.append([timestamp, tweet['lang'], tweet['text']])
        end = time.time()
        if end - start > BATCH_INTERVAL_SECS and len(records) > 0:
            start = end
            payload = {'token': args.token,
                       'dataset_id': dataset_id,
                       'records': records}
            resp = requests.post(
                endpoint, data=json.dumps(payload), headers=headers)
            if resp.status_code != 200:
                print(resp.content)
                print('API responded with: %s' % resp.json())
            else:
                print('[dataset_id=%s] Pushed %d tweets' %
                      (dataset_id, len(records)))
                records = []
