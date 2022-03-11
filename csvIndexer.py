import subprocess
import sys
import argparse

parser = argparse.ArgumentParser(description="Index CSV files as documents in opensearch")
parser.add_argument('index', type=str, help='name of desired index')
parser.add_argument('path', type=str, help='directory containing csv files')
parser.add_argument('url', type=str, help='url to POST to')
parser.add_argument('-b', dest='batches', type=int, default=-1, help='number of batches to index (default: all)')
parser.add_argument('-s', dest='size', type=int, default=500, help='batch size (default: 500 documents)')
args = parser.parse_args()

# install required packages
subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])

import gc
import glob
import os
import hashlib
import warnings
import requests
import numpy as np
import pandas as pd

from json.encoder import JSONEncoder

files = glob.glob(args.path + '/*.csv')

documents = pd.DataFrame()

for path in files:
    print('reading csv: ' + os.path.basename(path))
    df = pd.read_csv(path).rename(columns={'norm_value': os.path.splitext(os.path.basename(path))[0]})
    df['long'] = df['long'].round(4)
    df['lat'] = df['lat'].round(4)
    df = df.set_index(['long', 'lat'])
    documents = pd.concat([documents, df], axis=1).fillna(0)

    # collect garbage
    del df
    gc.collect()

print('formatting points')
# format coords as point
points = [point for point in map(lambda p: list(p), documents.index.tolist())]
documents['location'] = points

print('generating ids')
# hash points as doc ids
ids = [id_ for id_ in
       map(lambda coordinate: hashlib.md5(
           str(str(coordinate[0]) + ', ' + str(coordinate[1])).encode('utf8')).hexdigest(), points)]
documents['id'] = ids

# collect garbage
del points
del ids
gc.collect()

documents = documents.set_index(['id'])

# if previous file exists clean up
if os.path.exists('temp_request_body'):
    os.remove('temp_request_body')

last_index = 0
next_index = args.size

if args.size < 0:
    print('calculating batches')
    num_batches = int(np.ceil(documents.shape[0] / args.size))

for batch in range(1, args.batches + 1):
    print(f'indexing batch {batch}/{args.batches}', end='\r')
    request_body = open('temp_request_body', 'a')
    json_documents = [doc for doc in map(lambda series: series.to_json(), documents[last_index:next_index].iloc)]

    for id, doc in zip(documents.index, json_documents):
        request_body.write(str(JSONEncoder().encode({"index": {"_index": args.index, "_id": id},  "mappings": {"properties": {"location": {"type": "geo_point"}}}}) + '\n' + doc + '\n'))
    request_body.close()

    # cleanup memory
    del json_documents
    gc.collect()

    # post to server
    url = args.url + ':9200/' + args.index + '/_bulk'
    headers = {'Content-Type': 'application/json'}
    data = open('temp_request_body', 'rb').read()

    warnings.filterwarnings("ignore")
    response = requests.post(url=url, headers=headers, data=data, verify=False, auth=('admin', 'admin'))
    warnings.filterwarnings("default")

    if not response.ok:
        raise RuntimeError('The file was unable to upload successfully got status code: ' + str(
            response) + '\n' + 'Its possible the command might be formatted incorrectly see: "py ./csvIndexer -h"')

    # clean up request body
    os.remove('temp_request_body')

    # select next batch
    last_index = next_index
    next_index += last_index

print('--------Done!--------')
