import subprocess
import sys
import argparse
import glob


# install required packages
subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])

import pandas as pd
import requests

parser = argparse.ArgumentParser(description="Index CSV files as documents in opensearch")
parser.add_argument('path', type=str, help='directory containing csv files')
parser.add_argument('url', type=str, help='url to POST to')
args = parser.parse_args()

files = glob.glob(parser.path + '/*.csv')

dataframes = []

for path in files:
    df = pd.read_csv(path).rename(columns={'norm_value': os.path.basename(path)})
    df['long'] = df['long'].round(4)
    df['lat'] = df['lat'].round(4)
    df = df.set_index(['long', 'lat'])
    dataframes.append(df)

frame = pd.concat(dataframes, axis=1).fillna(0)

input()
