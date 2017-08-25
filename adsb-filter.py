#!/usr/bin/env python
import json
import os, sys
import argparse
import zipfile
import uuid

import requests, geojson

from shapely.geometry import Point, MultiPoint
from clint.textui import progress

parser = argparse.ArgumentParser(description='Download daily flight data from ADSBExchange.org and return a json file with flights within a bounding box')
parser.add_argument('--bounds', dest='bounds', default="bounds.geojson", help='Path to the geojson file that stores the bounds (default: bounds.geojson)')
parser.add_argument('--date', dest='date', default="2017-01-01", help='What date do you want to download? E.g. 2017-01-01 (default)')
parser.add_argument('--delete-after', dest="delete_after", action='store_true', help='Use this if you want the script to delete the 10gb-ish file after processing is complete' )

args = parser.parse_args()

bounds_fp = geojson.load(open(args.bounds))

coords = bounds_fp['features'][0]['geometry']['coordinates'][0]
shape = MultiPoint(coords).convex_hull

# Where the zip file is saved
temp_file = '/tmp/adsb_%s.zip' % (args.date)

# Local file pointer for the filtered positions
out_tmpfile = '%s.json' % str(uuid.uuid4())

# Array which holds the flight positions within the bounding box
inside = []


print 'Downloading http://history.adsbexchange.com/Aircraftlist.json/%s.zip' % args.date
if not os.path.exists(temp_file):
  r = requests.get('http://history.adsbexchange.com/Aircraftlist.json/%s.zip' % args.date, stream=True)
  with open(temp_file, 'wb') as f:
    total_length = int(r.headers.get('content-length'))
    for chunk in progress.bar(r.iter_content(chunk_size=1024), expected_size=(total_length/1024) + 1): 
      if chunk:
        f.write(chunk)
        f.flush()

zf = zipfile.ZipFile(temp_file)
for info in zf.infolist():
  try:
    data = json.loads(zf.read(info.filename))
    print "Working on", info.filename
    for sample in data['acList']:
      if 'Lat' in sample and 'Long' in sample and 'Icao' in sample:
        point = Point([sample['Long'], sample['Lat']])
        if shape.contains(point):
          inside.append(sample)    
  except:
    print "Could not read %s from ZIP archive" % info.filename

with open(out_tmpfile, 'w') as f:
  f.write(json.dumps(inside))

print 'Filtered positions are in %s' % out_tmpfile

if args.delete_after:
  os.remove(temp_file)