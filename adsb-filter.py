#!/usr/bin/env python
import json
import os, sys
import argparse
import zipfile
import uuid
import datetime

import requests, geojson

from shapely.geometry import Point, MultiPoint
from clint.textui import progress

parser = argparse.ArgumentParser(description='Download daily flight data from ADSBExchange.org and return a json file with flights within a bounding box')
parser.add_argument('--bounds', dest='bounds', default="bounds.geojson", help='Path to the geojson file that stores the bounds (default: bounds.geojson)')
parser.add_argument('--date', dest='date', default="2017-01-29", help='What date do you want to download? Y-m-d, e.g. 2017-01-29 (default)')
parser.add_argument('--range', dest='days_ahead', default=1, help="If specified, this will download flight data for n days after the specified start date.")

args = parser.parse_args()


class FlightParser(object):
  def __init__(self, args):
    self.bounds = args.bounds
    self.date = args.date
    self.days_ahead = int(args.days_ahead)
    self.bounds_fp = geojson.load(open(args.bounds))
    self.coords = self.bounds_fp['features'][0]['geometry']['coordinates'][0]
    self.shape = MultiPoint(self.coords).convex_hull
    self.out_tmpfile = "%s.json" % str(uuid.uuid4())
    self.inside = []

  def run(self):
    if self.days_ahead == 1:
      self.parse(self.date)
    else:
      year, month, day = self.date.split('-')
      for date in self.find_dates(int(year), int(month), int(day)):
        self.parse(date)
    with open(self.out_tmpfile, 'w') as f:
      f.write(json.dumps(inside))
    print 'Filtered data is now in %s' % self.out_tmpfile

  def find_dates(self, year, month, day):
    base = datetime.datetime(year, month, day)
    for obj in [base + datetime.timedelta(days=x) for x in range(0, self.days_ahead)]:
      yield obj.strftime('%Y-%m-%d')

  def parse(self, date):
    print 'Downloading http://history.adsbexchange.com/Aircraftlist.json/%s.zip' % date
    temp_file = '/tmp/adsb_%s.zip' % date
    if not os.path.exists(temp_file):
      r = requests.get('http://history.adsbexchange.com/Aircraftlist.json/%s.zip' % date, stream=True)
      with open(temp_file, 'wb') as f:
        total_length = int(r.headers.get('content-length'))
        for chunk in progress.bar(r.iter_content(chunk_size=1024), expected_size=(total_length/1024) + 1): 
          if chunk:
            f.write(chunk)
            f.flush()
    self.filter(temp_file, date)

  def filter(self, f, date):
    out_tmpfile = 'filtered_%s.json' % date
    zf = zipfile.ZipFile(f)
    for info in zf.infolist():
      try:
        data = json.loads(zf.read(info.filename))
        print "Working on", info.filename
        for sample in data['acList']:
          if 'Lat' in sample and 'Long' in sample and 'Icao' in sample:
            point = Point([sample['Long'], sample['Lat']])
            if self.shape.contains(point):
              self.inside.append(sample)    
      except Exception as e:
        print "Could not read %s from ZIP archive" % info.filename
        print "Error:", e

    os.remove(f)


p = FlightParser(args)
p.run()