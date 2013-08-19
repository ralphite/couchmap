#!/usr/bin/env python

import sys, urllib2, re, traceback

from sqlalchemy import *

import settings
from db import CrawlDB
from parse import parseHtml

# Crawler
def crawl(start_url):
	cdb = CrawlDB(settings.DB_FILE)
	cdb.connect()
	cdb.enqueue([start_url])

	while True:
		url = cdb.dequeue()
		if url is False:
			break
		if cdb.hasCrawled(url):
			continue
		print url

		status = 0
		req = urllib2.Request(str(url))
		req.add_header('User-Agent', 'couchmap 0.1')

		request = None

		try:
			request = urllib2.urlopen(req)
		except urllib2.URLError, e:
			continue
		except urllib2.HTTPError, e:
			status = e.code
		if status == 0:
			status = 200
		html = request.read()

		data = parseHtml(html)

		if data is None:
			continue

		try:
			data['address'] = url
			data['http_status'] = status
			data['member_name'] = url.split('/')[-1]

			cdb.enqueue([u for u in data['urls'] if (not cdb.isInQueue(u)) and \
				(not cdb.hasCrawled(u)) and u != settings.START_URL])

			cdb.addProfile(data)
		except Exception:
			traceback.print_exc()
			continue

if __name__ == '__main__':
	try:
		crawl(settings.START_URL)
	except KeyboardInterrupt:
		sys.exit()
	except Exception:
		traceback.print_exc()