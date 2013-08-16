#!/usr/bin/env python

import sys, datetime, urllib2, re, traceback
from BeautifulSoup import BeautifulSoup
from sqlalchemy import *

# Settings
# start point on cs website to crawl
START_URL = "http://www.couchsurfing.org/people/itim"

# DB
class CrawlDB:
	def __init__(self):
		self.connected = False

	def connect(self):
		self.engine = create_engine('sqlite:///cs.db')
		self.connection = self.engine.connect()
		self.connected = True if self.connection else False
		self.metadata = MetaData()

		# Define the tables
		self.queue_table = Table('queue', self.metadata,
			Column('id', Integer, primary_key = True),
			Column('address', String, nullable = False),
			Column('added', DateTime, nullable = False, default = datetime.datetime.now())
		)
		self.crawl_table = Table('crawl', self.metadata,
			Column('id', Integer, primary_key = True),
			Column('address', String, nullable = False),
			Column('http_status', String, nullable = False),
			Column('title', String, nullable = True),
			Column('size', Integer, nullable = True),
		)
		self.profile_table = Table('profile', self.metadata,
			Column('id', Integer, primary_key = True),
			Column('member_name', String, nullable = True),
			Column('country', String, nullable = True),
			Column('area', String, nullable = True),
			Column('city', String, nullable = True),
			Column('location', String, nullable = True),
			Column('refs_count', Integer, nullable = True),
			Column('friends_count', Integer, nullable = True),
		)

		# Create the tables
		self.metadata.create_all(self.engine)

	def enqueue(self, urls):
		if not self.connected:
			return False
		if len(urls) == 0:
			return True
		args = [{'address':u} for u in urls]
		result = self.connection.execute(self.queue_table.insert(), args)
		if result:
			return True
		return False

	def dequeue(self):
		if not self.connected:
			return False
		# Get the first url in the queue
		s = select([self.queue_table]).limit(1)
		res = self.connection.execute(s)
		result = res.fetchall()
		res.close()
		# If we get a result
		if len(result) > 0:
			# Remove from the queue
			delres = self.connection.execute(self.queue_table.delete().where(
				self.queue_table.c.id == result[0][0]))
			if not delres:
				return False
			# Return the row
			return result[0][1]
		return False

	def isInQueue(self, url):
		s = select([self.queue_table]).where(
			self.queue_table.c.address == url.decode("utf8"))
		result = self.connection.execute(s)
		if len(result.fetchall()) > 0:
			result.close()
			return True
		else:
			result.close()
			return False

	def hasCrawled(self, url):
		s = select([self.crawl_table]).where(
			self.crawl_table.c.address == url.decode("utf8"))
		result = self.connection.execute(s)
		if len(result.fetchall()) > 0:
			result.close()
			return True
		else:
			result.close()
			return False

	def addProfile(self, data):
		if not self.connected:
			return False
		# Add the page to the crawl table
		try:
			result = self.connection.execute(self.crawl_table.insert().values(
				address = unicode(data['address']),
				http_status = data['http_status'],
				title = unicode(data['title']),
				size = data['size']))
		except UnicodeDecodeError:
			return False
		if not result:
			return False
		# Add profile details
		try:
			self.connection.execute(self.profile_table.insert().values(
				member_name = data['member_name'],
				country = data['country'],
				area = data['area'],
				city = data['city'],
				location = data['location'],
				refs_count = data['refs_count'],
				friends_count = data['friends_count']
				))
		except:
			return False

		return True

	def close(self):
		self.connection.close()

# Page Parsing
# should return data =
# {
#   title: string, title of the page
#   size: int, size of the html
#   country: string,
#   area: string,
#   city: string,
#   location: string,
#   refs_count: int,
#   friends_count: int
#	urls: [], list of urls of profile page
# }
def parseHtml(html):
	bs = BeautifulSoup(html)
	data = {}

	try:
		if bs.findAll("td")[0].text.startswith("This member has chosen to show"):
			print "--- Profile not allowed to view without login ---"
			return {"title": '', "size": 0, "country":"", "area": "", "city":"", \
			"location":"", "refs_count":0, "friends_count":0, "urls":[]}

		data['title'] = bs.title.text
		# size is the length of the html string
		data['size'] = len(str(bs))
		cac = re.sub(r'<[^>]*>', ':', str(bs.findAll('table')[2].findAll('tr')[0].td.a))
		temparr = cac.strip().split(':')
		if temparr[0] == '':
			temparr = temparr[1:]
		if temparr[-1] == '':
			temparr = temparr[:-1]
		data['country'], data['area'], data['city'] = temparr
		data['location'] = bs.findAll('table')[2].findAll('tr')[0].td.a.attrs[2][1].split('@')[1]
		data['refs_count'] = bs.find(id='total_ref').span.text.split('(')[1].split(')')[0]
		data['friends_count'] = bs.find(id='friends').text.split('(')[1].split(')')[0]
		r = re.compile(r'href="/people/([^/]*)/')
		a = r.findall(str(bs))
		a = getUniqArray(a)
		data['urls'] = ["http://www.couchsurfing.org/people/" + i for i in a]
		return data
	except Exception:
		traceback.print_exc()
		return None

def getUniqArray(seq):
    seen = set()
    seen_add = seen.add
    return [ x for x in seq if x not in seen and not seen_add(x)]

# Crawler
def crawl(start_url):
	cdb = CrawlDB()
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
				(not cdb.hasCrawled(u)) and u != START_URL])

			cdb.addProfile(data)
		except Exception:
			traceback.print_exc()
			continue

if __name__ == '__main__':
	try:
		crawl(START_URL)
	except KeyboardInterrupt:
		sys.exit()
	except Exception:
		traceback.print_exc()