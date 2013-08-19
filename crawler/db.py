from sqlalchemy import *
import datetime

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