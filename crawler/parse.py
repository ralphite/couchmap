from BeautifulSoup import BeautifulSoup
import traceback, re

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