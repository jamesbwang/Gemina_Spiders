import scrapy
import os
import scipy.spatial.distance as dist
import re
from selenium import webdriver
from twisted.internet import reactor, defer
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError
from twisted.internet.error import ConnectionLost
from selenium.webdriver.support.ui import WebDriverWait


import constants

# replace the code below with the pathway to the infections folder

d = {}
a = []
d2 = {}
def generatePathogenDictionary():
	# generate the map that will tell the spider where to put the DOI files.
	global d
	for file in os.listdir(constants.newdir):
		f = os.path.join(constants.newdir, file)
		fileName = os.path.join(f, "pubmedAbstract.txt")
		if not f.endswith('.csv'):
			if os.path.isfile(fileName):
				PMIDfile = open(fileName, 'r')
				for url in PMIDfile:
					if url.startswith('DOI: '):
						doiUrl = 'http://doi.org/' + url[5:-2]
						if doiUrl not in d:
							d[doiUrl] = []
						d[doiUrl].append(f)


def checkReference(s, filename):
	# used to find all instances of a given reference in a PMID or DOI file
	s = s.replace('\n', ' ').replace('\t', ' ').replace('.', ' ').replace(',', ' ').replace(';', '').replace(':', '').replace('\n', ' ').replace('\t', ' ').replace('.', '').replace('-', ' ')
	filename = ' '.join((filename.split(' ')))
	reference = 0
	a = s.split(" ")
	for w in a:
		if (len(w) <= 3):
			continue
		if (w.endswith('es')):
			a.append(w[:-2])
		if (w.endswith('s')):
			a.append(w[:-1])
		for name in filename.split(" "):
			if (name == '.txt'):
				continue
			if (len(name) == len(w) and dist.hamming([i for i in w], [j for j in name]) * len(w) <= int(len(w) * .2)):
				reference += 1
	return reference

def getURLList():
	# generate the list of URLS to be crawled by the spider
	urlList = []
	for file in os.listdir(constants.newdir):
		f = os.path.join(constants.newdir, file)
		fileName = os.path.join(f, "pubmedAbstract.txt")
		if os.path.isfile(fileName):
			PMIDfile = open(fileName, 'r')
			for url in PMIDfile:
				if url.startswith('DOI: '):
					doiURL = 'http://doi.org/' + url[5:-2]
					if doiURL not in urlList:
						urlList.append(doiURL)
	return urlList


class DOISpider(scrapy.Spider):
	name = "doi2"
	r = 0


	def __init__(self):
		# use any browser you wish
		scrapy.Spider.__init__(self)
		self.driver = webdriver.Firefox()

	def start_requests(self):
		# map all the url-> file relationships, find all URLs, yield requests for all URLS
		generatePathogenDictionary()
		urls = a
		self.log(str(len(a)) + " doiURLs passed to second spider." )
		for url in urls:
			yield scrapy.Request(url=url, callback= self.parse)
	def parse(self, response):
		# extract all paragraph tags, place the URL in the correct place.
		self.driver.get(response.url)
		self.r = response
		WebDriverWait(self.driver, 60).until(self.process)
	def process(self, driver):
		url = self.r.request.meta['redirect_urls'][0]
		i = 0
		for path in d2[url]:
			while os.path.isfile(os.path.join(path, 'DOI_' + str(i) + '.txt')):
				i += 1
			filename = os.path.join(path, 'DOI_' + str(i) + '.txt')
			arr = self.driver.find_elements_by_tag_name('p')
			for para in arr:
				if os.path.isfile(filename):
					with open(filename, 'a', encoding='utf-8') as f:
						f.write(para.text)
				else:
					with open(filename, 'a', encoding='utf-8') as f:
						f.write(url + '\n')
						f.write(para.text)
			self.log('Saved file: ' + filename)
		return True


class InitialSpider(scrapy.Spider):
	name = "doi"
	http_user = 'someuser'
	http_pass = 'somepass'
	global d2
	global a
	def start_requests(self):
		# map all the url-> file relationships, find all URLs, yield requests for all URLS
		generatePathogenDictionary()
		i = 0
		urls = getURLList()
		for url in urls:
			yield scrapy.Request(url=url, callback=self.parse, errback=self.errback_httpbin, dont_filter=True)

	def errback_httpbin(self, failure):
	# log all errback failures,
	# in case you want to do something special for some errors,
	# you may need the failure's type
		self.logger.error(repr(failure))

	#if isinstance(failure.value, HttpError):
		if failure.check(HttpError):
		# you can get the response
			response = failure.value.response
			self.logger.error('HttpError on %s', response.url)

	#elif isinstance(failure.value, DNSLookupError):
		elif failure.check(DNSLookupError):
		# this is the original request
			request = failure.request
			self.logger.error('DNSLookupError on %s', request.url)

	#elif isinstance(failure.value, TimeoutError):
		elif failure.check(TimeoutError):
			request = failure.request
			self.logger.error('TimeoutError on %s', request.url)
		elif failure.check(ConnectionLost):
			request = failure.request
			self.logger.error('Connection-Lost error on %s', request.url)
		else:
			request = failure.request
			self.logger.error('Unknown error on %s', request.url)
			url = failure.request.meta['redirect_urls'][0]
			a.append(url)
			if url not in d2:
				d2[url] = []
			for path in d[url]:
				d2[url].append(path)


	def parse(self, response):
		# extract all paragraph tags, place the URL in the correct place.

		url = response.request.meta['redirect_urls'][0]
		for path in d[url]:
			i = 0
			while os.path.isfile(os.path.join(path, 'DOI_' + str(i) + '.txt')):
				i += 1
			filename = os.path.join(path, 'DOI_' + str(i) + '.txt')
			for para in response.css('p'):
				cleanr = re.compile('<.*?>')
				cleantext = re.sub(cleanr, '', para.extract())
				with open(filename, 'a', encoding='utf-8') as f:
					f.write(cleantext)
				f.close()
			self.log('Saved file: ' + filename)
			s = path[len(constants.newdir) + 1:]
			s = s.replace('_', ' ')
			s = s.replace(';', ':')
			s = s.replace('!', '?')
			s = s.replace('-', '/')
			b = False
			with open(filename, 'r', encoding = 'utf-8') as r:
				if checkReference(r.read(), s) == 0:
					b = True
					a.append(url)
					if url not in d2:
						d2[url] = []
					d2[url].append(path)
			if b:
				os.remove(filename)
				self.log('Destroyed file: ' + filename)

configure_logging()
runner = CrawlerRunner()

@defer.inlineCallbacks
def crawl():
	yield runner.crawl(InitialSpider)
	yield runner.crawl(DOISpider)
	reactor.stop()

crawl()
reactor.run()
