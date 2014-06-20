#coding: UTF-8


'''
@license: Apache License 2.0
@version: 0.1
@author: 张淳
@contact: mrchunzhang@gmail.com
@date: 2012-08-16
'''


from bs4 import BeautifulSoup
import re
import uuid
import time
import socket
import urlparse
import requests
import tldextract
import Common
import Config


class KoalaStatus(object):
	# 默认使用的数据库名
	DB_NAME 					= 'koalastatus'

	# 通用字段定义
	DOC_FIELD_DEFAULT_ID		= '_id'
	# NextEntry类型字段定义
	DOC_FIELD_HASH 				= 'Hash'
	DOC_FIELD_URL 				= 'URL'

	def __init__(self, webSiteID):
		if not webSiteID:
			raise ValueError('You must specified "webSiteID" parameter in constructor')

		self.conn = pymongo.Connection()

		# NextEntry集合
		self.collNextEntry = self.conn[KoalaStatus.DB_NAME]['%s_nextentry' % webSiteID]
		# 为NextEntry集合的Hash键创建唯一索引
		self.collNextEntry.ensure_index(KoalaStatus.DOC_FIELD_HASH, unique=True, dropDups=True)

	def __del__(self):
		self.conn.close()

	def is_have_next_entry(self):
		if self.collNextEntry.count():
			return True
		else:
			return False

	def get_all_next_entry(self):
		return \
		[
		item[KoalaStatus.DOC_FIELD_URL] for item in \
			self.collNextEntry.find(fields = {
				KoalaStatus.DOC_FIELD_URL: 1,
				KoalaStatus.DOC_FIELD_DEFAULT_ID: 0,
			})
		]

	def add_next_entry(self, nextEntries):
		for entryURL in nextEntries:
			doc = dict()
			doc[KoalaStatus.DOC_FIELD_HASH] 	= Common.hash(entryURL)
			doc[KoalaStatus.DOC_FIELD_URL] 		= entryURL
			try:
				self.collNextEntry.insert(doc, safe=True)
			except pymongo.errors.DuplicateKeyError as error:
				pass

	def remove_next_entry(self, nextEntries):
		for entryURL in nextEntries:
			self.collNextEntry.remove({KoalaStatus.DOC_FIELD_HASH: Common.hash(entryURL)}, safe=True)


class Koala(object):
	def __init__(self, webSiteURL, entryFilter=None, yieldFilter=None, identifier=None, enableStatusSupport=False,
				custom_url_source_proc=lambda url, sourceSoup: None):
		if not webSiteURL:
			raise ValueError('You must specified "webSiteURL" parameter in constructor')

		webSiteURL = Common.to_unicode(webSiteURL)

		# 如果url没有协议前缀，则使用默认协议前缀
		webSiteURL = ensure_url_default_scheme(webSiteURL)

		self.domain 		= get_domain(webSiteURL)
		self.webSiteURL 	= webSiteURL
		self.entryFilter 	= entryFilter
		self.yieldFilter 	= yieldFilter

		# 如果没有指定id，则生成uuid
		if not identifier:
			self.identifier = str(uuid.uuid1())
		else:
			self.identifier = identifier

		# 是否启用状态支持的标记
		if not enableStatusSupport:
			self.koalaStatus = None
		else:
			global pymongo
			import pymongo
			self.koalaStatus = KoalaStatus(Common.hash(self.webSiteURL))

		# 保存url源代码处理回调函数
		self.custom_url_source_proc = custom_url_source_proc

		# 记录访问过的页面
		self.visitedEntriesHash = set()

	def get_id(self):
		return self.identifier

	def go(self, maxDepth=10):
		# 恢复状态执行
		if self.koalaStatus:
			if self.koalaStatus.is_have_next_entry():
				nextEntries = self.koalaStatus.get_all_next_entry()
				for entryURL in nextEntries:
					for url in self.__crawl_proc(entryURL, maxDepth):
						yield url
				return

		# 全新执行
		for url in self.__crawl_proc(self.webSiteURL, maxDepth):
			yield url

	def __crawl_proc(self, entryURL, maxDepth):
		# 如果达到最大深度则返回
		if maxDepth <= 0:
			return

		# 得到url源代码并构建BeautifulSoup对象
		try:
			source = get_url_html(entryURL)
			soup = BeautifulSoup(source, Config.DEFAULT_HTML_PARSER)
		except Exception as error:
			Common.write_stderr(repr(error))
			return
		# 调用url源代码处理回调函数并忽略错误
		try:
			self.custom_url_source_proc(entryURL, soup)
		except:
			pass
		# 提取所有a标签的href属性
		links = list()
		for elemA in soup.find_all('a'):
			try:
				links.append(elemA['href'])
			except KeyError as error:
				pass

		# 生成符合规则的链接，并记录符合规则的子页面
		nextEntries = list()
		for link in links:
			url = urlparse.urljoin(entryURL, link)
			if self.__global_filter(entryURL, url):
				if self.__yield_filter(url):
					yield url
				if self.__entry_filter(url):
					nextEntries.append(url)

		# 执行到此处代表一个（子）页面（EntryURL）处理完成

		# 需要记录到已处理页面集合中。处于性能考虑，记录url的hash值而非url本身
		self.visitedEntriesHash.add(Common.hash(entryURL))

		# 如果启用状态支持，则同步删除数据库中对应的NextEntry数据（如果有的话）
		if self.koalaStatus:
			self.koalaStatus.remove_next_entry([entryURL])

		# 如果即将达到最大深度，处于性能考虑，不再进入子页面
		if maxDepth - 1 <= 0:
			return
		else:
			# 准备进入子页面之前，同步更新状态
			if self.koalaStatus:
				self.koalaStatus.add_next_entry(nextEntries)

			# 广度优先抓取
			for nextEntryURL in nextEntries:
				if Common.hash(nextEntryURL) not in self.visitedEntriesHash:
					for url in self.__crawl_proc(nextEntryURL, maxDepth - 1):
						yield url

	def __global_filter(self, currentEntryURL, checkURL):
		# 不能为非本站的url
		if get_domain(checkURL) != self.domain:
			return False

		# 不能和站点url相同
		if is_two_url_same(self.webSiteURL, checkURL):
			return False

		# 不能和当前正在处理的页面url相同
		if is_two_url_same(currentEntryURL, checkURL):
			return False

		return True

	def __filter(self, whichFilter, checkURL):
		try:
			if re.match(r'allow', whichFilter['Type'], re.IGNORECASE):
				for rule in whichFilter['List']:
					rule = Common.to_unicode(rule)
					if re.search(rule, checkURL, re.IGNORECASE | re.UNICODE):
						return True
				return False
			elif re.match(r'deny', whichFilter['Type'], re.IGNORECASE):
				for rule in whichFilter['List']:
					rule = Common.to_unicode(rule)
					if re.search(rule, checkURL, re.IGNORECASE | re.UNICODE):
						return False
				return True
			else:
				raise
		except:
			return True

	def __entry_filter(self, checkURL):
		return self.__filter(self.entryFilter, checkURL)

	def __yield_filter(self, checkURL):
		return self.__filter(self.yieldFilter, checkURL)


def get_url_html(url, **customHeader):
	# 默认只包含user-agent
	if 'User-Agent' not in customHeader:
		customHeader['User-Agent'] = Config.KOALA_USER_AGENT

	# 网络出错重试机制
	retryTimes = 0
	while True:
		try:
			# 先发送head做检测工作
			rsp = requests.head(url, headers=customHeader)
			if not rsp.ok:
				rsp.raise_for_status()
			if not rsp.headers['content-type'].startswith('text/html'):
				raise TypeError('Specified url does not return HTML file')

			rsp = requests.get(url, headers=customHeader)
			return Common.to_unicode(rsp.content)
		except requests.exceptions.RequestException as error:
			if retryTimes < Config.NETWORK_ERROR_MAX_RETRY_TIMES:
				retryTimes += 1
				Common.write_stderr('Retry times: %d ' % retryTimes + repr(error))
				time.sleep(Config.NETWORK_ERROR_WAIT_SECOND)
			else:
				raise


def download(url, saveToFile, **customHeader):
	if 'User-Agent' not in customHeader:
		customHeader['User-Agent'] = Config.KOALA_USER_AGENT

	rsp = requests.get(url, headers=customHeader, stream=True)
	if not rsp.ok:
		rsp.raise_for_status()
	with open(saveToFile, 'wb') as f:
		f.write(rsp.raw.read())


def is_two_url_same(url1, url2):
	pattern = re.compile(r'^[^:]+://', re.I | re.U)
	# 去除scheme前缀
	if pattern.search(url1):
		u1 = pattern.sub('', url1)
	else:
		u1 = url1
	if pattern.search(url2):
		u2 = pattern.sub('', url2)
	else:
		u2 = url2
	# 尾部保持/
	if not u1.endswith('/'):
		u1 += '/'
	if not u2.endswith('/'):
		u2 += '/'

	return u1 == u2


def ensure_url_default_scheme(url):
	# 检查url前缀，如果没有则添加默认前缀
	if not urlparse.urlsplit(url).scheme:
		return Config.URL_DEFAULT_SCHEME + url
	else:
		return url


def get_domain(url):
	tldStruct = tldextract.extract(url)
	if tldStruct.tld:
		domain = tldStruct.domain + '.' + tldStruct.tld
	else:
		domain = tldStruct.domain

	return domain
