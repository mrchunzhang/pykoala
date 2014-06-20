#coding: UTF-8


'''
@license: Apache License 2.0
@version: 0.1
@author: 张淳
@contact: mrchunzhang@gmail.com
@date: 2012-08-16
'''


# url地址的默认前缀为http
URL_DEFAULT_SCHEME = 'http://'

# 爬虫的默认user-agent为ie8-win7-32bit
KOALA_USER_AGENT = 'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0)'

# 默认使用的html解析器
DEFAULT_HTML_PARSER = 'html.parser'		# 默认使用py内置解析器，可以手动指定lxml/html5lib等，详见http://www.crummy.com/software/BeautifulSoup/bs4/doc/#installing-a-parser

# 网络出错的等待/重试
NETWORK_ERROR_MAX_RETRY_TIMES = 0		# 最大重试次数
NETWORK_ERROR_WAIT_SECOND = 10			# 重试前等待秒数
