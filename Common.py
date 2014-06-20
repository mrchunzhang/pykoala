#coding: UTF-8


'''
@license: Apache License 2.0
@version: 0.1
@author: 张淳
@contact: mrchunzhang@gmail.com
@date: 2012-08-16
'''


from __future__ import print_function
from bs4 import UnicodeDammit
import re
import sys
import hashlib
import datetime


# UTF-8字符集标准命名
UTF8_CHARSET_NAME = 'UTF-8'


def hash(text):
	try:
		return hashlib.sha1(unicode_to(text, UTF8_CHARSET_NAME)).hexdigest()
	except TypeError:
		return hashlib.sha1(text).hexdigest()


def to_unicode(byteSequence):
	# 如果已经是unicode则直接返回
	if isinstance(byteSequence, unicode):
		return byteSequence
	else:
		# 尝试从中查找charset的html文本，针对html文本的unicode转换可大幅加速
		charsetPattern = r'''charset\s*=\s*['"]?([-\w\d]+)['"]?'''
		find = re.search(charsetPattern, byteSequence, re.I)
		if find:
			if find.group(1):
				try:
					return byteSequence.decode(find.group(1), 'ignore')
				except Exception as error:
					write_stderr(repr(error))
		# 上述方法均不成功，则使用bs4内置的unicode转换装置
		dammit = UnicodeDammit(byteSequence)
		return dammit.unicode_markup


def unicode_to(unicodeString, charset):
	if not isinstance(unicodeString, unicode):
		raise TypeError('Parameter "unicodeString" is not unicode type')
	else:
		return unicodeString.encode(charset, 'ignore')


def write_stdout(text):
	print(text, file=sys.stdout)


def write_stderr(text):
	print(text, file=sys.stderr)
