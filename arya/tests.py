# coding: utf-8
import sys
import os

path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, path)

from arya import Pipe, Logic, ObjectPipe
from unittest import TestCase, main
from copy import copy
from datetime import datetime, date, time, timedelta


class PipeTest(TestCase):

	def setUp(self):

		self.dictionary = {
			"hostname": "mail.ru",
			"protocol": "http",
			"resource": "http://mail.ru",
			"informer": "MAILDAEMON",
			"status": 1,
			"repeats": 100,
			"sms": True,
			"tags": ["http", "alert"]
		}

	def test_check_match_without_logic(self):

		p = Pipe()

		section = {
			"hostname": {
				"conditions": [("exact", "mail.ru"),]
			}
		}
		res = p.check_match(copy(self.dictionary), section)
		self.assertEqual(res, True)

		section = {
			"hostname": {
				"conditions": [("exact", "bail.ru"),]
			}
		}
		res = p.check_match(copy(self.dictionary), section)
		self.assertEqual(res, False)

		section = {
			"hostname": {
				"conditions": [("startswith", "mai"),]
			}
		}
		res = p.check_match(copy(self.dictionary), section)
		self.assertEqual(res, True)

		section = {
			"hostname": {
				"conditions": [("startswith", "bai"),]
			}
		}
		res = p.check_match(copy(self.dictionary), section)
		self.assertEqual(res, False)

		section = {
			"hostname": {
				"conditions": [("endswith", "ru"),]
			}
		}
		res = p.check_match(copy(self.dictionary), section)
		self.assertEqual(res, True)

		section = {
			"hostname": {
				"conditions": [("endswith", "com"),]
			}
		}
		res = p.check_match(copy(self.dictionary), section)
		self.assertEqual(res, False)

		section = {
			"hostname": {
				"conditions": [("regex", "^mail[\.]{1,}ru"),]
			}
		}
		res = p.check_match(copy(self.dictionary), section)
		self.assertEqual(res, True)

		section = {
			"hostname": {
				"conditions": [("regex", "^mail[\-]{1,}ru"),]
			}
		}
		res = p.check_match(copy(self.dictionary), section)
		self.assertEqual(res, False)

		section = {
			"hostname": {
				"conditions": [("contains", "ail.r"),]
			}
		}
		res = p.check_match(copy(self.dictionary), section)
		self.assertEqual(res, True)

		section = {
			"hostname": {
				"conditions": [("contains", "ya.r"),]
			}
		}
		res = p.check_match(copy(self.dictionary), section)
		self.assertEqual(res, False)

		section = {
			"hostname": {
				"conditions": [("iexact", "Mail.ru"),]
			}
		}
		res = p.check_match(copy(self.dictionary), section)
		self.assertEqual(res, True)

		section = {
			"hostname": {
				"conditions": [("iexact", "Mail.ru"),]
			}
		}
		res = p.check_match(copy(self.dictionary), section)
		self.assertEqual(res, True)

		section = {
			"repeats": {
				"conditions": [("gt", 50),]
			}
		}
		res = p.check_match(copy(self.dictionary), section)
		self.assertEqual(res, True)

		section = {
			"repeats": {
				"conditions": [("lt", 5000),]
			}
		}
		res = p.check_match(copy(self.dictionary), section)
		self.assertEqual(res, True)

	def test_check_match_with_logic(self):
		p = Pipe()
		section = {
			"hostname": {
				"conditions": [("exact", "mail.ru"),]
			},
			"protocol": {
				"conditions": [("exact", "snmp"),]
			}
		}
		res = p.check_match(copy(self.dictionary), section, mode=Logic.AND)
		self.assertEqual(res, False)

		res = p.check_match(copy(self.dictionary), section, mode=Logic.OR)
		self.assertEqual(res, True)		

	def test_check_key(self):
		p = Pipe()
		section = {
			"conditions": [("startswith", "ma"), ("endswith", "ru")]
		}
		res = p.check_key("mail.ru", section['conditions'], mode=Logic.AND)
		self.assertEqual(res, True)

		section = {
			"conditions": [("startswith", "ma"), ("endswith", "r")]
		}
		res = p.check_key("mail.ru", section['conditions'], mode=Logic.AND)
		self.assertEqual(res, False)

		section = {
			"conditions": [("startswith", "ma"), ("endswith", "r")]
		}
		res = p.check_key("mail.ru", section['conditions'], mode=Logic.OR)
		self.assertEqual(res, True)

		section = {
			"conditions": [("startswith", "a"), ("endswith", "r")]
		}
		res = p.check_key("mail.ru", section['conditions'], mode=Logic.OR)
		self.assertEqual(res, False)

	def test_modify_add(self):
		p = Pipe()
		section = {'step': {'value':1}}
		res = p.modify_add(copy(self.dictionary), section)
		self.assertTrue(res.get('step', None) == 1)

		section = {'time': {'value': "21:40", "type":"time", "format": "%H:%M"}}
		res = p.modify_add(copy(self.dictionary), section)
		self.assertTrue(res.get('time', None) == time(21, 40))

		section = {'time': {'value': "21:40:33", "type":"time", "format": "%H:%M:%S"}}
		res = p.modify_add(copy(self.dictionary), section)
		self.assertTrue(res.get('time', None) == time(21, 40, 33))

		section = {'time': {'value': "2012-12-12 21:40:33", "type":"datetime", "format": "%Y-%m-%d %H:%M:%S"}}
		res = p.modify_add(copy(self.dictionary), section)
		self.assertTrue(res.get('time', None) == datetime(2012, 12, 12, 21, 40, 33))	

		section = {'time': {'value': "2012-12-12", "type":"date", "format": "%Y-%m-%d"}}
		res = p.modify_add(copy(self.dictionary), section)
		self.assertTrue(res.get('time', None) == date(2012, 12, 12))	

		section = {'time': {'value': "1000", "type":"timedelta", "format": "seconds"}}
		res = p.modify_add(copy(self.dictionary), section)
		self.assertTrue(res.get('time', None) == timedelta(seconds=1000))	

	def test_modify_delete(self):
		p = Pipe()
		section = "all"
		res = p.modify_delete(copy(self.dictionary), section)
		self.assertEqual(res, None)

		section = ["hostname", "status"]
		res = p.modify_delete(copy(self.dictionary), section)
		self.assertEqual(
			[res.get('hostname', None), res.get('status', None)],
			[None, None]
		)

	def test_modify_update(self):
		p = Pipe()
		section = {
			"tags": {
				"operator": "append",
				"value": "test"
			}
		}
		res = p.modify_update(copy(self.dictionary), section)
		self.assertTrue("test" in res.get('tags', []))	

		section = {
			"hostname": {
				"value": "example.com"
			}
		}
		res = p.modify_update(copy(self.dictionary), section)
		self.assertTrue(res.get('hostname', None) == "example.com")	

		section = {
			"repeats": {
				"operator": "add",
				"value": "50",
				"type": 'int'
			}
		}
		res = p.modify_update(copy(self.dictionary), section)
		self.assertTrue(res.get('repeats', None) == 150)


class ObjectPipeTest(TestCase):

	def setUp(self):

		class Test(object):
			def __init__(self, hostname, protocol, resource, status):
				self.hostname = hostname
				self.protocol = protocol
				self.resource = resource
				self.status = status

		self.obj = Test("mail.ru", "http", "http://mail.ru", 1)

	def test_check_match(self):
		p = ObjectPipe()

		section = {
			"hostname": {
				"conditions": [("exact", "mail.ru"),]
			}
		}
		res = p.check_match(copy(self.obj), section)
		self.assertEqual(res, True)

		section = {
			"hostname": {
				"conditions": [("exact", "bail.ru"),]
			}
		}
		res = p.check_match(copy(self.obj), section)
		self.assertEqual(res, False)

	def test_modify_add(self):
		p = ObjectPipe()
		section = {'step': {'value':1}}
		res = p.modify_add(copy(self.obj), section)
		self.assertTrue(res.step == 1)

		section = {'time': {'value': "21:40", "type":"time", "format": "%H:%M"}}
		res = p.modify_add(copy(self.obj), section)
		self.assertTrue(res.time == time(21, 40))

	def test_modify_delete(self):
		p = ObjectPipe()
		section = "all"
		res = p.modify_delete(copy(self.obj), section)
		self.assertEqual(res, None)
		section = ["hostname", "status"]
		res = p.modify_delete(copy(self.obj), section)
		with self.assertRaises(AttributeError):
			a = res.hostname
		with self.assertRaises(AttributeError):
			a = res.status

	def test_modify_update(self):
		p = ObjectPipe()
		section = {
			"hostname": {
				"value": "example.com"
			}
		}
		res = p.modify_update(copy(self.obj), section)
		self.assertTrue(res.hostname == "example.com")	


if __name__ == '__main__':
    main()
