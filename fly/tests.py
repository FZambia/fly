# coding: utf-8
import sys
import os

path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, path)

from fly import Pipe, Logic, ObjectPipe, Match, Alter, Converter
from unittest import TestCase, main
from copy import copy
from datetime import datetime, date, time, timedelta
try:
    import simplejson as json
except ImportError:
    import json


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

    def test_match_functions(self):

        m = Match()

        self.assertTrue(m.make_match_exact('test', 'test'))

        self.assertTrue(m.make_match_iexact('tEst', 'test'))

        self.assertTrue(m.make_match_startswith('test', 'te'))

        self.assertTrue(m.make_match_istartswith('test', 'Te'))

        self.assertTrue(m.make_match_endswith('test', 'st'))

        self.assertTrue(m.make_match_iendswith('tesT', 'St'))

        self.assertTrue(m.make_match_contains('test', 'es'))

        self.assertTrue(m.make_match_icontains('tEst', 'eS'))

        self.assertTrue(m.make_match_ne('test', 'rest'))

        self.assertTrue(m.make_match_gt(10, 5))

        self.assertTrue(m.make_match_lt(1, 5))

        self.assertTrue(m.make_match_gte(10, 10))

        self.assertTrue(m.make_match_lte(10, 10))


    def test_alter_functions(self):

        class Test(Alter, Converter):
            pass

        a = Test()

        self.assertEqual('test', a.make_alter_set('test2', 'test', {}))

        self.assertEqual(
            'I love python',
            a.make_alter_replace(
                'I love ruby',
                'ruby',
                {'replacement': 'python'}
            )
        )

        self.assertEqual([1, 2], a.make_alter_append([1], 2, {}))
        self.assertEqual('ab', a.make_alter_append('a', 'b', {}))

        self.assertEqual([1, 2], a.make_alter_prepend([2], 1, {}))
        self.assertEqual('ab', a.make_alter_prepend('b', 'a', {}))

        self.assertEqual(100, a.make_alter_incr(20, 80, {}))

    def test_check_match_without_logic(self):

        p = Pipe()

        section = {
            "hostname": {
                "conditions": [("exact", "mail.ru"), ]
            }
        }
        res = p.check_match(copy(self.dictionary), section)
        self.assertEqual(res, True)

        section = {
            "hostname": {
                "conditions": [("exact", "bail.ru"), ]
            }
        }
        res = p.check_match(copy(self.dictionary), section)
        self.assertEqual(res, False)

        section = {
            "hostname": {
                "conditions": [("startswith", "mai"), ]
            }
        }
        res = p.check_match(copy(self.dictionary), section)
        self.assertEqual(res, True)

        section = {
            "hostname": {
                "conditions": [("startswith", "bai"), ]
            }
        }
        res = p.check_match(copy(self.dictionary), section)
        self.assertEqual(res, False)

        section = {
            "hostname": {
                "conditions": [("endswith", "ru"), ]
            }
        }
        res = p.check_match(copy(self.dictionary), section)
        self.assertEqual(res, True)

        section = {
            "hostname": {
                "conditions": [("endswith", "com"), ]
            }
        }
        res = p.check_match(copy(self.dictionary), section)
        self.assertEqual(res, False)

        section = {
            "hostname": {
                "conditions": [("regex", "^mail[\.]{1,}ru"), ]
            }
        }
        res = p.check_match(copy(self.dictionary), section)
        self.assertEqual(res, True)

        section = {
            "hostname": {
                "conditions": [("regex", "^mail[\-]{1,}ru"), ]
            }
        }
        res = p.check_match(copy(self.dictionary), section)
        self.assertEqual(res, False)

        section = {
            "hostname": {
                "conditions": [("contains", "ail.r"), ]
            }
        }
        res = p.check_match(copy(self.dictionary), section)
        self.assertEqual(res, True)

        section = {
            "hostname": {
                "conditions": [("contains", "ya.r"), ]
            }
        }
        res = p.check_match(copy(self.dictionary), section)
        self.assertEqual(res, False)

        section = {
            "hostname": {
                "conditions": [("iexact", "Mail.ru"), ]
            }
        }
        res = p.check_match(copy(self.dictionary), section)
        self.assertEqual(res, True)

        section = {
            "hostname": {
            "conditions": [("iexact", "Mail.ru"), ]
            }
        }
        res = p.check_match(copy(self.dictionary), section)
        self.assertEqual(res, True)

        section = {
            "repeats": {
                "conditions": [("gt", 50), ]
            }
        }
        res = p.check_match(copy(self.dictionary), section)
        self.assertEqual(res, True)

        section = {
            "repeats": {
                "conditions": [("lt", 5000), ]
            }
        }
        res = p.check_match(copy(self.dictionary), section)
        self.assertEqual(res, True)

    def test_check_match_with_logic(self):
        p = Pipe()
        section = {
            "hostname": {
                "conditions": [("exact", "mail.ru"), ]
            },
            "protocol": {
                "conditions": [("exact", "snmp"), ]
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

    def test_alter_delete(self):
        p = Pipe()
        section = "all"
        res = p.alter_delete(copy(self.dictionary), section)
        self.assertEqual(res, None)

        section = ["hostname", "status"]
        res = p.alter_delete(copy(self.dictionary), section)
        self.assertEqual(
            [res.get('hostname', None), res.get('status', None)],
            [None, None]
        )

    def test_apply_with_dict_pipe(self):
        pipe = {
            "match": {
                "hostname": {
                    "conditions": [("iexact", "mail.ru")]
                }
            },
            "alter": {
                "drop": "ALL"
            }
        }
        p = Pipe()
        res = p.apply(copy(self.dictionary), pipe)
        self.assertEqual(res, None)

    def test_apply_with_json_pipe(self):
        pipe = {
            "match": {
                "hostname": {
                    "conditions": [("iexact", "mail.ru")]
                }
            },
            "alter": {
                "drop": "ALL"
            }
        }
        pipe = json.dumps(pipe)
        p = Pipe()
        res = p.apply(copy(self.dictionary), pipe)
        self.assertEqual(res, None)


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
                "conditions": [("exact", "mail.ru"), ]
            }
        }
        res = p.check_match(copy(self.obj), section)
        self.assertEqual(res, True)

        section = {
            "hostname": {
                "conditions": [("exact", "bail.ru"), ]
            }
        }
        res = p.check_match(copy(self.obj), section)
        self.assertEqual(res, False)

    def test_alter_delete(self):
        p = ObjectPipe()
        section = "all"
        res = p.alter_delete(copy(self.obj), section)
        self.assertEqual(res, None)
        section = ["hostname", "status"]
        res = p.alter_delete(copy(self.obj), section)
        self.assertFalse(hasattr(res, 'hostname'))
        self.assertFalse(hasattr(res, 'status'))


if __name__ == '__main__':
    main()
