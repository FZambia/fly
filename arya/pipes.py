# coding: utf-8
#
# Copyright 2012 Alexandr Emelin
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
from __future__ import print_function
from heapq import heappush, heappop
from datetime import datetime, time, date, timedelta
import re
import sys


# True if we are running on Python 3.
PY3 = sys.version_info[0] == 3


if PY3:
    _iteritems = "items"
else:
    _iteritems = "iteritems"


def pyiteritems(d):
    """Return an iterator over the (key, value) pairs of a dictionary."""
    return iter(getattr(d, _iteritems)())


class Logic(object):

	AND = 'and'
	OR = 'or'


class Converter(object):

	default_format = {
		"datetime": "%Y-%m-%d %H:%M",
		"date": "%Y-%m-%d",
		"time": "%H:%M:%S",
		"timedelta": "seconds"
	}

	default_type = {
		"bool": bool,
		"int": int,
		"float": float,
		"str": str
	}

	def convert(self, value, value_type, value_format):
		"""
		Convert value according to given value_type and value_format.
		"""
		if not value_type:
			return value

		converter = self.default_type.get(value_type, None)
		if converter is not None:
			return converter(value)

		if not value_format:
			try:
				value_format = self.default_format[value_type]
			except KeyError:
				return value

		if value_type in ["datetime", "date", "time"]:
			try:
				is_string = isinstance(value, basestring)
			except NameError:
				is_string = isinstance(value, str)
			if is_string:
				value = datetime.strptime(value, value_format)
			if value_type == "datetime":
				return value
			elif value_type == "date" and isinstance(value, datetime):
				return value.date()
			elif value_type == "time" and isinstance(value, datetime):
				return value.time()
		elif value_type == "timedelta" and type(value).__name__ in ('str', 'int'):
			return timedelta(**{value_format: int(value)})

		return value


class Match(object):

	def make_match_exact(self, obj_value, condition_value):
		return obj_value == condition_value

	def make_match_iexact(self, obj_value, condition_value):
		return obj_value.lower() == condition_value.lower()

	def make_match_regex(self, obj_value, condition_value):
		match = re.match(condition_value, obj_value)
		return hasattr(match, 'group')

	def make_match_startswith(self, obj_value, condition_value):
		return obj_value.startswith(condition_value)

	def make_match_istartswith(self, obj_value, condition_value):
		return obj_value.lower().startswith(condition_value.lower())

	def make_match_endswith(self, obj_value, condition_value):
		return obj_value.endswith(condition_value)

	def make_match_iendswith(self, obj_value, condition_value):
		return obj_value.lower().endswith(condition_value.lower())

	def make_match_contains(self, obj_value, condition_value):
		return obj_value.find(condition_value) > -1

	def make_match_icontains(self, obj_value, condition_value):
		return obj_value.lower().find(condition_value.lower()) > -1

	def make_match_ne(self, obj_value, condition_value):
		return obj_value != condition_value

	def make_match_gt(self, obj_value, condition_value):
		return obj_value > condition_value

	def make_match_lt(self, obj_value, condition_value):
		return obj_value < condition_value

	def make_match_gte(self, obj_value, condition_value):
		return obj_value >= condition_value

	def make_match_lte(self, obj_value, condition_value):
		return obj_value <= condition_value


class Update(object):

	def make_update_set(self, obj_value, update_value):
		return update_value

	def make_update_add(self, obj_value, update_value):
		return obj_value + update_value

	def make_update_sub(self, obj_value, update_value):
		return obj_value - update_value

	def make_update_mul(self, obj_value, update_value):
		return obj_value * update_value

	def make_update_div(self, obj_value, update_value):
		return obj_value / update_value

	def make_update_append(self, obj_value, update_value):
		obj_value.append(update_value)
		return obj_value

	def make_update_prepend(self, obj_value, update_value):
		obj_value.insert(0, update_value)
		return obj_value


class Pipe(Match, Update, Converter):
	"""
	This is a basic Pipe class for dictionaries.
	"""
	def __init__(self, pipes=[]):
		self.pipes = []
		for pipe in pipes:
			priority = pipe.get('priority', 0)
			heappush(self.pipes, (priority, pipe))

	def get_object_value(self, obj, key):
		"""
		This is for object of type dict. This method must be overriden 
		to get attribute value of custom object.
		"""
		return obj.get(key, None)

	def del_object_key(self, obj, key):
		"""
		Delete key from dictionary.
		"""
		try:
			del obj[key]
		except KeyError:
			pass

	def set_object_key(self, obj, key, value):
		"""
		add
		"""
		obj[key] = value

	def object_has_key(self, obj, key):
		"""
		Test if dictionary has key.
		"""
		return key in obj

	def process(self, obj):
		"""
		Pull object through pipes and return it.
		"""
		if obj is None:
			return obj
		if not self.pipes:
			return obj
		for i in xrange(len(self.pipes)):
			_priority, pipe = heappop(self.pipes)
			obj = self.apply(obj, pipe)
			if not obj:
				return None
		return obj

	def apply(self, obj, pipe):
		"""
		Apply pipe for obj.
		This process consists of two stages:
		- matching (make sure that object satisfies conditions in match section of pipe)
		- modifying (update object keys(attributes), add new or delete some of them)
		"""
		match_section = pipe.get("match", None)
		if not match_section:
			# we have no match conditions, so just return object back.
			return obj

		# get general logic operator for all keys(attributes).
		mode = pipe.get("mode", Logic.AND)

		is_matched = self.check_match(obj, match_section, mode=mode)
		if not is_matched:
			# no match with object. We do not need change it, so just return it back.
			return obj

		# object matched! go to next stage - modify it.
		return self.modify(obj, pipe)

	def check_match(self, obj, match_section, mode=Logic.AND):
		"""
		Given an object, match_section, logic mode - what we should do
		is to determine is object matches with conditions from match_section.
		
		match_section is a dictionary which keys are keys(attributes) of object.
		"""
		matches = []

		for key, match_key_section in pyiteritems(match_section):
			# get object key(attribute) value.
			object_value = self.get_object_value(obj, key)
			# get logic operator for key conditions.
			condition_mode = match_key_section.get("mode", Logic.AND)
			# get list of conditions for key to check.
			conditions = match_key_section.get("conditions", [])
			# get value type
			condition_value_type = match_key_section.get("type", None)
			# get value format
			condition_value_format = match_key_section.get("format", None)

			is_matched = self.check_key(
				object_value,
				conditions,
				mode=condition_mode,
				value_type=condition_value_type,
				value_format=condition_value_format
			)
			matches.append(is_matched)

		if mode == Logic.OR:
			return any(matches)
		else:
			return all(matches)

	def check_key(self, object_value, conditions, mode=Logic.AND, value_type=None, value_format=None):
		"""
		Here we determine if object value matches conditions.
		We also have logic operator for conditions, value_type and value_format
		that used to convert value from condition to necessary form to use with
		condition operators.
		"""
		if not isinstance(conditions, list):
			raise TypeError('conditions must be list')

		matches = []
		# convert object value according to format
		object_value = self.convert(object_value, value_type, value_format)

		for operator, condition_value in conditions:
			# convert condition value according to format
			condition_value = self.convert(condition_value, value_type, value_format)
			# check if object value matches this condition
			is_matched = self.check_condition(operator, object_value, condition_value)
			matches.append(is_matched)

		if mode == Logic.OR:
			return any(matches)
		else:
			return all(matches)

	def check_condition(self, operator, object_value, condition_value):
		"""
		Every key can have several conditions object_value should
		satisfy. This is a check of one of those conditions. Just
		run corresponding operator method to make this check and 
		return True or False.
		"""
		method = getattr(self, "make_match_%s" % operator, None)
		if not method:
			raise ValueError('Unsupported operator %s' % operator)
		is_matched = method(object_value, condition_value)
		return is_matched

	def modify(self, obj, pipe):
		"""
		This method returns modified object according to given pipe.
		Pipe can have several sections responsible for object
		modification:
		- delete
		- update
		- add
		"""
		if obj and "delete" in pipe:
			obj = self.modify_delete(obj, pipe["delete"])
		if obj and "update" in pipe:
			obj = self.modify_update(obj, pipe["update"])
		if obj and "add" in pipe:
			obj = self.modify_add(obj, pipe["add"])
		return obj

	def modify_delete(self, obj, section):
		"""
		Delete all this object(i.e. return None) or remove
		custom keys(attributes) from object.
		"""
		if isinstance(section, list):
			for key in section:
				self.del_object_key(obj, key)
			return obj
		else:
			return None

	def modify_add(self, obj, section):
		"""
		Add custom keys(attributes) to object.
		"""
		for key, key_section in pyiteritems(section):
			obj = self.modify_add_key(obj, key, key_section)
		return obj

	def modify_add_key(self, obj, key, key_section):
		"""
		Add key(attribute) to object according to key_section data.
		"""
		if self.object_has_key(obj, key):
			return obj
		value = key_section.get("value", None)
		value_type = key_section.get("type", None)
		value_format = key_section.get("format", None)
		value = self.convert(value, value_type, value_format)
		self.set_object_key(obj, key, value)
		return obj

	def modify_update(self, obj, section):
		"""
		Update object keys(attributes) values.
		"""
		for key, key_section in pyiteritems(section):
			obj = self.modify_update_key(obj, key, key_section)
		return obj

	def modify_update_key(self, obj, key, key_section):
		"""
		Update object's key(attribute) value according to key_section data.
		"""
		if not self.object_has_key(obj, key):
			return obj

		# get object key(attribute) value.
		object_value = self.get_object_value(obj, key)
		# get value type
		update_value_type = key_section.get("type", None)
		# get value format
		update_value_format = key_section.get("format", None)
		# get action operator
		operator = key_section.get("operator", "set")
		# get update value
		update_value = key_section.get("value", None)

		update_value = self.convert(
			update_value,
			update_value_type,
			update_value_format
		)
		method = getattr(self, "make_update_%s" % operator)
		new_value = method(object_value, update_value)
		self.set_object_key(obj, key, new_value)
		return obj


class ObjectPipe(Pipe):
	"""
	class to work with class instances, not dictionaries
	"""
	def get_object_value(self, obj, key):
		"""
		This is for object of type dict. This method must be overriden 
		to get attribute value of custom object.
		"""
		return getattr(obj, key, None)

	def del_object_key(self, obj, key):
		"""
		Delete key from dictionary.
		"""
		try:
			delattr(obj, key)
		except AttributeError:
			pass

	def set_object_key(self, obj, key, value):
		"""
		add
		"""
		setattr(obj, key, value)

	def object_has_key(self, obj, key):
		"""
		Test if dictionary has key.
		"""
		return hasattr(obj, key)


if __name__ == '__main__':
	print('This is a simple example of using Pipe class.')
	data = {
		"hostname": "mail.ru",
		"protocol": "http",
		"resource": "http://mail.ru",
		"status": 1
	}
	import pprint
	print('\ninitial data:\n')
	pprint.pprint(data) 
	pipe = {
		"priority": 0, # priority only useful if you use process Pipe method
		"mode": "and", # logic operator for keys matching
		"match": { # match section
			"hostname": {
				"mode": "or", # logic operator for conditions matching
				"conditions": [("endswith", ".ru"), ("endswith", ".com")],
			},
			"protocol": {
				"conditions": [("exact", "http")]
			}
		},
		"update": { # update section
			"status": {
				"operator": "set",
				"value": "0",
				"type": "int"
			}
		}
	}

	p = Pipe()
	modified_data = p.apply(data, pipe)
	print('\ndata after applying pipe:\n')
	pprint.pprint(modified_data)

	conclusion = """\nAs you can see we set status key to 0 in dictionary - because it 
contains hostname which ends with ".ru" or ".com" and its protocol
key has exact value "http"."""
	print(conclusion)

