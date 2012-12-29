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
try:
    import simplejson as json
except ImportError:
    import json


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
        elif value_type == "json":
            return json.loads(value)
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


class Alter(object):

    def make_alter_set(self, obj_value, alter_value, alter_info):
        return alter_value

    def make_alter_replace(self, obj_value, alter_value, alter_info):
        try:
            is_string = isinstance(obj_value, basestring)
        except NameError:
            is_string = isinstance(obj_value, str)

        replacement = alter_info.get('replacement', '')
        replacement = self.convert(
            replacement,
            alter_info.get("type", None),
            alter_info.get("format", None)
        )
        if is_string:
            return obj_value.replace(alter_value, replacement)
        elif isinstance(obj_value, list):
            return [x if x != alter_value else replacement for x in obj_value]
        return obj_value

    def make_alter_append(self, obj_value, alter_value, alter_info):
        try:
            is_string = isinstance(obj_value, basestring)
        except NameError:
            is_string = isinstance(obj_value, str)
        if is_string:
            return obj_value + alter_value
        elif isinstance(obj_value, list):
            obj_value.append(alter_value)
            return obj_value
        return obj_value

    def make_alter_prepend(self, obj_value, alter_value, alter_info):
        try:
            is_string = isinstance(obj_value, basestring)
        except NameError:
            is_string = isinstance(obj_value, str)
        if is_string:
            return alter_value + obj_value
        elif isinstance(obj_value, list):
            obj_value.insert(0, alter_value)
            return obj_value
        return obj_value

    def make_alter_incr(self, obj_value, alter_value, alter_info):
        return obj_value + alter_value


class Pipe(Match, Alter, Converter):
    """
    This is a basic Pipe class for dictionaries.
    """
    def __init__(self, pipes=[]):
        self.pipes = []
        for pipe in pipes:
            pipe = self.load_pipe(pipe)
            priority = pipe.get('priority', 0)
            heappush(self.pipes, (priority, pipe))

    def load_pipe(self, pipe):
        """
        Pipe can be json string or python dictionary.
        """
        if isinstance(pipe, dict):
            return pipe
        else:
            return json.loads(pipe)

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
        pipe = self.load_pipe(pipe)
        match_section = pipe.get("match", None)
        if not match_section:
            # we have no match conditions, so just return object back.
            return obj

        # get general logic operator for all keys(attributes).
        mode = pipe.get("mode", Logic.AND)

        is_matched = self.check_match(obj, match_section, mode=mode)
        if not is_matched:
            # no match with object. We do not need change it, so just return it
            # back.
            return obj

        # object matched! go to next stage - modify it.
        return self.alter(obj, pipe)

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
            condition_value = self.convert(
                condition_value, value_type, value_format)
            # check if object value matches this condition
            is_matched = self.check_condition(
                operator, object_value, condition_value)
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

    def alter(self, obj, pipe):
        """
        This method returns modified object according to "alter"
        section of given pipe. This section can contain different
        operators: "set", "drop", "replace" etc..
        """
        alter_section = pipe.get("alter", None)
        if not alter_section or not isinstance(alter_section, dict):
            return obj
        drop = alter_section.get("drop", None)
        if drop:
            obj = self.alter_delete(obj, drop)
            del alter_section['drop']
        if obj:
            obj = self.apply_operators(obj, alter_section)
        return obj

    def apply_operators(self, obj, section):
        """
        For every operator in alter section apply it to
        keys(attributes) of object.
        """
        for operator, key_section in pyiteritems(section):
            for key, alter_info in pyiteritems(key_section):
                obj_value = self.get_object_value(obj, key)
                new_value = self.alter_value(operator, obj_value, alter_info)
                self.set_object_key(obj, key, new_value)
        return obj

    def alter_value(self, operator, obj_value, alter_info):
        """
        Change object key(attribute) value according to operator and
        alter information provided.
        """
        method = getattr(self, "make_alter_%s" % operator)
        value = alter_info.get('value', None)
        value_type = alter_info.get('type', None)
        value_format = alter_info.get('format', None)
        alter_value = self.convert(
            value,
            value_type,
            value_format    
        )
        altered_value = method(obj_value, alter_value, alter_info)
        return altered_value

    def alter_delete(self, obj, section):
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


class ObjectPipe(Pipe):
    """
    class to work with class instances, not dictionaries
    """
    def get_object_value(self, obj, key):
        """
        Get attribute.
        """
        return getattr(obj, key, None)

    def del_object_key(self, obj, key):
        """
        Delete attribute.
        """
        try:
            delattr(obj, key)
        except AttributeError:
            pass

    def set_object_key(self, obj, key, value):
        """
        Set attribute.
        """
        setattr(obj, key, value)

    def object_has_key(self, obj, key):
        """
        Test if object has attribute.
        """
        return hasattr(obj, key)


if __name__ == '__main__':
    print('This is a simple example of using Pipe class.')
    data = {
        "hostname": "mail.ru",
        "protocol": "http",
        "resource": "http://mail.ru",
        "status": 1,
        "message": "critical error",
        "repeats": 100
    }
    import pprint
    print('\ninitial data:\n')
    pprint.pprint(data)
    pipe = {
        "priority": 0,  # priority only useful if you use process Pipe method
        "mode": "and",  # logic operator for keys matching
        "match": {  # match section
            "hostname": {
                "mode": "or",  # logic operator for conditions matching
                "conditions": [("endswith", ".ru"), ("endswith", ".com")],
            },
            "protocol": {
                "conditions": [("exact", "http")]
            }
        },
        "alter": {
            "set": {
                "status": {
                    "value": "2",
                    "type": "int",
                }
            },
            "replace": {
                "message": {
                    "value": "critical",
                    "replacement": "info"
                }
            }
        }
    }

    p = Pipe()
    modified_data = p.apply(data, pipe)
    print('\ndata after applying pipe:\n')
    pprint.pprint(modified_data)

    conclusion = """\nAs you can see we set status key to 2 and 
changed word "critical" with word "info" in message in dictionary - because it
contains hostname which ends with ".ru" or ".com" and its protocol
key has exact value "http"."""
    print(conclusion)
