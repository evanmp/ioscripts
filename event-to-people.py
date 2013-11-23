'''

step #1 get all events of a name
 (1B get event name from sys args)

step #2 get all distinct ids from these events with the max date

Step #3 get all people profiles -> text file

Step #4 read into memory the text file of people profiles

Step #5 create a list of intersecting distinct ids between events and people profiles

Step #6 for each distinct_id, $set( EVENT_NAME: date ) with $ignore_time = TRUE

'''

#! /usr/bin/env python
#
# Mixpanel, Inc. -- http://mixpanel.com/
#
# Python API client library to consume mixpanel.com analytics data.
#
# Copyright 2010-2013 Mixpanel, Inc
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# See the License for the specific language governing permissions and
# limitations under the License.

import hashlib
import urllib
import time
import sys

try:
    import json
except ImportError:
    import simplejson as json


def getDistinctIds(jsonList):    
	'''this is very likely to fail for properties given the $ in people exports'''
	ids = []
	for element in jsonList:
		properties = element['properties']
		ids.append(properties['distinct_id'])
	'''make the id list unique by casting it into a set'''		
	ids = set(ids)

 
class Mixpanel(object):

    ENDPOINT = 'http://data.mixpanel.com/api'
    VERSION = '2.0'

    def __init__(self, api_key, api_secret, token):
        self.api_key = api_key
        self.api_secret = api_secret
        self.token = token

    def request(self, methods, params, format='json'):
        """
            methods - List of methods to be joined, e.g. ['events', 'properties', 'values']
                      will give us http://mixpanel.com/api/2.0/events/properties/values/
            params - Extra parameters associated with method
        """
        params['api_key'] = self.api_key
        params['expire'] = int(time.time()) + 600   # Grant this request 10 minutes.
        params['format'] = format
        if 'sig' in params: del params['sig']
        params['sig'] = self.hash_args(params)

        request_url = '/'.join([self.ENDPOINT, str(self.VERSION)] + methods) + '/?' + self.unicode_urlencode(params)
        # print request_url
        request = urllib.urlopen(request_url)
        data_output = request.read()    
        self.data = data_output

    def unicode_urlencode(self, params):
        """
            Convert lists to JSON encoded strings, and correctly handle any
            unicode URL parameters.
        """
        if isinstance(params, dict):
            params = params.items()
        for i, param in enumerate(params):
            if isinstance(param[1], list):
                params[i] = (param[0], json.dumps(param[1]),)

        return urllib.urlencode(
            [(k, isinstance(v, unicode) and v.encode('utf-8') or v) for k, v in params]
        )

    def hash_args(self, args, secret=None):
        """
            Hashes arguments by joining key=value pairs, appending a secret, and
            then taking the MD5 hex digest.
        """
        for a in args:
            if isinstance(args[a], list): args[a] = json.dumps(args[a])

        args_joined = ''
        for a in sorted(args.keys()):
            if isinstance(a, unicode):
                args_joined += a.encode('utf-8')
            else:
                args_joined += str(a)

            args_joined += '='

            if isinstance(args[a], unicode):
                args_joined += args[a].encode('utf-8')
            else:
                args_joined += str(args[a])

        hash = hashlib.md5(args_joined)

        if secret:
            hash.update(secret)
        elif self.api_secret:
            hash.update(self.api_secret)
        return hash.hexdigest()

    def data_to_json(self, exportType):       
        """
        takes the data export object and returns a json object
        """

        if exportType == "events":
	        event_raw = self.data.split('\n')
	        '''remove last line'''
	        event_raw.pop()
	        # print type(event_raw)  <type 'list'>

	        # print type(event_raw[0])
	        # str  <-- the list elements are strings

	        event_list = []
	        for event in event_raw:
	            event_json = json.loads(event)
	            event_list.append(event_json)

	        self.events = event_list
		'''
		if exportType == "people":
	    	self.people = "foo"

		return
		'''

if __name__ == '__main__':
    mixpanel = Mixpanel(
        api_key = '3591de50eb56dd8f2c4813c6cef7ff00',
        api_secret = 'ffe503a4dbb621844d2e1f2a7dc1852e',
        token = '7cf84d01db1eab390298ed500bc43610'
    )
    
    mixpanel.request(['export'], 
    	{'event' : ['Landing Page Loaded'],
        'to_date' : '2013-11-22',
        'from_date': '2013-11-15'
    })

    # print mixpanel.data
    # print type(mixpanel.data) # <type 'str'>
    ''' change self.events to json encoding'''
    mixpanel.data_to_json("events")
    ids_events = getDistinctIds(mixpanel.events)

