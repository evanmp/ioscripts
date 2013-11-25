'''

Step #6 for each distinct_id, pass through the $set: {event_name: date}  //currently true

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
import urllib2 #for sending requests
import time
import sys
import base64

try:
    import json
except ImportError:
    import simplejson as json

def get_options():

	to_date = raw_input("What should the latest date be in YYYY-MM-DD format? (max: yesterday)"+'\n')
	from_date = raw_input("What should the earlist date be in YYYY-MM-DD format?"+'\n')
	event = raw_input("What event do you wish to convert into a People property?"+'\n')
	fname = raw_input("What should the people export file be called?"+'\n')

	api_key = raw_input("Tell me your api_key:"+'\n')
	api_secret = raw_input("Tell me your api_secret"+'\n')
	token = raw_input("Tell me your token"+'\n')

	optionsDict = {'to_date': to_date, 
				'from_date': from_date, 
				'event': event, 
				'fname': fname,
				'api_key': api_key,
				'api_secret': api_secret,
				'token': token
				}

	return optionsDict


def getDistinctIdsEvents(jsonList):    
	'''this gets unique distinctids in the event list'''
	'''to do pass through max dates too'''
	ids = {}
	for element in jsonList:
		properties = element['properties']
		try: 
			ids[properties['distinct_id']] # if distinct_id is already in dict
			if ids[properties['distinct_id']] < properties['time']: #if current time < max time
				ids[properties['distinct_id']] = properties['time']
		except:
			ids[properties['distinct_id']] = properties['time']
	'''make the id list unique by casting it into a set'''
	return ids

def getDistinctIdsPeople(filename):
	'''this is for people updates'''
	ids = []
	with open(filename, 'rb') as f:
		users = f.readlines()
		for user in users:
			ids.append(json.loads(user)['$distinct_id'])
	f.close()

	return set(ids)

class Mixpanel(object):

    ENDPOINT = 'http://data.mixpanel.com/api'
    VERSION = '2.0'

    def __init__(self, api_key, api_secret, token):
        self.api_key = api_key
        self.api_secret = api_secret
        self.token = token

    def event_request(self, methods, params, format='json'):
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

    def people_request(self, params, format = 'json'):
        '''let's craft the http request'''
        params['api_key']=self.api_key
        params['expire'] = int(time.time())+600 # 600 is ten minutes from now
        if 'sig' in params: del params['sig']
        params['sig'] = self.hash_args(params)

        request_url = 'http://mixpanel.com/api/2.0/engage/?' + self.unicode_urlencode(params)

        request = urllib.urlopen(request_url)
        data = request.read()
        #print request_url

        return data

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

    def update(self, userlist, idList, uparams):
    	'''this is for people updates'''
        url = "http://api.mixpanel.com/engage/"
        batch = []
        i = 0
        for user in userlist:
            distinctid = json.loads(user)['$distinct_id']
            if distinctid in idList:
				tempparams = {
					'token':self.token,
					'$distinct_id':distinctid
				}
				tempparams.update(uparams)
				batch.append(tempparams)
				i =+ 1
				
        #print "Updated %s users in this batch" % i
        payload = {"data":base64.b64encode(json.dumps(batch)), "verbose":1,"api_key":self.api_key}

        response = urllib2.urlopen(url, urllib.urlencode(payload))
        message = response.read()

        '''if something goes wrong, this will say what'''
        if json.loads(message)['status'] != 1:
            print message

    def batch_update(self, filename, idList, params):
    	'''this is for people batch updates'''
    	with open(filename,'r') as f:
			users = f.readlines()
        counter = len(users) // 100
        while len(users):
            batch = users[:50]
            self.update(batch, idList, params)
            if len(users) // 100 != counter:
                counter = len(users) // 100
                print "%d users left to check for event and update" % len(users)
            users = users[50:]


if __name__ == '__main__':

    ''' CHANGE THIS TO get_options() if you want command line inputs'''
    options = get_options()
    #options = {'to_date': '2013-10-26', 
	#		    'from_date': '2013-10-26', 
	#		    'event': 'View Item', 
	#		    'fname': 'output_people.txt',
	#		    'api_key': 'c629de7e6c491a1021b3353017647f6a',
	#		    'api_secret': 'bd9773e3650c3b42e2c5b9c1247e2ea9',
	#		    'token': '60c59d9fe30244bd5c56c7d054c83d66'
	#		    }

    mixpanel = Mixpanel(
        api_key = options['api_key'],
        api_secret = options['api_secret'],
        token = options['token']
    )


    '''TO DO CHANGE THIS TO SYS ARG'''  
    mixpanel.event_request(['export'], 
    	{'event' : [options['event']],
        'to_date' : options['to_date'],
        'from_date': options['from_date']
    })

    # print mixpanel.data
    # print type(mixpanel.data) # <type 'str'>
    ''' change self.events to json encoding'''
    mixpanel.data_to_json("events")
    ids_events = getDistinctIdsEvents(mixpanel.events) #this is actually a dict with id: max(date)

    ''' let us get all the people, export them, and read in the data'''
    fname = options['fname']
    '''Here is the place to define your selector to target only the users that you're after'''
    '''parameters = {'selector':'(properties["$email"] == "Albany") or (properties["$city"] == "Alexandria")'}'''
    parameters = {'selector': '(properties["$country_code"] == "VN")'}
    response = mixpanel.people_request(parameters)
    
    parameters['session_id'] = json.loads(response)['session_id']
    parameters['page']=0
    global_total = json.loads(response)['total']
    
    print "Session id is %s \n" % parameters['session_id']
    print "Here are the total # of People Profiles: %d" % global_total
    has_results = True
    total = 0
    with open(fname,'w') as f:
        while has_results:
            responser = json.loads(response)['results']
            total += len(responser)
            has_results = len(responser) == 1000
            for data in responser:
                f.write(json.dumps(data)+'\n')
            print "Downloading %d / %d" % (total,global_total)
            parameters['page'] += 1
            if has_results:
                response = mixpanel.people_request(parameters)
	f.close()

	ids_people = getDistinctIdsPeople(fname)
	ids_common = ids_people.intersection(ids_events.keys())

	dates_common = {}
	for element in ids_common:
		print element
		dates_common[element] = ids_events[element]
	print dates_common
	quit()

	'''TO DO get max date and pass it'''
	mixpanel.batch_update(fname, ids_common, {'$set': {options['event']: 'true'}, '$ignore_time': "true"})

    ####