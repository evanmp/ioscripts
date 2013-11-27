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
import csv

try:
    import json
except ImportError:
    import simplejson as json

def getSubKeys(listOfDicts):
    
    subkeys = set()
    for event_dict in listOfDicts:
        if event_dict[u'properties']:
            subkeys.update(set(event_dict[u'properties'].keys()))
        else:
            pass

    return subkeys
 
class Mixpanel(object):

    ENDPOINT = 'http://data.mixpanel.com/api'
    VERSION = '2.0'

    def __init__(self, api_key, api_secret):
        self.api_key = api_key
        self.api_secret = api_secret

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
        print request_url
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

    def export_csv(self, outfile):
        """
        takes the json and returns a csv file
        """

        event_raw = self.data.split('\n')

        '''remove the lost line, which is busted'''
        event_raw.pop()

        # print type(event_raw[0])
        # str  <-- the list elements are strings

        event_list = []
        for event in event_raw:
            event_json = json.loads(event)
            event_list.append(event_json)

        #print type(event_list[0:3])
        #list
        #print type(event_list[0])
        #dict

        subkeys = getSubKeys(event_list)

        #open the file
        # write the header 

        header = ['event']
        for key in subkeys:
            header.append('property_' + key)

        f = csv.writer(open(outfileName, "wb"))
        f.writerow(header)

        #write the data
        topkeys  = [u'event', u'properties']    

        for event in event_list:
            line = []
            for key in topkeys:
                try:
                    line.append(event[key])
                except:
                    line.append("")

                for subkey in subkeys:
                    try:
                        line.append(event['properties'][subkey])
                    except:
                        line.append("")
            f.writerow(line)

if __name__ == '__main__':
    mixpanel = Mixpanel(
        api_key = 'dd959fa9489903c4176448019afdbb2f',
        api_secret = 'c0c77677a06b28f39b8a0d79f6bd2de7'
    )
    
    mixpanel.request(['export'], 
        {'to_date' : '2013-11-15',
        'from_date': '2013-11-14'
        })

    if (len(sys.argv) <= 1):
        outfileName = raw_input("What should the outfile name be?" + "\n")
        mixpanel.export_csv(outfileName)
    else:        
        outfileName = sys.argv[1]
        #print "writing to " + outfileName
        mixpanel.export_csv(outfileName)