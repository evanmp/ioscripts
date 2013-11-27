
"""
Usage is incredibly simple:

intitialize first (see line 55)
import_event = EventImporter("7cf84d01db1eab390298ed500bc43610", "3591de50eb56dd8f2c4813c6cef7ff00")
import_event.mpimport("test event", {"time": 1381363200, "distinct_id": "12345"})

convert datetimes into integers as unix timestamps because MixPanel handles them
"""

IMPORT_BASE_URL = "http://api.mixpanel.com/import/?data=%s&api_key=%s"
import urllib2
import json
import base64
import time

class EventImporter(object):
  
  def __init__(self, token, api_key):
    """Create a new event tracker
    :param token: The auth token to use to validate each request
    """
    self.token = token
    self.api_key = api_key

  def mpimport(self, event, properties=None, callback=None):
    """Track a single event
    :param event: The name of the event to track
    :type event: str
    :param properties: An optional dict of properties to describe the event
    :type properties: dict
    :param callback: An optional callback to execute when
      the event has been tracked.
      The callback function should accept two arguments, the event
      and properties, just as they are provided to this function 
      This is mostly used for handling Async operations
    :type callback: function
    """
    
    assert(properties.has_key("time")), "Must specify a backdated time"
    assert(properties.has_key("distinct_id")), "Must specify a distinct ID"
    if "token" not in properties:
      properties["token"] = self.token
    
    params = {"event": event, "properties": properties}
    data = base64.b64encode(json.dumps(params))
    
    resp = urllib2.urlopen(IMPORT_BASE_URL % (data, self.api_key))
    print resp.read()

    if callback is not None:
      callback(event, properties)


''' set you project detail here !!!!!! '''
token = "YOUR TOKEN"
api_key = "YOUR_API_KEY"
import_event = EventImporter(token, api_key)

""" do whatever you need to get your events formatted into a JSON object called archive; for example read them from a file """

f = open("foo.json", "r")
archive = f.readlines()

for event in archive:
  import_event.mpimport(event)