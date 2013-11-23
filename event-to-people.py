'''

step #1 get all events of a name
1B get event name from sys args

step #2 get all distinct ids from these events with the max date

Step #3 get all people profiles -> text file

Step #4 read into memory the text file of people profiles

Step #5 create a list of intersecting distinct ids between events and people profiles

Step #6 for each distinct_id, $set( EVENT_NAME: date ) with $ignore_time = TRUE

'''
