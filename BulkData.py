import time
import datetime
s = "01-12-2011 21:17:04"
print(time.mktime(datetime.datetime.strptime(s, "%d-%m-%Y %H:%M:%S").timetuple())) # %H:%M:%S"

