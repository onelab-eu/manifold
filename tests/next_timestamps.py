#!/usr/bin/env python
# -*- coding: utf-8 -*-

import calendar, dateutil.parser
from datetime import datetime, timedelta

NUM      = 20
INTERVAL = 30 # minutes

tm = datetime.now()
tm = tm - timedelta(minutes=tm.minute % 30,
                    seconds=tm.second,
                    microseconds=tm.microsecond)
for i in range(1, NUM+1):
    tm += timedelta(minutes=30)
    print calendar.timegm(tm.utctimetuple()), tm
