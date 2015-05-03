# Copyright 2014 Treode, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys, time

from functools import total_ordering
from tx_clock import *

@total_ordering
class TxClock(object):

    __min_micro_seconds = long(0)
    __max_micro_seconds = long(2**63 - 1)

    # Bounds on TxClock values
    @staticmethod
    def min_value():
        return TxClock(micro_seconds=TxClock.__min_micro_seconds) 
    
    # In Python, longs are actually unbounded, but we'll give TxClock a max
    @staticmethod
    def max_value():
        return TxClock(micro_seconds=TxClock.__max_micro_seconds)

    @staticmethod
    def now():
        current_time_micro_seconds = long(time.time())*10**6
        return TxClock(micro_seconds=current_time_micro_seconds)

    # Input time in micro-seconds!
    def __init__(self, micro_seconds=None):
        if (micro_seconds == None):
            raise ValueError("Please input time in micro-seconds!")
        elif (micro_seconds > TxClock.__max_micro_seconds):
            print "micro_seconds: ", micro_seconds
            print "max: ", TxClock.__max_micro_seconds
            raise ValueError("micro_seconds arg > max micro_seconds value")
        # Assume user input time in micro-seconds
        else:
            self.time = long(micro_seconds)

    def to_seconds(self):
        # Times are ints, not floats
        return self.time / (10**6)

    def __repr__(self):
        return "TxClock(%s)" % str(self.time)
        
    # Make TxClock instances comparable
    def __eq__(self, other):
        if (type(other) == TxClock):
            return self.time == other.time
        else:
            return False

    def __gt__(self, other):
        if (type(other) == TxClock):
            return self.time > other.time and not self.__eq__(other)
        elif (other == None):
            return True
        else:
            return False
        