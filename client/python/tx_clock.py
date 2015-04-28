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

import sys

from functools import total_ordering

@total_ordering
class TxClock(object):

    # Bounds on TxClock values
    MinValue = 0
    # In Python, longs are actually unbounded, but we'll give TxClock a max
    # TODO: How big do we want the max to be?
    MaxValue = 2**64 - 1

    # REQUIRES: Input time in seconds
    def __init__(self, time=None, input_in_usecs=False):
        self._input_in_usecs = input_in_usecs

        # Default to current time
        if (time == None):
            # time in seconds
            time = long(time.time())

        # Convert time to microseconds if needed
        self.u_factor = 6
        if (self._input_in_usecs):
            self.time = long(time) % TxClock.MaxValue
        else:
            self.time = long(time * 10**self.u_factor) % (TxClock.MaxValue + 1)

    def to_seconds(self):
        # Times are ints, not floats
        return self.time / (10**self.u_factor)

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
        