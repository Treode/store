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
            time = time.time()

        # Convert time to microseconds if needed
        if (self._input_in_usecs):
            self.time = long(time) % TxClock.MaxValue
        else:
            self.u_factor = 6
            self.time = long(time * 10**self.u_factor) % TxClock.MaxValue

    def to_seconds(self):
        # Times are ints, not floats
        return self.time / (10**self.u_factor)
        
    def _almostEqual(self, x, y):
        # TODO Appropriate epsilon value?
        epsilon = 0.5
        return (abs(x - y) < epsilon)

    # Make TxClock instances comparable
    def __eq__(self, other):
        if (type(other) == TxClock):
            return self._almostEqual(self.time, other.time)
        else:
            return False

    def __gt__(self, other):
        if (type(other) == TxClock):
            return self.time > other.time and not self.__eq__(self, other)
        elif (other == None):
            return True
        else:
            return False
        