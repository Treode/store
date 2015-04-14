class TxClock(object):
    def __init__(self, time):
        self.time = long(time)

    def _almostEqual(self, x, y):
        # TODO Appropriate epsilon value?
        epsilon = 0.5
        return (abs(x - y) < epsilon)

    # Make TxClock instances comparable
    def __eq__(self, other):
        return self._almostEqual(self.time, other.time)

    def __gt__(self, other):
        return self.time > other.time and not self.__eq__(self, other)

    def __ge__(self, other):
        return self.time >= other.time

    def __ne__(self, other):
        return not self.__eq__(self, other)

    def __lt__(self, other):
        return self.time < other.time and not self.__eq__(self, other)

    def __le__(self, other):
        return self.time <= other.time
        