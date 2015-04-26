class StaleException(Exception):
    def __init__(self, read_txclock=None, value_txclock=None):
        self.read_txclock = read_txclock
        self.value_txclock = value_txclock

    def __str__(self):
        return str((self.read_txclock, self.value_txclock))

    def toTuple(self):
        return (self.read_txclock, self.value_txclock)