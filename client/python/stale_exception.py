class StaleException(Exception):
    def __init__(self, value_txclock):
        self.value_txclock = value_txclock

    def __str__(self):
        return str(self.value_txclock)