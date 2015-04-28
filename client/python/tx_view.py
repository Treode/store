class TxView(object):

    CREATE = "create"
    HOLD = "hold"
    UPDATE = "update"
    DELETE = "delete"

    def __init__(self):
        self.__view = {}

    def create(self, table, key, value):
        self.__view[(table, key)] = (TxView.CREATE, value)

    def hold(self, table, key):
        self.__view[(table, key)] = (TxView.HOLD, None)  

    def update(self, table, key, value):
        self.__view[(table, key)] = (TxView.UPDATE, value)        

    def delete(self, table, key):
        self.__view[(table, key)] = (TxView.DELETE, None)

    def get_view(self):
        return self.__view  