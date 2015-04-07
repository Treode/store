class ClassName1(object):
    pass

class ClassName2(object):
    pass

class PatchClass(object):
    def isCool(self):
        return "patch successful"

class UnpatchedClass(object):
    def isCool(self):
        return "patch failed"