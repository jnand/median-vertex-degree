"""Node object that can be weak-referenced by Python.

Future improvement should implement a new CPython type
"""


class Node(object):

    def __init__(self, deg=0 ):
        self.deg = deg

    def __add__(self, val):
        return self.deg + val

    def __radd__(self, val):
        return self.deg + val

    def __sub__(self, val):
        return self.deg - val

    def __rsub__(self, val):
        return self.deg - val

    def __repr__(self):
        return str(self.deg)

    def __cmp__(self, other):
        return cmp(self.deg, other)