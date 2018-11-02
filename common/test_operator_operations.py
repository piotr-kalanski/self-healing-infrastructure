import unittest
from common.base_operator import BaseOperator


class DummyOperator(BaseOperator):
    def __init__(self, x):
        super(DummyOperator, self).__init__()
        self.x = x

    def execute(self, context: dict):
        return


class BaseOperatorTests(unittest.TestCase):

    def test_add_one_dependency(self):
        o1 = DummyOperator(1)
        o2 = DummyOperator(2)

        o1.add_downstream_operator(o2)

        self.assertEquals([o2], o1.downstream_operators)

    def test_add_one_dependency_with_rshift_operator(self):
        o1 = DummyOperator(1)
        o2 = DummyOperator(2)

        o1 >> o2

        self.assertEquals([o2], o1.downstream_operators)

    def test_add_chain_of_dependencies_with_rshift_operator(self):
        o1 = DummyOperator(1)
        o2 = DummyOperator(2)
        o3 = DummyOperator(3)

        o1 >> o2 >> o3

        self.assertEquals([o2], o1.downstream_operators)
        self.assertEquals([o3], o2.downstream_operators)

    def test_add_chain_of_dependencies_with_lshift_operator(self):
        o1 = DummyOperator(1)
        o2 = DummyOperator(2)
        o3 = DummyOperator(3)

        o1 << o2 << o3

        self.assertEquals([o2], o3.downstream_operators)
        self.assertEquals([o1], o2.downstream_operators)

    def test_add_chain_of_dependencies_with_rshift_lshift_operators(self):
        o1 = DummyOperator(1)
        o2 = DummyOperator(2)
        o3 = DummyOperator(3)
        o4 = DummyOperator(4)
        o5 = DummyOperator(5)

        o1 >> o2 >> o3
        o2 >> o4
        o5 << o4 << o3

        self.assertEquals([o2], o1.downstream_operators)
        self.assertEquals([o3, o4], o2.downstream_operators)
        self.assertEquals([o4], o3.downstream_operators)
        self.assertEquals([o5], o4.downstream_operators)
