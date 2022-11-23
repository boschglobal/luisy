# Copyright (c) 2022 - for information on the respective copyright owner see the NOTICE.rst file or
# the repository https://github.com/boschglobal/luisy
#
# SPDX-License-Identifier: Apache-2.0

import unittest
import luigi
from luigi.util import requires
import luisy.visualize as vis


class Task2b(luigi.Task):
    a = luigi.IntParameter(default=84)


class Task2a(luigi.Task):
    a = luigi.IntParameter(default=82)


class Task1(luigi.Task):
    def requires(self):
        return [Task2a(a=c) for c in ["a", "b", "c"]] + [Task2b()]


class Task1b(luigi.Task):
    def requires(self):
        return {Task2b(a=5), Task2b(a=10)}


class Task1c(luigi.Task):
    def requires(self):
        return Task2b()


@requires(Task1)
class Task0(luigi.Task):
    pass


def get_names(g):
    return list(g.nodes())


class TestVisualization(unittest.TestCase):

    def test_unique_types(self):
        objects = [luigi.Task(), 1, "bla", 30.0, "test", 5, Task0, luigi.Task()]
        res = vis.unique_types(objects)
        self.assertEqual(res, objects[:4] + [objects[6]])

    def test_dependency_graph_with_unique_children(self):
        # unique_children = True
        dag = vis.make_dependency_graph(
            task=Task0(),
            namefunc=vis.simple_task_string,
            unique_children=True,
        )
        names = get_names(dag)
        type_names = [n.split("\n")[0] for n in names]
        self.assertEqual(type_names, ["Task0", "Task1", "Task2a", "Task2b"])

    def test_dependency_graph_with_none_unique_children(self):
        dag = vis.make_dependency_graph(
            task=Task0(),
            namefunc=vis.simple_task_string,
            unique_children=False,
        )
        names = get_names(dag)
        type_names = [n.split("\n")[0] for n in names]
        self.assertEqual(type_names, ["Task0", "Task1", "Task2a", "Task2a", "Task2a", "Task2b"])

    def test_dependency_graph_with_limited_depth(self):
        dag = vis.make_dependency_graph(
            Task0(),
            namefunc=vis.simple_task_string,
            unique_children=True,
            depth_limit=1,
        )
        names = get_names(dag)
        type_names = [n.split("\n")[0] for n in names]
        self.assertEqual(type_names, ["Task0", "Task1"])

    def test_dependency_graph_with_set_as_requirement(self):
        # requires is a set
        dag = vis.make_dependency_graph(
            task=Task1b(),
            namefunc=vis.simple_task_string,
            unique_children=False,
        )
        names = get_names(dag)
        type_names = [n.split("\n")[0] for n in names]
        self.assertEqual(type_names, ["Task1b", "Task2b", "Task2b"])

    def test_dependency_graph_with_single_requirement(self):
        # requires is a single task
        dag = vis.make_dependency_graph(Task1c(), unique_children=False)
        names = get_names(dag)
        type_names = [n.split("\n")[0] for n in names]
        self.assertEqual(type_names, ["Task1c", "Task2b"])
