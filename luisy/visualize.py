# Copyright (c) 2022 - for information on the respective copyright owner see the NOTICE.rst file or
# the repository https://github.com/boschglobal/luisy
#
# SPDX-License-Identifier: Apache-2.0

import networkx as nx
import numpy as np
import matplotlib.pyplot as plt

from functools import partial
from luigi.task import flatten


def simple_task_string(task):
    """
    Create simple string label out of task.
    """
    groups = task.task_id.split("_")
    return f"{groups[0]}\n{groups[-1]}"


def create_label(task, excl_list=(), with_id=False):
    """
    Make a string label out of a luigi task. Make sure the string is unique !
    Args:
        task (luisy.tasks.base.Task): The task the label should be created for
        excl_list (list): List of parameter kwargs that are not included in the label
        with_id (bool): If False, do not include the hash of the task.

    Returns:
        str: Label to be used for the node
    """
    groups = task.task_id.split("_")
    name = groups[0]
    if with_id:
        name += f"{groups[-1]}"
    for k in task.param_kwargs:
        if k not in excl_list:
            name += f"\n{k}={task.param_kwargs[k]}"
    return name


def add_children(task, graph, namefunc=create_label, depth=1, depth_limit=10,
                 unique_children=False):
    """
    Recursive Function that adds the children of task to a graph object. Make sure that 'namefunc'
    returns unique strings for all tasks that will be included in the tree
    Args:
        task (luisy.tasks.base.Task): The task
        graph (networkx.DiGraph): The directed ayclic graph
        namefunc (callable): Function that creates a string out a task.
        depth (int): Depth of current node
        depth_limit (int): Max allowed depth of tree
        unique_children (bool): If true, add only subset of unique types of children for each task
    """
    if depth <= depth_limit:
        children = flatten(task.requires())
        if unique_children:
            children = unique_types(children)
        current_depth = depth + 1
        for c in children:  # depth first search
            graph.add_edge(namefunc(task), namefunc(c))
            add_children(
                task=c,
                graph=graph,
                namefunc=namefunc,
                depth=current_depth,
                unique_children=unique_children,
                depth_limit=depth_limit,
            )


def make_dependency_graph(task, excl_list=(), namefunc=None, unique_children=True, depth_limit=10):
    """
    Create pydot graph out of a tasks dependencies.
     Args:
        task (luisy.tasks.base.Task): task
        excl_list (list): Parameter forwarded to 'create_label' if namefunc is none
        namefunc (callable): Function that creates a string out a task.
        depth_limit (int): Max allowed depth of tree
        unique_children (bool): If true, add only subset of unique types of children
    """
    dag = nx.DiGraph()
    if namefunc is None:
        namefunc = partial(create_label, excl_list=excl_list)
    dag.add_node(namefunc(task))  # if task has no children the root node has to be created
    add_children(task, dag, namefunc, unique_children=unique_children, depth_limit=depth_limit)
    return dag


def unique_types(tasks):
    """
    Given a list of objects, return a subset that has each type only once.
    """
    types = []
    res_list = []
    for e in tasks:
        name = type(e).__name__
        if name not in types:
            types.append(name)
            res_list.append(e)
    return res_list


def visualize_task(task, ax=None, unique_children=True, parameters_to_exclude=()):
    """
    Visualizes the dependencies of a :py:class:`luisy.Task`.

    Args:
        task (luisy.tasks.base.Task): The task whose dependencies should be visualized.
        ax (matplotlib.axis.Axis): A matplotlib axis to draw in
        unique_children (bool): If true, add only subset of unique types of children
        parameters_to_exclude (list): List of names of py:mod:`luigi`-parameters to exclude in
            visualization.
    """

    dag = make_dependency_graph(
        task=task,
        unique_children=unique_children,
        depth_limit=np.inf,
        excl_list=parameters_to_exclude,
    )

    if ax is None:
        _, ax = plt.subplots(figsize=(15, 10))

    pos = nx.shell_layout(dag)

    nx.draw_networkx_labels(dag, ax=ax, pos=pos)
    nx.draw_networkx_edges(dag, ax=ax, pos=pos)
    ax.axis('off')
