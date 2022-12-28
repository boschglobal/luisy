# Copyright (c) 2022 - for information on the respective copyright owner see the NOTICE.rst file or
# the repository https://github.com/boschglobal/luisy
#
# SPDX-License-Identifier: Apache-2.0

import ast
import json
import hashlib
import inspect
import importlib
import logging
import os
import subprocess
import sys

from distlib.database import DistributionPath
import numpy as np
import pandas as pd
import requirements

logger = logging.getLogger(__name__)


class RequirementFileNotFound(Exception):
    pass


def create_hashes(task_list, requirements_path=None):
    """
    Produces a hash for each luisy Task in the 'task_list'.
    The hash changes as soon as any of the code that is inside the ClassDef of a task (or any
    other code used by the task) changes.

    Args:
        task_list (list[luisy.Task]): List of task classes
        requirements_path (str): location if requirements.txt, if None, try to get it automatically

    Returns:
        list[str]: The hash values for the tasks in hexadecimal.
    """
    hashes_list = list()

    if requirements_path is None:
        #  This should work with any task from the list
        task_class = task_list[0]
        try:
            requirements_path = get_requirements_path(task_class)
            requirements_dict = get_requirements_dict(requirements_path)
        except RequirementFileNotFound:
            logger.warning(
                'No requirements.txt found.'
                'Will try to infer from installed version'
            )
            requirements_dict = get_all_deps_with_versions(
                task_class.__module__.split('.')[0],
            )
    else:
        requirements_dict = get_requirements_dict(requirements_path)

    deps_map = create_deps_map(requirements_dict)  # req -> dep, one line per dep

    for task_class in task_list:
        module = inspect.getmodule(task_class)
        hashes, imported_packages, vis_data = walk_nodes(task_class.__name__, module.__name__)
        df_reqs = map_imports_to_requirements(imported_packages, requirements_dict, deps_map)

        # Exclude luisy, as we don't want that a bugfix or new feature in luisy leads to
        # re-execution of all pipelines
        df_reqs = df_reqs[df_reqs['package'] != 'luisy']

        version_info_to_hash = df_reqs.requirement_line.unique().tolist()
        version_info_to_hash.sort()  # order comes from a set
        to_hash = "".join(hashes + version_info_to_hash)
        final_hash = hashlib.md5(to_hash.encode()).hexdigest()
        hashes_list.append(final_hash)

    return hashes_list


def walk_nodes(variable_name, module_name, parent="", parent_module=""):
    """
    Function that recursively visits AST nodes.
    Theses nodes are either ClassDefs, FunctionDefs or simply Name nodes.
    For each node

    * the sourcecode within the node body is hashed and added to a return list
    * the names of external packages used inside the node body is added to a return list
    * if there are variable names coming from outside the node body, but from the same package as
      the node, the function also visits these nodes

     Args:
        module_name (str): module name of current node
        parent (str): name of parent node
        parent_module (str): name of module of parent node
        variable_name (str): name of AST node. Class name, Function name, or a module variable.

    Returns:
        tuple: Triple of list of hashes, set of strings representing the imported packages, and a
        list of dicts holding the visualization data collected during traversal of
        the ast syntax tree. Each dict represents an AST node. Contains names of node and its
        parent, modules names of node and parent and node type. This info can be used to plot
        the dependency graph.
    """
    visualization_data = []

    module = importlib.import_module(module_name)
    if not hasattr(module, variable_name):
        raise AttributeError(f"Module '{module_name}' does not contain any variable named "
                             f"'{variable_name}'!")
    attr_type = type(getattr(module, variable_name)).__name__
    if attr_type == 'module':  # check if variable is itself a module
        module_name = f"{module_name}.{variable_name}"
        module = importlib.import_module(module_name)
    module_source = inspect.getsource(module)
    module_tree = ast.parse(module_source)
    node = module_tree  # node is the whole module

    if attr_type != 'module':
        node = get_node(variable_name, module_tree)  # node is a branch of the module tree

    visualization_data.append(
        {
            "name": variable_name,
            "parent": parent,
            "module": module_name,
            "parent_module": parent_module, "type": type(node).__name__
        }
    )
    hashes = [produce_node_hash(node)]

    # df containing all imports in module
    df_imports = get_import_data(module_tree)
    # all variables used inside node, but defined outside
    var_names = get_varnames_delta(node)

    #  filter imports that are used inside node
    df_relevant_imports = df_imports.loc[
        np.isin(df_imports.used_name, var_names)]  # .loc keeps columns in empty case
    df_relevant_imports = df_relevant_imports.loc[~ df_relevant_imports.package.apply(is_standard)]
    # join external imports with requirements file
    package = module_name.split(".")[0]
    df_internal_imports = df_relevant_imports.loc[df_relevant_imports.package.values == package]
    imported_packages = set(df_relevant_imports.package)
    imported_packages.discard(package)  # imports of external libraries

    # filter variables that come from the module itself
    vars_same_module = var_names[~np.isin(var_names, df_imports.used_name)]

    # call function recursively on package-internal nodes
    varnames_next_level = vars_same_module.tolist() + df_internal_imports.varname.tolist()
    module_names_next_level = [module_name] * len(
        vars_same_module) + df_internal_imports.module.tolist()

    for var_, mod_ in zip(varnames_next_level, module_names_next_level):
        # drop special case like __name__  , or recursive calls
        if not var_.startswith("__") and var_ != variable_name:
            hashes_c, imports_c, digraph_data_c = walk_nodes(var_, mod_, parent=variable_name,
                                                             parent_module=module_name)
            hashes = hashes + hashes_c
            imported_packages.update(imports_c)
            visualization_data = visualization_data + digraph_data_c
    return hashes, imported_packages, visualization_data


def _remove_docstrings(node):
    """
    Removes the doc-strings
    """

    if isinstance(node, (ast.FunctionDef, ast.ClassDef, ast.Module)):
        if node.body and isinstance(node.body[0], ast.Expr):
            if isinstance(node.body[0].value, ast.Str):
                # Cut-off that element from the body
                # The reason why we do not set it to the empty string is that we also want to have
                # the same doc-string if someone adds a doc-string, not just changes it
                node.body = node.body[1:]

    if hasattr(node, 'body'):
        for _node in node.body:
            _remove_docstrings(_node)


def produce_node_hash(class_node):
    """
    Produce hash for AST node, normally ClassDef or FunctionDef or Constant.
    invariant to: comments, spaces, blank lines.
    not invariant to: variable renaming
    """

    _remove_docstrings(class_node)
    st = ast.dump(class_node, annotate_fields=False)  # no need to include unambiguous field names.
    hash_ = hashlib.md5(st.encode())
    return hash_.hexdigest()


def get_import_data(module_tree):
    """
    Get imports of a module.
    Args:
        module_tree(ast.AST): Syntax tree of module
    Returns:
        pandas.DataFrame: Data about imports to the module.
    """
    v = ImportReturner()
    v.visit(module_tree)
    data = v.import_data
    if len(data) > 0:
        df = pd.DataFrame(data)
        df["used_name"] = df.apply(lambda row: row.varname if row.asname is None else row.asname,
                                   axis=1)
        df["package"] = df.module.apply(lambda st: st.split(".")[0])
    else:
        df = pd.DataFrame(columns=["module", "varname", "asname", "used_name", "package"])

    return df


class ImportReturner(ast.NodeVisitor):
    """
    Scans syntax tree for imports.
    """

    def __init__(self):
        self.import_data = []

    def star_import_in_imports(self, lst):
        return any([import_node["varname"] == '*' for import_node in lst])

    def generic_visit(self, node):
        imports = self.filter_imports(node)
        if imports is not None:
            if self.star_import_in_imports(imports):
                raise Exception("Star import not supported by luisy hash creation!")
            self.import_data = self.import_data + imports
        ast.NodeVisitor.generic_visit(self, node)

    def filter_imports(self, node):
        if type(node).__name__ == "Import":
            aliases = node.names
            return [{"module": alias.name, "varname": alias.name, "asname": alias.asname} for alias
                    in
                    aliases]
        if type(node).__name__ == "ImportFrom":
            mod = node.module
            aliases = node.names
            return [{"module": mod, "varname": alias.name, "asname": alias.asname} for alias in
                    aliases]


def get_varnames_delta(node):
    """
    Find all variable names that are used the node body, but defined outside the node body.
    """
    vr = VariablesReturner()
    vr.visit(node)
    df_names = pd.DataFrame(vr.variable_names)  # lists loaded and stored varnames inside node
    if len(df_names) == 0:
        return np.array([])
    df_load = df_names.query("kind == 'Load'")
    df_store = df_names.loc[np.isin(df_names["kind"], ["Store", "arg", "FunctionDef", "ClassDef",
                                                       "ExceptHandler"])]
    df_load = df_load.groupby("id").agg({"lineno": "min"}).reset_index()
    df_store = df_store.groupby("id").agg({"lineno": "min"}).reset_index()
    df_match = df_store.merge(df_load, on="id")

    ids_to_ignore = list(df_match.id) + ["self"]
    df_names_outside_classdef = df_load.loc[~np.isin(df_load.id, ids_to_ignore)]
    filter_builtins = df_names_outside_classdef.id.apply(is_builtin)
    df_names_outside_classdef = df_names_outside_classdef.loc[~filter_builtins]

    return df_names_outside_classdef.id.values


class VariablesReturner(ast.NodeVisitor):
    """
    Scans syntax tree for variable names. While visiting all nodes of the ast syntax tree,
    save all names that are loaded or stored. These include FunctionDefs, ClassDefs, Function
    arguments or the Exception Handler context name.
    """

    def __init__(self):
        self.variable_names = []

    def filter_variables(self, node):
        if type(node).__name__ == "Name":
            ctx = node.ctx
            return {"id": node.id, "kind": type(ctx).__name__, "lineno": node.lineno}
        if type(node).__name__ == "arg":
            return {"id": node.arg, "kind": "arg", "lineno": node.lineno}

        #  Function Defs and Class Defs can also be inside another class def
        if type(node).__name__ == "FunctionDef":
            return {"id": node.name, "kind": "FunctionDef", "lineno": node.lineno}
        if type(node).__name__ == "ClassDef":
            return {"id": node.name, "kind": "ClassDef", "lineno": node.lineno}
        if type(node).__name__ == "ExceptHandler":
            return {"id": node.name, "kind": "ExceptHandler", "lineno": node.lineno}

    def generic_visit(self, node):
        var_names = self.filter_variables(node)
        if var_names is not None:
            self.variable_names.append(var_names)
        ast.NodeVisitor.generic_visit(self, node)


def get_node(node_name, module_tree):
    """
    Find an node with given name in the upper level of a module.
    """
    for node in module_tree.body:
        if type(node).__name__ == "Assign":
            target_names = [t.id for t in node.targets]
            if node_name in target_names:
                return node.value
        if type(node).__name__ in ["FunctionDef", "ClassDef"]:
            if node.name == node_name:
                return node
    raise ValueError(f"Node '{node_name}' was not found in AST tree.")


def get_requirements_path(task_class):
    """
    Tries to automatically find the path of requirements file.
    """
    path_ = inspect.getfile(task_class)
    while True:
        groups = path_.split(os.sep)
        if len(groups) == 1:
            raise RequirementFileNotFound(
                "Requirements file not could not be found automatically!"
            )
        path_ = os.sep.join(groups[:-1])
        req_path = os.path.join(path_, "requirements.txt")
        if os.path.exists(req_path):
            break
    return req_path


def get_requirements_dict(path):
    """
    Open the requirements file and create a dictionary which maps each package to its
    corresponding line in the requirements.txt. This line contains all version info and will be used
    for the hash later.
    """
    reqs_dict = {}
    with open(path, "r") as f:
        for req in requirements.parse(f):
            reqs_dict[req.name] = req.line.strip()
    return reqs_dict


def _harmonize_package_name(package_name):
    return package_name.replace("-", "_")


def create_deps_map(requirements_dict):
    """
    Map each requirement to its dependencies.
    Both keys and values are in unified format. That means to apply: .lower().replace("-","_")
    """
    deps_map = {}
    for req in requirements_dict:  # here we need the Pypi name
        deps = [_harmonize_package_name(d.lower()) for d in get_all_deps(req)]
        deps_map[_harmonize_package_name(req.lower())] = deps
    return deps_map


def get_all_deps_with_versions(package):
    """
    Get names of all dependencies of 'package' using 'pipdeptree' including their required version.

    Args:
        package(str): package for which we want deps.

    Returns:
        dict: Dict where the keys are the package names and the values are the version constraints
        of all dependencies.
    """

    # Underlines in package names become hyphens due to a strange behavior of setuptools
    package = package.replace('_', '-')
    reqs = subprocess.check_output(
        [
            sys.executable,
            "-m", "pipdeptree", "--package",
            package,
            "-w", "silence", "--json-tree"
        ],
        stderr=subprocess.DEVNULL,
    )
    reqs_list = json.loads(reqs)

    if len(reqs_list) == 0:
        return {}

    reqs_list = reqs_list[0].get('dependencies', [])

    reqs_dict = {
        _harmonize_package_name(req['package_name']): "".join(
            [
                _harmonize_package_name(req['package_name']),
                req['required_version'].replace('Any', ''),
            ]) for req in reqs_list
    }

    return reqs_dict


def get_all_deps(package):
    reqs_dict = get_all_deps_with_versions(package)
    return set(reqs_dict.keys())


def map_imports_to_requirements(imported_packages, requirements_dict, deps_map):
    """
    Given a list of packages, look whether they are in the requirements of the repository.
    If it is not a direct requirement, look if it is a dependency of a requirements.
    Args:
        imported_packages(iterable[str]): list of package names
        requirements_dict(dict): dict returned by ...
        deps_map(dict): dict returned by

    Returns:
        pandas.DataFrame: Dictionary listing for each package, one or several lines of the
        requirements file that concern it.
    """
    package_name_to_pypi = make_pypi_mapper()
    imported_packages = [package_name_to_pypi(p) for p in imported_packages]
    requirements_dict = {k.lower().replace("-", "_"): v for (k, v) in requirements_dict.items()}
    imports_to_req = []
    for package in imported_packages:
        if package in requirements_dict:
            imports_to_req.append(
                {"package": package, "requirement_line": requirements_dict[package]})
        else:  # Not directly in requirements file.
            for req in deps_map:
                if package in deps_map[req]:
                    imports_to_req.append(
                        {"package": package, "requirement_line": requirements_dict[req]})

    if len(imports_to_req) == 0:
        df_reqs = pd.DataFrame(data={'package': [], 'requirement_line': []})
    else:
        df_reqs = pd.DataFrame(imports_to_req)
    is_in_reqs = np.isin(imported_packages, df_reqs.package)
    if not is_in_reqs.all():
        not_found = np.array(imported_packages)[~is_in_reqs]
        raise Exception(f"Imported packages {not_found} "
                        f"could not be mapped to requirements file")
    return df_reqs


def is_builtin(func):
    """
    This function is work in progress and will be improved with every upcoming error.
    """
    try:
        f = eval(func)
        if hasattr(f, "__module__"):
            if f.__module__ == 'builtins' or type(f).__name__ == 'builtin_function_or_method':
                return True
    except NameError:  # variable is not defined
        return False
    return False


def is_standard(package_name):
    """
    Determines whether a package is in the python standard lib.
    """
    if package_name in ["sys"]:  # did not work like for the others
        return True
    try:
        major = sys.version_info.major
        minor = sys.version_info.minor
        std_folder = f"python{major}.{minor}"
        case_1 = os.path.join(std_folder, f"{package_name}.py")
        case_2 = os.path.join(std_folder, package_name, "__init__.py")
        path = importlib.util.find_spec(package_name).origin
        if path is None:
            # have to actually load module to determine file.
            mod = importlib.__import__(package_name)
            path = mod.__file__
        if path.endswith(case_1) or path.endswith(case_2) or path == 'built-in':
            return True

    except Exception:
        return False

    return False


def make_pypi_mapper():
    special_pypi = get_irregular_pypi_names()

    def package_name_to_pypi(package):
        """
        If the standardized package name is not the Pypi name, return standardized pypi-name
        the node, the function also visits these nodes

         Args:
            package(str): name of python package, (standardized with .lower().replace("-", "_"))
        Returns:
            package(str): Pypi name (standardized with .lower().replace("-", "_")) if the latter
            differs from package name.
        """
        if package in special_pypi:
            return special_pypi[package]
        else:
            return package

    return package_name_to_pypi


def get_irregular_pypi_names():
    """
    Get pypi name for given package if they differ.
     unify everything to format lower().replace("-","_")  is not the package name
    """
    distlib_logger = logging.getLogger("distlib")
    distlib_logger.setLevel(40)  # too much info
    dist_path = DistributionPath(include_egg=True)
    map_ = {}
    for dist in dist_path.get_distributions():
        modules = [m.lower().replace("-", "_") for m in dist.modules]
        name = dist.name.lower().replace("-", "_")
        if len(modules) > 0 and modules != [name]:
            for m in modules:
                map_[m] = name
    return map_
