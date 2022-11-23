# Copyright (c) 2022 - for information on the respective copyright owner see the NOTICE.rst file or
# the repository https://github.com/boschglobal/luisy
#
# SPDX-License-Identifier: Apache-2.0

import unittest
import ast
from unittest.mock import (
    patch,
    call,
    Mock
)
import inspect
import pandas as pd
import numpy as np

import luigi
from luisy import Task
from luisy.decorators import (
    raw,
    interim,
    requires,
)
from luisy.code_inspection import (
    get_requirements_path,
    get_requirements_dict,
    create_hashes,
    get_node,
    _remove_docstrings,
    produce_node_hash,
    VariablesReturner,
    get_import_data,
    walk_nodes,
    get_varnames_delta,
    is_builtin,
    is_standard,
    get_all_deps,
    get_irregular_pypi_names,
    make_pypi_mapper,
    create_deps_map,
    map_imports_to_requirements,
)

import tempfile


@raw
class ToyLowestTask(Task):
    def run(self):
        self.write({'A': 2})


@interim
@requires(ToyLowestTask)
class ToyTask(Task):
    """
    Doc string of the class
    """
    a = luigi.IntParameter()
    b = luigi.IntParameter(default=2)

    def run(self):
        """
        Doc string of method
        """
        data = {'A': self.a, 'B': self.b}
        self.write(data)


class TestCodeInspection(unittest.TestCase):

    def test_hash_the_same_on_luisy_change(self):
        req_path = get_requirements_path(ToyTask)
        return_value = get_requirements_dict(req_path)

        return_value["luisy"] = "luisy==42.8"
        with patch("luisy.code_inspection.get_requirements_dict") as mock_req_dict:
            mock_req_dict.return_value = return_value
            hash1 = create_hashes([ToyTask])

        return_value["luisy"] = "luisy==41.0"
        with patch("luisy.code_inspection.get_requirements_dict") as mock_req_dict:
            mock_req_dict.return_value = return_value
            hash2 = create_hashes([ToyTask])
        self.assertEqual(hash1, hash2)

    def test_create_hash(self):
        #  tests are not inside luisy package
        req_path = get_requirements_path(ToyTask)
        return_value = get_requirements_dict(req_path)
        return_value["luisy"] = "luisy==42.8"

        return_value["luigi"] = "luigi==42.7"
        with patch("luisy.code_inspection.get_requirements_dict") as mock_req_dict:
            mock_req_dict.return_value = return_value
            hash1 = create_hashes([ToyTask])
        call_args = mock_req_dict.call_args[0][0]
        self.assertTrue(call_args.endswith("/requirements.txt"))

        return_value["luigi"] = "luigi==42.8"
        with patch("luisy.code_inspection.get_requirements_dict") as mock_req_dict:
            mock_req_dict.return_value = return_value
            hash2 = create_hashes([ToyTask])

        self.assertNotEqual(hash1, hash2)

    def test_doc_string_removal(self):
        mod = inspect.getmodule(ToyTask)
        mod_source = inspect.getsource(mod)
        mod_tree = ast.parse(mod_source)
        node = get_node("ToyTask", mod_tree)

        str_dump = ast.dump(node)
        self.assertIn('Doc string of the class', str_dump)
        self.assertIn('Doc string of method', str_dump)

        _remove_docstrings(node)

        str_dump = ast.dump(node)
        self.assertNotIn('Doc string of the class', str_dump)
        self.assertNotIn('Doc string of the class', str_dump)

    def test_hash_on_doc_string_change(self):
        mod = inspect.getmodule(ToyTask)
        mod_source = inspect.getsource(mod)
        mod_tree = ast.parse(mod_source)
        node = get_node("ToyTask", mod_tree)

        self.assertEqual(
            node.body[0].value.s,
            '\n    Doc string of the class\n    '
        )
        hash_a = produce_node_hash(node)

        node = get_node("ToyTask", mod_tree)
        node.body[0].value.s = "Some other doc string of the class"
        hash_b = produce_node_hash(node)

        self.assertEqual(hash_a, hash_b)

    def test_get_varnames_delta(self):
        variable_names = [{"id": "len", "kind": "FunctionDef", "lineno": 42},
                          {"id": "func", "kind": "FunctionDef", "lineno": 42},
                          {"id": "func", "kind": "Load", "lineno": 42},
                          {"id": "func2", "kind": "Load", "lineno": 42}]
        instance_mock = Mock()
        instance_mock.variable_names = variable_names
        with patch("luisy.code_inspection.VariablesReturner") as class_mock:
            class_mock.return_value = instance_mock
            res = get_varnames_delta("mocked_anyway")
        self.assertEqual(res, ["func2"])  # 'len is filtered out

    def test_get_varnames_delta_empty_case(self):
        instance_mock = Mock()
        instance_mock.variable_names = []
        with patch("luisy.code_inspection.VariablesReturner") as class_mock:
            class_mock.return_value = instance_mock
            res = get_varnames_delta("mocked_anyway")
        self.assertEqual(len(res), 0)

    def test_is_builtin(self):
        self.assertTrue(is_builtin("len"))
        self.assertFalse(is_builtin("blubbasd"))

    def test_is_standard(self):
        self.assertTrue(is_standard("random"))
        self.assertTrue(is_standard("pickle"))
        self.assertTrue(is_standard("sys"))
        self.assertFalse(is_standard("numpy"))


class TestWalkNodes(unittest.TestCase):
    def setUp(self):
        def walk_side_effect(variable_name, module_name, parent="", parent_module=""):
            if parent == "":  # act normally
                walk_nodes(variable_name, module_name, parent="", parent_module="")
            else:
                return [], [], []  # do not continue on children

        self.walk_side_effect = walk_side_effect

    def test_walk_nodes_normal_case(self):
        # case when node is a module itself
        hashes, imports, viz_data = walk_nodes("code_inspection", "luisy")
        self.assertEqual(viz_data[0]["type"], "Module")

        # case when node is not a Module
        hashes, imports, viz_data = walk_nodes("walk_nodes", "luisy.code_inspection")
        self.assertEqual(viz_data[0]["type"], "FunctionDef")

    def test_walk_nodes_recursive_calls_correct(self):
        """
        Make sure, that the recursive function 'walk_nodes' calls itself correctly when variables
        found in the code from the current node come from the same module or package as the node.
        """
        #  last row simulates recursive call on 'get_node' on itself
        import_data = [["luisy.my_mod", "my_func", None, "my_func", "luisy"],
                       ["luisy.code_inspection", "my_func2", None, "my_func2", "luisy"],
                       ["external.mod", "my_func3", None, "my_func3", "external"],
                       ["luisy.code_inspection", None, "get_node", "luisy"],
                       ["luisy.my_mod", "__my_func__", None, "my_func", "luisy"]]
        df_mock_imports = pd.DataFrame(import_data, columns=["module", "varname", "asname",
                                                             "used_name", "package"])
        with patch("luisy.code_inspection.get_import_data") as mock_imports:
            mock_imports.return_value = df_mock_imports
            with patch("luisy.code_inspection.get_varnames_delta") as mock_vars:
                mock_vars.return_value = np.array(["my_func", "my_func2", "get_node"])
                with patch("luisy.code_inspection.walk_nodes") as mock_walk:
                    mock_walk.side_effect = self.walk_side_effect
                    hashes, imports, viz_data = walk_nodes("get_node", "luisy.code_inspection")
        calls = mock_walk.call_args_list
        self.assertEqual(calls[0], call('my_func', 'luisy.my_mod', parent='get_node',
                                        parent_module='luisy.code_inspection'))
        self.assertEqual(calls[1], call('my_func2', 'luisy.code_inspection', parent='get_node',
                                        parent_module='luisy.code_inspection'))
        self.assertEqual(len(calls), 2)  # make sure external vars and recursive calls on node
        # itself are dropped

    def test_walk_nodes_exception_raised(self):
        with self.assertRaises(AttributeError) as ctx:
            walk_nodes("node_not_found", "luisy.code_inspection")
        self.assertEqual(str(ctx.exception), "Module 'luisy.code_inspection' does not contain any "
                                             "variable named 'node_not_found'!")


class TestVariablesReturner(unittest.TestCase):
    def setUp(self):
        self.func_code = """def my_func(var_a=35):
                                return 7
        """
        self.class_code = """@superluisy\nclass my_class:
                                            def __init__(self):
                                                self.my_var = some_func()
                                            def my_method():
                                                some_func = 42
                                                return 7
                                                """
        self.exception_code = """try:\n\traise Exception()\nexcept Exception as e:\n\tprint(e)"""
        self.for_code = """for i, c in enumerate(data):
                                print()
        """
        self.eval_expr_code = """t = eval("myfunc()")"""
        self.with_code = """with open as f:\n\tbpass"""
        self.dict_comp_code = """d = {k: v \\
                                        for k,v in tup}"""

    def assert_properties(self, dict, id, kind):
        """
        Helper func to test whether 'id' and 'kind' of name node is correct.
        """
        self.assertEqual(dict["id"], id)
        self.assertEqual(dict["kind"], kind)

    def get_variable_names(self, code_snippet):
        """
        Get data of ast.name nodes from code snippet.
        """
        tree = ast.parse(code_snippet)
        vr = VariablesReturner()
        vr.visit(tree)
        vars_dicts = vr.variable_names
        return vars_dicts

    def test_FunctionDefNode(self):
        vars_dicts = self.get_variable_names(self.func_code)
        self.assert_properties(vars_dicts[0], "my_func", "FunctionDef")
        self.assert_properties(vars_dicts[1], "var_a", "arg")

    def test_ClassDefNode(self):
        vars_dicts = self.get_variable_names(self.class_code)
        class_node = vars_dicts[0]
        self.assert_properties(class_node, "my_class", "ClassDef")
        superluisy_decorator = list(filter((lambda x: x["id"] == "superluisy"), vars_dicts))[0]
        self.assert_properties(superluisy_decorator, "superluisy", "Load")

    def test_ExceptionHandler(self):
        vars_dicts = self.get_variable_names(self.exception_code)
        handler_dict = vars_dicts[1]
        exception_dict = vars_dicts[-1]
        self.assert_properties(handler_dict, "e", "ExceptHandler")
        self.assert_properties(exception_dict, "e", "Load")

    def test_for_enumerate(self):
        vars_dicts = self.get_variable_names(self.for_code)
        self.assert_properties(vars_dicts[0], "i", "Store")
        self.assert_properties(vars_dicts[1], "c", "Store")

    def test_eval_expr(self):
        """
        Code that is a string argument to the `eval` function is not traced!
        """
        vars_dicts = self.get_variable_names(self.eval_expr_code)
        ids = [d["id"] for d in vars_dicts]
        self.assertFalse("myfunc" in ids)

    def test_with(self):
        vars_dicts = self.get_variable_names(self.with_code)
        self.assert_properties(vars_dicts[1], "f", "Store")

    def test_dict_comp(self):
        """
        Dict comprehension always includes load and store of variables!
        """
        vars_dicts = self.get_variable_names(self.dict_comp_code)
        self.assert_properties(vars_dicts[-2], "v", "Store")
        self.assert_properties(vars_dicts[-3], "k", "Store")


class TestImports(unittest.TestCase):
    def setUp(self):
        self.code = "import pandas as pd\n" \
                    "import luisy\n" \
                    "from luisy.code_inspection import VariablesReturner\n" \
                    "from some_persistence_framework.helpers import (\n" \
                    "   try_to_save,\n" \
                    "   try_to_load)"
        self.execption_code = "from luisy import *"

    def test_get_import_data(self):
        tree = ast.parse(self.code)
        df = get_import_data(tree)
        self.assertListEqual(
            df["module"].tolist(),
            [
                "pandas",
                "luisy",
                "luisy.code_inspection",
                "some_persistence_framework.helpers",
                "some_persistence_framework.helpers"
            ]
        )
        self.assertListEqual(
            df["varname"].tolist(),
            [
                "pandas",
                "luisy",
                "VariablesReturner",
                "try_to_save",
                "try_to_load"
            ]
        )
        self.assertEqual(df["asname"][0], "pd")
        self.assertListEqual(
            df["used_name"].tolist(),
            [
                "pd",
                "luisy",
                "VariablesReturner",
                "try_to_save",
                "try_to_load"
            ]
        )
        self.assertListEqual(
            df["package"].tolist(),
            [
                "pandas",
                "luisy",
                "luisy",
                "some_persistence_framework",
                "some_persistence_framework"
            ]
        )

    def test_exception_case(self):
        tree = ast.parse(self.execption_code)
        with self.assertRaises(Exception):
            get_import_data(tree)


class TestGetNode(unittest.TestCase):
    def setUp(self):
        self.code = "module_var = somefunc()\n" \
                    "def func():\n" \
                    "   pass\n" \
                    "class my_class42:" \
                    "   pass"

    def test_get_node(self):
        tree = ast.parse(self.code)

        assign_target = get_node("module_var", tree)
        self.assertTrue(isinstance(assign_target, ast.Call))

        function_def = get_node("func", tree)
        self.assertTrue(isinstance(function_def, ast.FunctionDef))

        class_def = get_node("my_class42", tree)
        self.assertTrue(class_def.name, "my_class42")

        with self.assertRaises(ValueError):
            get_node("not_found", tree)


class TestExternalDependenciesManagement(unittest.TestCase):
    def test_get_all_deps(self):
        # empty set for unknown package
        deps_set = get_all_deps("bla")
        self.assertEqual(deps_set, set())

        deps_set = get_all_deps("pandas")
        self.assertIn("numpy", deps_set)

    def test_requirements_path(self):
        with patch("luisy.code_inspection.inspect.getfile") as mock_getfile:
            mock_getfile.return_value = "/c/this/will/never/be/found.py"
            with patch("luisy.code_inspection.os.path.exists") as mock_exists:
                mock_exists.return_value = False
                with self.assertRaises(Exception):
                    get_requirements_path("mocked_anyway")
            calls = mock_exists.call_args_list
            self.assertEqual(calls[0], call('/c/this/will/never/be/requirements.txt'))
            self.assertEqual(calls[1], call('/c/this/will/never/requirements.txt'))
            self.assertEqual(calls[-2], call('/c/requirements.txt'))
            self.assertEqual(calls[-1], call('requirements.txt'))

    def test_get_requirements_dict(self):
        with tempfile.NamedTemporaryFile() as tmpfile:
            tmpfile.write(b"luisy==0.0.1\nnumpy>0.7.5\npytest-timeout>=1.3.0,<1.4.0")
            tmpfile.seek(0)
            reqs = get_requirements_dict(tmpfile.name)
            self.assertEqual(reqs["luisy"], "luisy==0.0.1")
            self.assertEqual(reqs['pytest-timeout'], 'pytest-timeout>=1.3.0,<1.4.0')

    def test_create_deps_map(self):
        req_dict = {"pandas": "pandas==1.2.3",
                    "luisy": "luisy>5.0.1"}
        pandas_deps = {"numpy"}
        luisy_deps = {"PyYaml", "python-dateutil", "pytest-cov"}

        with patch("luisy.code_inspection.get_all_deps") as mock_deps:
            mock_deps.side_effect = [pandas_deps, luisy_deps]
            deps_map = create_deps_map(req_dict)
        self.assertEqual(deps_map["pandas"], ["numpy"])
        deps_map["luisy"].sort()  # set order can not be controlled, need to sort!
        self.assertEqual(deps_map["luisy"], ['pytest_cov', 'python_dateutil', 'pyyaml'])

    def test_map_imports_to_requirements(self):
        imported_packages = ["numpy", "dateutil", "yaml", "skimage", "some_ml_framework"]
        requirements_dict = {
            "luisy": "luisy==0.0.1",
            "pandas": "pandas>=0.8.15",
            "not-used": "not-used==1.2.3",
            "some_ml_framework": "some_ml_framework==45.47.5"
        }
        deps_map = {
            "luisy": ['pytest_cov', 'python_dateutil', 'pyyaml', 'scikit_image'],
            "pandas": ["numpy"], "some_ml_framework": ["pandas", "numpy"],
            "not_used": ["really_not_used"]
        }
        pypi_mapping = {
            'cv2': 'opencv_python',
            '_yaml': 'pyyaml',
            'yaml': 'pyyaml',
            'dateutil': 'python_dateutil',
            'skimage': 'scikit_image',
            'caffe2': 'torch',
            'torch': 'torch'
        }
        mocked_mapper = Mock()
        mocked_mapper.side_effect = lambda x: pypi_mapping[x] if x in pypi_mapping else x
        with patch("luisy.code_inspection.make_pypi_mapper") as mocked_maker:
            mocked_maker.return_value = mocked_mapper
            df_res = map_imports_to_requirements(imported_packages, requirements_dict, deps_map)
        self.assertListEqual(
            df_res.package.tolist(),
            [
                "numpy",
                "numpy",
                "python_dateutil",
                "pyyaml",
                "scikit_image",
                "some_ml_framework"
            ]
        )
        self.assertListEqual(
            df_res.requirement_line.tolist(),
            [
                "pandas>=0.8.15",
                "some_ml_framework==45.47.5",
                "luisy==0.0.1",
                "luisy==0.0.1",
                "luisy==0.0.1",
                "some_ml_framework==45.47.5"
            ]
        )

        # imported package that cannot be mapped to any package in requirements file
        deps_map["luisy"].remove('scikit_image')
        with patch("luisy.code_inspection.make_pypi_mapper") as mocked_maker:
            mocked_maker.return_value = mocked_mapper
            with self.assertRaises(Exception) as ctx:
                map_imports_to_requirements(imported_packages, requirements_dict, deps_map)
            self.assertEqual(
                str(ctx.exception),
                "Imported packages ['scikit_image'] could not be "
                "mapped to requirements file"
            )

        # empty case
        df_res = map_imports_to_requirements([], requirements_dict, deps_map)
        self.assertEqual(len(df_res), 0)


class PypiNamesHandling(unittest.TestCase):
    def setUp(self):
        self.dist_data = {
            'psutil': ['psutil'],
            'opencv-python': ['cv2'],
            'PyYAML': ['_yaml', 'yaml'],
            'python-dateutil': ['dateutil'],
            'scikit-image': ['skimage'],
            'torch': ['caffe2', 'torch'],
            'Jinja2': ['jinja2']
        }
        self.dist_path_instance_mock = Mock()
        mock_distributions = []
        for k in self.dist_data:
            mock_dist = Mock()
            mock_dist.name = k
            mock_dist.modules = self.dist_data[k]
            mock_distributions.append(mock_dist)
        self.dist_path_instance_mock.get_distributions = lambda: mock_distributions

    def test_get_irregular_pypi_names(self):
        with patch("luisy.code_inspection.DistributionPath") as mock_dp:
            mock_dp.return_value = self.dist_path_instance_mock
            res = get_irregular_pypi_names()
        self.assertDictEqual(
            res,
            {
                'cv2': 'opencv_python',
                '_yaml': 'pyyaml',
                'yaml': 'pyyaml',
                'dateutil': 'python_dateutil',
                'skimage': 'scikit_image',
                'caffe2': 'torch',
                'torch': 'torch'
            }
        )

    def test_make_pypi_mapper(self):
        with patch("luisy.code_inspection.DistributionPath") as mock_dp:
            mock_dp.return_value = self.dist_path_instance_mock
            pypi_mapper = make_pypi_mapper()
            self.assertEqual(pypi_mapper("test_test"), "test_test")
            self.assertEqual(pypi_mapper("caffe2"), "torch")
            self.assertEqual(pypi_mapper("dateutil"), "python_dateutil")
