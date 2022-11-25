import os
import sys
import inspect
import shutil
from sphinx.ext import apidoc

__location__ = os.path.join(os.getcwd(), os.path.dirname(
    inspect.getfile(inspect.currentframe())))

output_dir = os.path.join(__location__, "api")
module_dir = os.path.join(__location__, "../../luisy")

try:
    shutil.rmtree(output_dir)
except FileNotFoundError:
    pass

try:
    import sphinx
    from distutils.version import LooseVersion

    cmd_line_template = "sphinx-apidoc -f -o {outputdir} {moduledir}"
    cmd_line = cmd_line_template.format(outputdir=output_dir, moduledir=module_dir)

    args = cmd_line.split(" ")
    if LooseVersion(sphinx.__version__) >= LooseVersion('1.7'):
        args = args[1:]

    apidoc.main(args)
except Exception as e:
    print("Running `sphinx-apidoc` failed!\n{}".format(e))

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.intersphinx',
    'sphinx.ext.todo',
    'sphinx.ext.autosummary',
    'sphinx.ext.viewcode',
    'sphinx.ext.coverage',
    'sphinx.ext.doctest',
    'sphinx.ext.ifconfig',
    'sphinx.ext.mathjax',
    'sphinx.ext.napoleon',
    'sphinx.ext.inheritance_diagram'
]
source_suffix = '.rst'
todo_include_todos = True
todo_link_only = True
inheritance_graph_attrs = dict(
    rankdir="TB",  # from top to bottom
    size='"10.0, 20.0"',
    fontsize=14, ratio='compress')

inheritance_node_attrs = dict(
    shape='ellipse',
    fontsize=14,
    height=0.75,
    color='dodgerblue1',
    style='filled')

# Generate an autosummary
autosummary_generate = True
master_doc = 'index'
project = u'luisy'
copyright = u'2021-2022, Robert Bosch GmbH'
exclude_patterns = ['_build']
pygments_style = 'sphinx'
html_theme = 'sphinx_rtd_theme'
html_theme_options = {
    'canonical_url': '',
    'analytics_id': '',
    'logo_only': False,
    'display_version': True,
    'prev_next_buttons_location': 'bottom',
    'style_external_links': False,
    'collapse_navigation': True,
    'sticky_navigation': True,
    'navigation_depth': 4,
    'includehidden': True,
    'titles_only': False
}
try:
    from luisy import __version__ as version
except ImportError:
    pass
else:
    release = version

html_logo = ""
htmlhelp_basename = 'luisy-doc'

python_version = '.'.join(map(str, sys.version_info[0:2]))
intersphinx_mapping = {
    'sphinx': ('http://www.sphinx-doc.org/en/master', None),
    'python': ('https://docs.python.org/' + python_version, None),
    'matplotlib': ('https://matplotlib.org', None),
    'numpy': ('https://numpy.org/doc/stable/', None),
    'pandas': ('https://pandas.pydata.org/pandas-docs/stable', None),
}
