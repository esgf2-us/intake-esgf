import datetime
import os
import sys

import sphinx_autosummary_accessors

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
# sys.path.insert(0, os.path.abspath('.'))

cwd = os.getcwd()
parent = os.path.dirname(cwd)
sys.path.insert(0, parent)

# -- General configuration -----------------------------------------------------

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.viewcode",
    "sphinx.ext.autosummary",
    "sphinx.ext.doctest",
    "sphinx.ext.intersphinx",
    "sphinx.ext.extlinks",
    "numpydoc",
    "sphinx_autosummary_accessors",
    "IPython.sphinxext.ipython_directive",
    "myst_nb",
    "sphinx_copybutton",
]

extlinks = {
    "issue": ("https://github.com/esgf2-us/intake-esgf/issues%s", "GH#%s"),
    "pr": ("https://github.com/esgf2-us/intake-esgf/pull/%s", "GH#%s"),
}

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates", sphinx_autosummary_accessors.templates_path]

# The suffix of source filenames.
source_suffix = ".rst"

# Enable notebook execution
# https://nbsphinx.readthedocs.io/en/0.4.2/never-execute.html
# nbsphinx_execute = 'auto'
# Allow errors in all notebooks by
# nbsphinx_allow_errors = True

# Disable cell timeout
nbsphinx_timeout = -1
nb_execution_timeout = -1
nbsphinx_prolog = ""

# The encoding of source files.
# source_encoding = 'utf-8-sig'

# The master toctree document.
master_doc = "index"

# General information about the project.
project = "intake-esgf"
current_year = datetime.datetime.now().year
copyright = f"2023-{current_year}, intake-esgf Developers"
author = "intake-esgf Developers"

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
exclude_patterns = ["_build"]

# The name of the Pygments (syntax highlighting) style to use.
pygments_style = "igor"

# -- Options for HTML output ---------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
html_theme = "furo"

# Theme options are theme-specific and customize the look and feel of a theme
# further.  For a list of options available for each theme, see the
# documentation.
css_vars = {
    "admonition-font-size": "0.9rem",
    "font-size--small": "92%",
    "font-size--small--2": "87.5%",
}

html_theme_options = dict(
    sidebar_hide_name=True,
    light_css_variables=css_vars,
    dark_css_variables=css_vars,
)

html_context = {
    "github_user": "xarray-contrib",
    "github_repo": "cf-xarray",
    "github_version": "main",
    "doc_path": "doc",
}
html_logo = "_static/logo.png"

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ["_static"]
html_css_files = ["style.css"]

# Output file base name for HTML help builder.
htmlhelp_basename = "intake_esgfdoc"

# -- Options for manual page output --------------------------------------------

# One entry per manual page. List of tuples
# (source start file, name, description, authors, manual section).
man_pages = [("index", "intake_esgf", "intake_esgf Documentation", [author], 1)]

# If true, show URL addresses after external links.
intersphinx_mapping = {
    "python": ("https://docs.python.org/3/", None),
    "xarray": ("https://xarray.pydata.org/en/stable/", None),
    "pandas": ("https://pandas.pydata.org/pandas-docs/stable", None),
}

autosummary_generate = True

autodoc_typehints = "description"
autodoc_typehints_description_target = "documented"
autodoc_default_options = {
    "members": True,
    "undoc-members": True,
    "private-members": True,
}
napoleon_use_param = True
napoleon_use_rtype = True

numpydoc_show_class_members = False
# Report warnings for all validation checks except the ones listed after "all"
numpydoc_validation_checks = {"all", "ES01", "EX01", "SA01", "SA04"}
# don't report on objects that match any of these regex
numpydoc_validation_exclude = {
    r"\.__repr__$",
}
