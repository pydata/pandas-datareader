"""Sphinx configuration for the pandas-datareader documentation."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas_datareader as pdr

SOURCE_DIR = Path(__file__).parent

# -- General configuration ------------------------------------------------

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.intersphinx",
    "sphinx.ext.extlinks",
    "sphinx.ext.napoleon",
    "sphinx.ext.todo",
    "IPython.sphinxext.ipython_directive",
    "IPython.sphinxext.ipython_console_highlighting",
]

project = "pandas-datareader"
author = "The PyData Development Team"
copyright = f"{date.today():%Y}, {author}"

release = pdr.__version__
version = release.split("+", 1)[0]
if ".dev" in version and "+" in release:
    commits_since_tag = version.rsplit(".dev", 1)[1]
    commit_hash = release.split("+", 1)[1]
    version = f"{version} (+{commits_since_tag}, {commit_hash})"

with (SOURCE_DIR / "_version.txt").open("w", encoding="utf-8") as version_file:
    doc_date = date.today().strftime("%B %d, %Y")
    version_file.write(f"Version: **{version}** Date: **{doc_date}**\n")

root_doc = "index"
master_doc = root_doc
source_suffix = {".rst": "restructuredtext"}
language = "en"
templates_path = ["_templates"] if (SOURCE_DIR / "_templates").exists() else []
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]
pygments_style = "default"
todo_include_todos = True

intersphinx_mapping = {"python": ("https://docs.python.org/3", None)}

extlinks = {
    "issue": ("https://github.com/pydata/pandas-datareader/issues/%s", "GH %s"),
    "wiki": ("https://github.com/pydata/pandas-datareader/wiki/%s", "wiki %s"),
}


# -- Options for HTML output ----------------------------------------------

html_theme = "pydata_sphinx_theme"
html_theme_options = {
    "github_url": "https://github.com/pydata/pandas-datareader",
    "logo": {
        "alt_text": "pandas-datareader",
        "image_light": "_static/images/pandas-datareader-plain.svg",
        "image_dark": "_static/images/pandas-datareader-plain-dark.svg",
    },
    "navigation_with_keys": True,
}
html_static_path = ["_static"]


# -- Options for HTMLHelp output ------------------------------------------

htmlhelp_basename = "pandas-datareaderdoc"


# -- Options for LaTeX output ---------------------------------------------

latex_elements = {}
latex_documents = [
    (
        root_doc,
        "pandas-datareader.tex",
        "pandas-datareader Documentation",
        "pydata",
        "manual",
    )
]


# -- Options for manual page output ---------------------------------------

man_pages = [
    (root_doc, "pandas-datareader", "pandas-datareader Documentation", [author], 1)
]


# -- Options for Texinfo output -------------------------------------------

texinfo_documents = [
    (
        root_doc,
        "pandas-datareader",
        "pandas-datareader Documentation",
        author,
        "pandas-datareader",
        "One line description of project.",
        "Miscellaneous",
    )
]
