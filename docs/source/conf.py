import datetime as dt
import os
import sys

sys.path.insert(0, os.path.abspath("../.."))

extensions = ["sphinx.ext.autodoc", "sphinx.ext.intersphinx", "sphinx.ext.viewcode"]

project = "myscaledb"
author = "Mochi Xu"
copyright = "Moqi Inc. and contributors {0:%Y}".format(dt.datetime.utcnow())
version = "2.0.0"
source_suffix = ".rst"
master_doc = "index"
pygments_style = "default"
html_theme = "sphinx_rtd_theme"
html_static_path = ["_static"]

html_theme_options = {
    "description": (
        "Async and sync http(s) ClickHouse client for python 3.6+ "
        "with types converting and streaming support"
    ),
    "show_powered_by": False,
    "display_version": True,
}
html_title = "myscaledb Documentation"
html_short_title = "myscaledb"
