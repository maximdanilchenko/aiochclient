.. _install-docs:

Building the Docs
=================

::

   $ pip install -r dev-requirements/dev-requirements.txt
   $ sphinx-build -b html -d build/doctrees docs/source build/html


The files will be saved to the `build` directory. You can view these files
locally by simply opening `build/html/index.html`.
