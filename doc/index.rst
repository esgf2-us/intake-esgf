.. image:: _static/logo.png
   :align: left
   :width: 40%

.. Silly I know, but these push the title so it does not wrap with the logo.

|
|
|
|
|
|
|
|
|
|
|
|

Documentation for intake-esgf
=============================

``intake-esgf`` is an `intake`_ and `intake-esm`_ *inspired* package under
development in ESGF2. The data catalog is populated by querying a number of
index nodes and puts together a global view of where the datasets may be found.
If you are familiar with the interface for ``intake-esm``, then using this
package should be straightforward.

Installing
----------

``intake-esgf`` can be installed using ``pip``

    >>> pip install git+https://github.com/esgf2-us/intake-esgf

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: User Guide

   beginner
   quickstart

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Features

   configure
   measures
   dictkeys
   logging
   modelgroups
   operators
   reproduce

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Communicate

   GitHub repository <https://github.com/esgf2-us/intake-esgf>
   Bugs or Suggestions <https://github.com/esgf2-us/intake-esgf/issues>

.. _intake: https://github.com/intake/intake
.. _intake-esm: https://github.com/intake/intake-esm
