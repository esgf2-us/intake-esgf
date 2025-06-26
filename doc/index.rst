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

Announcement
------------

ESGF is in the process of decomissioning the old index technology (Solr). While this process takes place, you may find some services may be broken or disappear completely. The information in the US-based Solr indices has been moved into a Globus (ElasticSearch) index which is this package's default index.

Installing
----------

``intake-esgf`` can be installed using ``pip``:

    >>> pip install intake-esgf

or through ``conda-forge``

    >>> conda install -c conda-forge intake-esgf

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: User Guide

   beginner
   intake
   quickstart
   faq

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Features

   measures
   stream
   time
   modelgroups
   configure
   paths
   reproduce
   dictkeys
   logging

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Experimental

   globus

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Communicate

   GitHub repository <https://github.com/esgf2-us/intake-esgf>
   Bugs or Suggestions <https://github.com/esgf2-us/intake-esgf/issues>

.. _intake: https://github.com/intake/intake
.. _intake-esm: https://github.com/intake/intake-esm
