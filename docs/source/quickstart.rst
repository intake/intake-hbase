Quickstart
==========

``intake-hbase`` provides quick and easy access to tabular data stored in
Apache `HBase`_

.. _hbase: https://hbase.apache.org/

This plugin reads hbase query results without random access: there is only ever
a single partition.

Installation
------------

To use this plugin for `intake`_, install with the following command::

   conda install -c intake intake-hbase

.. _intake: https://github.com/ContinuumIO/intake

Usage
-----

Ad-hoc
~~~~~~

After installation, the function ``intake.open_hbase``
will become available. It can be used to execute queries on the hbase
server, and download the results as a list of dictionaries.

Three parameters are of interest when defining a data source:

- query: the query to execute, which can be defined either using `Lucene`_ or
  `JSON`_ syntax, both of which are to be provided as a string.


.. _Lucene: https://www.elastic.co/guide/en/kibana/current/lucene-query.html

Creating Catalog Entries
~~~~~~~~~~~~~~~~~~~~~~~~

To include in a catalog, the plugin must be listed in the plugins of the catalog::

   plugins:
     source:
       - module: intake_hbase

and entries must specify ``driver: hbase``.



Using a Catalog
~~~~~~~~~~~~~~~

