from intake.source import base
from . import __version__


class HBaseSource(base.DataSource):
    """Execute a query on HBASE

    Parameters
    ----------
    table: str
        HBase table to query. If within a project/namespace, either use the
        full table name, e.g., ``myproject_mytable`` or use
        ``table_prefix='myproject'`` in the connection parameters.
    connection: str or dict
        See happybase connection arguments
        https://happybase.readthedocs.io/en/latest/api.html#happybase.Connection
    divisions: list or None
        Partition key boundaries. If None, will have one partition for the
        whole table. The number of partitions will be ``len(divisions) - 1``.
    qargs: dict or None
        Further arguments to ``table.scan``, see
        https://happybase.readthedocs.io/en/latest/api.html#happybase.Table.scan
    """
    container = 'python'
    name = 'hbase'
    version = __version__
    partition_access = True

    def __init__(self, table, connection, divisions=None, qargs=None,
                 metadata=None):
        if isinstance(connection, str):
            self.conn = {'host': connection}
        else:
            self.conn = connection
        self.table = table
        self.metadata = metadata or {}
        self.divisions = divisions
        self.qargs = qargs or {}
        self._schema = None
        super(HBaseSource, self).__init__(metadata=metadata)
        if self.divisions is not None:
            self.npartitions = len(self.divisions) - 1
        else:
            self.npartitions = 1

    def _get_schema(self):
        return base.Schema(datashape=None,
                           dtype=None,
                           shape=None,
                           npartitions=self.npartitions,
                           extra_metadata={})

    def _do_query(self, start, end):
        import happybase
        conn = happybase.Connection(**self.conn)
        table = conn.table(self.table)
        return list(table.scan(row_start=start, row_stop=end, **self.qargs))

    def _get_partition(self, i):
        if self.divisions:
            return self._do_query(self.divisions[i], self.divisions[i+1])
        else:
            return self._do_query(None, None)

    def read(self):
        return sum([self._get_partition(i)
                    for i in range(self.npartitions)], [])
