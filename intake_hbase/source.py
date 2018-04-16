from intake.source import base
import happybase


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
        Partition key boundaries. If None, will have one partition for the whole
        table. The number of partitions will be ``len(divisions) - 1``.
    qargs: dict or None
        Further arguments to ``table.scan``, see
        https://happybase.readthedocs.io/en/latest/api.html#happybase.Table.scan
    """
    container = 'python'

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
        super(HBaseSource, self).__init__(container=self.container,
                                          metadata=metadata)

    def _get_schema(self):
        if self.divisions is not None:
            npartitions = len(self.divisions) - 1
        else:
            npartitions = 1
        return base.Schema(datashape=None,
                           dtype=None,
                           shape=None,
                           npartitions=npartitions,
                           extra_metadata={})

    def _do_query(self, start, end):
        conn = happybase.Connection(**self.conn)
        table = conn.table(self.table)
        return list(table.scan(row_start=start, row_stop=end, **self.qargs))

    def _get_partition(self, i):
        if self.divisions:
            return self._do_query(self.divisions[i], self.divisions[i+1])
        else:
            return self._do_query(None, None)
