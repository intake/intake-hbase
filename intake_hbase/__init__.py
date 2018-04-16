from intake.source import base
from ._version import get_versions
__version__ = get_versions()['version']
del get_versions


class HBasePlugin(base.Plugin):
    """Plugin for HBase reader"""

    def __init__(self):
        super(HBasePlugin, self).__init__(name='hbase',
                                          version=__version__,
                                          container='python',
                                          partition_access=True)

    def open(self, table, connection, partitions=None, **kwargs):
        """
        Create HBaseSource instance

        Parameters
        ----------
        table, connection, qargs, partitions
            See ``HBaseSource``.
        """
        from intake_hbase.source import HBaseSource
        base_kwargs, source_kwargs = self.separate_base_kwargs(kwargs)
        qargs = source_kwargs.pop('qargs', {})
        return HBaseSource(table, connection, partitions, qargs,
                           metadata=base_kwargs['metadata'],
                           **source_kwargs)
