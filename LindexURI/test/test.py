import unittest
from LindexURI import LindexURI
from collections import OrderedDict


class TestLindexURI(unittest.TestCase):
    good_hive_uri = "hive://datalake/subscribers?dt=2012&service=pacman"
    good_hive_uri2 = "hive://datalake/subscribers?country=IT&service=pacman&dt=2012"
    bad_hive_uri = "hive://datalake?dt=2012"
    good_unpartitioned_hive_uri = "hive://datalake/subscribers"
    good_hdfs_path = "hdfs://hdfs-prod/datalake.db/tablename/country=IT/service=pacman/dt=201212"

    def test_isValid(self):
        self.assertTrue(LindexURI.isValid(self.good_hive_uri))
        self.assertFalse(LindexURI.isValid(self.bad_hive_uri))

    def test_isPartitioned(self):
        l = LindexURI(self.good_unpartitioned_hive_uri)
        self.assertFalse(l.isPartitioned())
        l = LindexURI(self.good_hive_uri)
        self.assertTrue(l.isPartitioned())

    def test_getPartitions(self):
        l = LindexURI(self.good_hive_uri)
        expected = OrderedDict((('dt', '2012'), ('service', 'pacman')))
        self.assertEquals(l.getPartitions(), expected)
        l = LindexURI(self.good_hive_uri2)
        expected = OrderedDict((('country', 'IT'), ('service', 'pacman'), ('dt', '2012')))
        self.assertEquals(l.getPartitions(), expected)

    def test_database(self):
        l = LindexURI(self.good_hive_uri2)
        self.assertEqual('datalake', l.getDatabase())
        self.assertRaises(RuntimeError, LindexURI, self.bad_hive_uri)

    def test_getTable(self):
        l = LindexURI(self.good_hive_uri2)
        self.assertEqual('subscribers', l.getTable())

    def test_getSchema(self):
        l = LindexURI(self.good_hive_uri2)
        self.assertEqual('hive', l.getSchema())

    def test_getHostName(self):
        l = LindexURI(self.good_hdfs_path)
        self.assertEqual('hdfs-prod', l.getHDFSHostname())

    def test_getHdfsPath(self):
        l = LindexURI(self.good_hdfs_path)
        self.assertEqual('/datalake.db/tablename/country=IT/service=pacman/dt=201212', l.getHDFSPath())

    def test_looksPartitioned(self):
        l = LindexURI(self.good_hdfs_path)
        self.assertTrue(l.looksPartitioned())

    def test_getPartionsAsHDFSPath(self):
        l = LindexURI(self.good_hive_uri2)
        self.assertEqual('country=IT/service=pacman/dt=2012', l.getPartitionsAsHDFSPath())


    def test_gestHDFSAsPartition(self):
        l = LindexURI(self.good_hdfs_path)
        partitions, root = l.getHDFSPathAsPartition()
        self.assertEqual('/datalake.db/tablename', root)
        self.assertEquals(OrderedDict((('country', 'IT'), ('service', 'pacman'), ('dt', '201212'))), partitions)