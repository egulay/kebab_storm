import unittest
from unittest.mock import Mock, patch

from util.spark_util import managed_spark_session


class ManagedSparkSessionTest(unittest.TestCase):
    @patch('util.spark_util.create_spark_session')
    def test_stops_session_after_success(self, create_spark_session):
        spark = Mock()
        create_spark_session.return_value = spark

        with managed_spark_session('test-app') as active_session:
            self.assertIs(spark, active_session)

        spark.stop.assert_called_once_with()

    @patch('util.spark_util.create_spark_session')
    def test_stops_session_after_failure(self, create_spark_session):
        spark = Mock()
        create_spark_session.return_value = spark

        with self.assertRaisesRegex(RuntimeError, 'job failed'):
            with managed_spark_session('test-app'):
                raise RuntimeError('job failed')

        spark.stop.assert_called_once_with()


if __name__ == '__main__':
    unittest.main()
