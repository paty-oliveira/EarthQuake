import unittest
import logging
from pyspark.sql import SparkSession


class PipelinePySparkSetup(unittest.TestCase):
    @classmethod
    def suppress_py4j_logging(cls):
        logger = logging.getLogger("py4j")
        logger.setLevel(logging.ERROR)

    @classmethod
    def spark_session(cls):
        return SparkSession\
            .builder\
            .master("local")\
            .appName("my-local-pipeline-testing")\
            .config("spark.sql.shuffle.partitions", "1")\
            .getOrCreate()

    @classmethod
    def mock_api_data(cls):
        return ""

    @classmethod
    def setUpClass(cls):
        cls.suppress_py4j_logging()
        cls.spark = cls.spark_session()
        cls.mock_data = cls.mock_api_data()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()


class TestSparkDataPipeline(PipelinePySparkSetup):
    def test_something(self):
        self.assertEqual(True, False)


if __name__ == '__main__':
    unittest.main()
