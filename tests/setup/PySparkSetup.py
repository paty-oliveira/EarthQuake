import unittest
from pyspark.sql import SparkSession
import logging


class PySparkSetup(unittest.TestCase):
    @classmethod
    def suppress_py4j_logging(cls):
        logger = logging.getLogger("py4j")
        logger.setLevel(logging.ERROR)

    @classmethod
    def create_testing_spark_session(cls):
        return SparkSession \
            .builder \
            .master("local") \
            .appName("my-local-testing-pyspark-context") \
            .config("spark.sql.shuffle.partitions", "1") \
            .getOrCreate()

    @classmethod
    def setUpClass(cls):
        cls.suppress_py4j_logging()
        cls.spark = cls.create_testing_spark_session()
        cls.test_data = cls.spark.createDataFrame([
            (1704567252, "California", 0.82, "Automatic", [-116.8, 33.3333333, 12.04], None),
            (1391707828, "Alaska", 1.1, None, [-148.942, 64.9081, 10.6], "green"),
            (1435498694, "Chile", 4.9, "Reviewed", [-70.6202, -21.4265, 52.24], None),
            (1609879110, "Hawaii", 2.0099, "Automatic", [-155.429000854492, 19.2180004119873, 33.2999992370605],
             "yellow"),
            (1224994646, "Indonesia", 4.8, "Reviewed", [126.419, 0.2661, 10], "green"),
            (1801059964, "Nevada", 0.5, "Automatic", [-116.242, 36.7564, 0.8], None),
            (1262739669, "Arkansas", 1.9, "Reviewed", [-91.4295, 35.863, 16.41], "green"),
            (1890118874, "Montana", 1.33, "Reviewed", [-110.434, 44.4718333, 2.21], None),
            (1025727100, "Oklahoma", 1.58, "Reviewed", [-98.53233333, 36.57083333, 6.31], None),
            (1834567116, "Idaho", 2.6, "Reviewed", [-115.186, 44.2666, 10], "green")
        ], ["date", "place", "mag", "status", "coordinates", "alert"]
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()


if __name__ == '__main__':
    unittest.main()
