import dlt
from pyspark.sql.functions import col

@dlt.table
def ccda_inserts():
    return (
        spark.readStream
        .format("delta")
        .option("readChangeFeed", "true")
        .table("ccda_bronze")
        .filter(col("_change_type").isin("insert", "update_postimage"))
    )