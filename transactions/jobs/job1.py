import logging
import os

from pyspark.sql.functions import col, lit, concat, when, lpad, split
import transactions.config.properties
from transactions.config import properties
from delta.tables import DeltaTable


def run_transformations(spark):
    logging.basicConfig(level=logging.INFO)
    dir_path = properties.get_dir_path()
    clients_df_bronze = (spark.read.format("csv")
                         .option("header", "true")
                         .option("delimiter", ",")
                         .load(dir_path + "/clients_1.csv"))
    stores_df_bronze = (spark.read.format("csv")
                        .option("header", "true")
                        .option("delimiter", ",")
                        .load(dir_path + "/stores_1.csv")
                        .withColumn("lat", col("latlng").getItem(0))
                        .withColumn("lng", col("latlng").getItem(1))
                        .drop("latlng"))

    products_df_bronze = (spark.read.format("csv")
                          .option("header", "true")
                          .option("delimiter", ",")
                          .load(dir_path + "/products_1.csv"))

    file_path = os.environ.get("file_path")
    print(f"file_path: {file_path}")
    transactions_df_bronze = (spark.read.format("csv")
                              .option("header", "true")
                              .option("delimiter", ",")
                              .load(file_path))

    transactions_df_silver = (transactions_df_bronze
                              .join(clients_df_bronze.select("id", "account_id"),
                                    transactions_df_bronze.client_id == clients_df_bronze.id)
                              .withColumn("real_hour", lpad(col("hour").cast("string"), 2, "0"))
                              .withColumn("real_minutes", lpad(col("minute").cast("string"), 2, "0"))
                              .withColumn("complete_data",
                                          concat(lit("("), col("date"), lit(","), col("real_hour"), lit(","),
                                                 col("real_minutes"), lit(")"))))
    stores_df_silver = (stores_df_bronze
                 .withColumn("lat", split(col("latlng"), ',')[0])
                 .withColumn("lng", split(col("latlng"), ',')[1])
                 .drop("latlng"))

    transactions_table = DeltaTable.forName(spark, "dev.transactions")
    (transactions_table.alias("t").merge(transactions_df_silver.alias("s"),"t.transaction_id = s.transaction_id")
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute())
