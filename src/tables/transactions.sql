CREATE EXTERNAL TABLE dev.transactions (
  transaction_id STRING,
  client_id STRING,
  date STRING,
  hour STRING,
  minute STRING,
  product_id STRING,
  quantity STRING,
  store_id STRING,
  id STRING,
  account_id STRING,
  real_hour STRING,
  real_minutes STRING,
  complete_data STRING
)
USING DELTA
PARTITIONED BY (date)
LOCATION 'abfss://mrstoragecontainer@mystorageaccountmr001.dfs.core.windows.net/dev/output/transactions';
