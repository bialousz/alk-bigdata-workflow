import sys

from pyspark.sql import SparkSession
import pyspark.sql.functions as f

spark = SparkSession.builder.getOrCreate()
spark.conf.set("temporaryGcsBucket","ab-bigdata-temp")

accounts = spark.read.json(sys.argv[1])
customers = spark.read.json(sys.argv[2])
transactions = spark.read.json(sys.argv[3])

transactions_grouped = transactions.groupBy('account_id').sum('amount')
transactions_rounded = transactions_grouped.withColumn("rounded_transaction", f.round(f.col("sum(amount)"), 2))

accounts_rounded = accounts.withColumn("rounded_account_balance", f.round(f.col("account_balance"), 2))

accounts_rounded_with_transactions = accounts_rounded.join(transactions_rounded, transactions_rounded.account_id == accounts_rounded.id)

customers_with_all_data = accounts_rounded_with_transactions.join(customers, accounts_rounded_with_transactions.customer_id == customers.id)

customers_with_all_data.createOrReplaceTempView("customers_with_all_data")

suspicious_customers = spark.sql("""SELECT DISTINCT name, surname, account_id from customers_with_all_data where rounded_account_balance != rounded_transaction""")

suspicious_customers.write.format("com.google.cloud.spark.bigquery").option("table", sys.argv[4]).save()



