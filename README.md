spark.sql("SHOW TABLES IN staging").show()

spark.table("staging.stg_customers").show(5)
spark.table("staging.stg_products").show(5)
spark.table("staging.stg_invoices").show(5)
spark.table("staging.stg_invoice_line_items").show(5)
spark.table("staging.stg_payments").show(5)
spark.table("staging.stg_exchange_rates").show(5)
spark.table("staging.stg_regions").show(5)

from pyspark.sql.functions import current_timestamp

# Create schema
spark.sql("CREATE SCHEMA IF NOT EXISTS staging")

# Customers
customers = spark.table("azure_blob_storage.src_customers") \
    .withColumn("ingestion_ts", current_timestamp())

customers.write.mode("overwrite").saveAsTable("staging.stg_customers")

# Products
products = spark.table("azure_blob_storage.src_products") \
    .withColumn("ingestion_ts", current_timestamp())

products.write.mode("overwrite").saveAsTable("staging.stg_products")

# Invoices
invoices = spark.table("azure_blob_storage.src_invoices") \
    .withColumn("ingestion_ts", current_timestamp())

invoices.write.mode("overwrite").saveAsTable("staging.stg_invoices")

# Invoice Line Items
invoice_line_items = spark.table("azure_blob_storage.src_invoice_line_items") \
    .withColumn("ingestion_ts", current_timestamp())

invoice_line_items.write.mode("overwrite").saveAsTable("staging.stg_invoice_line_items")

# Payments
payments = spark.table("azure_blob_storage.src_payments") \
    .withColumn("ingestion_ts", current_timestamp())

payments.write.mode("overwrite").saveAsTable("staging.stg_payments")

# Exchange Rates
exchange_rates = spark.table("azure_blob_storage.src_exchange_rates") \
    .withColumn("ingestion_ts", current_timestamp())

exchange_rates.write.mode("overwrite").saveAsTable("staging.stg_exchange_rates")

# Regions
regions = spark.table("azure_blob_storage.src_regions") \
    .withColumn("ingestion_ts", current_timestamp())

regions.write.mode("overwrite").saveAsTable("staging.stg_regions")

# Check tables
spark.sql("SHOW TABLES IN azure_blob_storage").show()

# Load tables
customers = spark.table("azure_blob_storage.src_customers")
products = spark.table("azure_blob_storage.src_products")
invoices = spark.table("azure_blob_storage.src_invoices")
invoice_line_items = spark.table("azure_blob_storage.src_invoice_line_items")
payments = spark.table("azure_blob_storage.src_payments")
exchange_rates = spark.table("azure_blob_storage.src_exchange_rates")
regions = spark.table("azure_blob_storage.src_regions")

# Count rows
print("customers:", customers.count())
print("products:", products.count())
print("invoices:", invoices.count())
print("invoice_line_items:", invoice_line_items.count())
print("payments:", payments.count())
print("exchange_rates:", exchange_rates.count())
print("regions:", regions.count())

# Show sample
customers.show(5)
products.show(5)
invoices.show(5)
invoice_line_items.show(5)
payments.show(5)
exchange_rates.show(5)
regions.show(5)