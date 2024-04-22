# Databricks notebook source
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType

schema= StructType([
    StructField("product_id", IntegerType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_date", DateType(), True),
    StructField("location", StringType(), True),
    StructField("source_order", StringType(), True)
])

schema_menu = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("price", StringType(), True)
])






# COMMAND ----------

from pyspark.sql.functions import month, year, quarter


# File location and type
file_location_menu = "/FileStore/tables/menu_csv-8.txt"
file_location_sales = "/FileStore/tables/sales_csv-8.txt"

file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "false"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df_sales = spark.read.format(file_type) \
  .option("inferSchema", infer_schema).schema(schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location_sales)

df_menu = spark.read.format(file_type) \
  .option("inferSchema", infer_schema).schema(schema_menu) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location_menu)

#display(df_sales)
display(df_menu)

#Deriving year, month, and quarter from date data
df_sales = df_sales.withColumn("order_year",year(df_sales.order_date))
df_sales = df_sales.withColumn("order_month",month(df_sales.order_date))
df_sales = df_sales.withColumn("order_quarter",quarter(df_sales.order_date))


display(df_sales)




# COMMAND ----------

#calculating the total amount spent by each customer
total_amount_spent = (df_sales.join(df_menu, 'product_id').groupBy('customer_id').agg({'price':'sum'}).orderBy('customer_id'))

display(total_amount_spent)

# COMMAND ----------

#calculating the total amount spent by each food category
total_amount_spent_food_category = (df_sales.join(df_menu, 'product_id').groupBy('product_name').agg({'price':'sum'}).orderBy('product_name'))

display(total_amount_spent)

# COMMAND ----------

#calculating the total amount of sales by month
total_amount_spent_month = (df_sales.join(df_menu, 'product_id').groupBy('order_month').agg({'price':'sum'}).orderBy('order_month'))

display(total_amount_spent_month)

# COMMAND ----------

 #calculating the total amount of sales by year
total_amount_spent_year = (df_sales.join(df_menu, 'product_id').groupBy('order_year').agg({'price':'sum'}).orderBy('order_year'))

display(total_amount_spent_year)

# COMMAND ----------

#calculating the total amount of sales by month
total_amount_spent_quarter = (df_sales.join(df_menu, 'product_id').groupBy('order_quarter').agg({'price':'sum'}).orderBy('order_quarter'))

display(total_amount_spent_quarter)

# COMMAND ----------

#How many times each product is purchased
from pyspark.sql.functions import count

most_df = df_sales.join(df_menu, 'product_id').groupBy('product_id', 'product_name').agg(count('product_id').alias('product_count')).orderBy('product_count', ascending=0)

display(most_df)


# COMMAND ----------

#Frequency of customer visited to resturant
from pyspark.sql.functions import countDistinct

df = (df_sales.filter(df_sales.source_order == 'Restaurant').groupBy('customer_id').agg(countDistinct('order_date')))
df.show()

# COMMAND ----------

#Total sales by country
total_amount_spent_Country = (df_sales.join(df_menu, 'product_id').groupBy('location').agg({'price':'sum'}).orderBy('location'))

display(total_amount_spent_Country)


# COMMAND ----------

#Total sales by order source
total_amount_spent_Source = (df_sales.join(df_menu, 'product_id').groupBy('source_order').agg({'price':'sum'}).orderBy('source_order'))

display(total_amount_spent_Source)


# COMMAND ----------



# Create a view or table

temp_table_name_sales = "`sales_csv-8_txt`"
temp_table_name_menu = "`menu_csv-8_txt`"

df_sales.createOrReplaceTempView(temp_table_name_sales)
df_menu.createOrReplaceTempView(temp_table_name_menu)


#%sql

#/* Query the created temp table in a SQL cell */

#select * from `sales_csv-8_txt`

# With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.

#permanent_table_name = "sales_csv-8_txt"

# df.write.format("parquet").saveAsTable(permanent_table_name)

