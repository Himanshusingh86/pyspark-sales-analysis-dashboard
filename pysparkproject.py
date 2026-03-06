# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

 

schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_date", DateType(), True),  # Keep as String first
    StructField("location", StringType(), True),
    StructField("source_order", StringType(), True)
])


sales_df = spark.read.format("csv") \
    .option("header", "true") \
    .schema(schema) \
    .load("/Volumes/workspace/default/sales/sales.csv.txt")

display(sales_df)

# COMMAND ----------

from pyspark.sql.functions  import month,year ,quarter 

sales_df = sales_df.withColumn("order_year",year(sales_df.order_date ))

display(sales_df)


# COMMAND ----------

# DBTITLE 1,Cell 3

sales_df = sales_df.withColumn("order_month ",month (sales_df.order_date ))


sales_df = sales_df.withColumn("order_quater r",quarter(sales_df.order_date ))


display(sales_df)


# COMMAND ----------

# MAGIC %md 
# MAGIC menu dataframe 

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

 

schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("price",StringType(),True)
])


menu_df = spark.read.format("csv") \
    .option("header", "true") \
    .schema(schema) \
    .load("/Volumes/workspace/default/menu/menu.csv.txt")

display(sales_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC TOTAL AMOUNT SPEND BY THE CUSTOMER 
# MAGIC

# COMMAND ----------

total_amount_spend= (sales_df.join(menu_df,'product_id').groupBy('customer_id').agg({'price':'sum'})
                     .orderBy('customer_id'))
display(total_amount_spend)



# COMMAND ----------

# MAGIC %md 
# MAGIC TOTAL AMOUNT SPEMD BY EACH FOOD CATEGORY 

# COMMAND ----------

total_amount_spend= (sales_df.join(menu_df,'product_id').groupBy('product_name').agg({'price':'sum'})
                     .orderBy('product_name'))
display(total_amount_spend)


# COMMAND ----------

# MAGIC %md 
# MAGIC TOTAL AMOUNT OF SALES IN EACH MONTH 

# COMMAND ----------

df1= (sales_df.join(menu_df,'product_id').groupBy('order_month ').agg({'price':'sum'})
                     .orderBy('order_month ') )
display(df1)


# COMMAND ----------

# MAGIC %md 
# MAGIC yearly sales 

# COMMAND ----------

total_amount_spend= (sales_df.join(menu_df,'product_id').groupBy('order_year').agg({'price':'sum'})
                     .orderBy('order_year'))
display(total_amount_spend)


# COMMAND ----------

# MAGIC %md 
# MAGIC QUEARLY SALES 

# COMMAND ----------

# DBTITLE 1,Fix column typo for quarter grouping
df2 = (
    sales_df
    .join(menu_df, 'product_id')
    .groupBy('order_quater r')
    .agg({'price': 'sum'})
    .orderBy('order_quater r')
)

display(df2)


# COMMAND ----------

# MAGIC %md 
# MAGIC HOW MANY TIMES EACH PRODUCT PURCHASE 
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import count 

most_df = (sales_df.join(menu_df, 'product_id')
           .groupBy('product_id', 'product_name')
           .agg(count('product_id').alias('product_count'))
           .orderBy('product_count', ascending=0))


display(most_df )

# COMMAND ----------

# MAGIC %md
# MAGIC TOP 5 ORDERED ITEMS

# COMMAND ----------

 from pyspark.sql.functions import count 

most_df = (sales_df.join(menu_df, 'product_id')
           .groupBy('product_id', 'product_name')
           .agg(count('product_id').alias('product_count'))
           .orderBy('product_count', ascending=0)
           .drop('product_id').limit(5))


display(most_df )

# COMMAND ----------

# MAGIC %md
# MAGIC TOP ORDERED ITEM 

# COMMAND ----------

  from pyspark.sql.functions import count 

most_df = (sales_df.join(menu_df, 'product_id')
           .groupBy('product_id', 'product_name')
           .agg(count('product_id').alias('product_count'))
           .orderBy('product_count', ascending=0)
           .drop('product_id').limit(1))


display(most_df )

# COMMAND ----------

# MAGIC %md 
# MAGIC FREQUECY OF CUSTOMER VISITED TO RESTAURANT 

# COMMAND ----------

# DBTITLE 1,Cell 23
from pyspark.sql.functions import lower, countDistinct

df = (sales_df
      .filter(lower(sales_df.source_order) == "restaurant")
      .groupBy("customer_id")
      .agg(countDistinct("order_date").alias("order_count"))
     )

display(df)

# COMMAND ----------

# MAGIC %md 
# MAGIC TOTAL SALES BY EACH COUNTRY 

# COMMAND ----------

total_amount_spend= (sales_df.join(menu_df,'product_id').groupBy('location').agg({'price':'sum'}))

display(total_amount_spend)

# COMMAND ----------

# MAGIC %md 
# MAGIC TOTAL SALES ORDER_SOURCE

# COMMAND ----------

total_amount_spend= (sales_df.join(menu_df,'product_id').groupBy('source_order').agg({'price':'sum'}))

display(total_amount_spend)

# COMMAND ----------

