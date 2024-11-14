from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("DZ3").getOrCreate()

usersdf = spark.read.options(delimiter=",", sep = ",", header=True, inferSchema=True,multiline=True, quote="\"", escape="\"").format("csv").load("/mnt/e/tmp/DZ1/users.csv")
purchasesdf = spark.read.options(delimiter=",", sep = ",", header=True, inferSchema=True,multiline=True, quote="\"", escape="\"").format("csv").load("/mnt/e/tmp/DZ1/purchases.csv")
productsdf = spark.read.options(delimiter=",", sep = ",", header=True, inferSchema=True,multiline=True, quote="\"", escape="\"").format("csv").load("/mnt/e/tmp/DZ1/products.csv")

usersdf_clean=usersdf.dropna()
purchasesdf_clean=purchasesdf.dropna()
productsdf_clean=productsdf.dropna()

prod_purch=purchasesdf_clean.join(productsdf_clean, productsdf_clean.product_id == purchasesdf_clean.product_id)
prod_purch_total=prod_purch.select(['category','price','quantity']).withColumn('total',col('price')*col('quantity')).groupby('category').sum('total')

users_prod_purch=usersdf_clean.join(prod_purch,usersdf_clean.user_id == prod_purch.user_id)
prod_purch_18_25=users_prod_purch.withColumn("cost",col("price")*col("quantity")).filter((col('Age')>18) & (col('Age')<25))

prod_purch_18_25_total=prod_purch_18_25.select(['category','cost']).groupby('category').sum('cost')

prod_purch_18_25_total.show()
