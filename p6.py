from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round

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


prod_purch_18_25_total=prod_purch_18_25_total.withColumnRenamed('sum(cost)','y_total')
prod_purch_total=prod_purch_total.withColumnRenamed('sum(total)','total').withColumnRenamed('category','category1')
differ_df=prod_purch_18_25_total.join(prod_purch_total,prod_purch_18_25_total.category==prod_purch_total.category1)


differ_df=differ_df.withColumn('part',col('y_total')/col('total')).drop(prod_purch_total.category1)
differ_df=differ_df.withColumn('part',round(differ_df.part,2))

differ_df.orderBy(col('part').desc()).select('category','part').limit(3).show()