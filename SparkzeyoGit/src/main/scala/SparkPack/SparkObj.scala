package SparkPack

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import java.io.File
import org.apache.spark.sql.functions._


object SparkObj {


	def main(args:Array[String]):Unit={

			val conf=new SparkConf().setAppName("spark_integration").setMaster("local[*]")
					val sc=new SparkContext(conf)
					sc.setLogLevel("Error")
					val spark=SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
					import spark.implicits._


					val urldata = "https://randomuser.me/api/0.8/?results=10"	
					val result = scala.io.Source.fromURL(urldata).mkString
					val rdd = sc.parallelize(List(result))
					rdd.foreach(println)

					println("---------------- Raw Json dataframe -------------------------")
					val df = spark.read.json(rdd)
					df.show()
					df.printSchema()


					println("--------------- Json to flatten dataframe -------------------")
					val flattendf = df.withColumn("results", explode(col("results")))
					flattendf.show()
					flattendf.printSchema()

					println("============ Final flattened dataframe =====================")

					val finaldf = flattendf.select(
							col("nationality"),
							col("results.user.cell").alias("cell"),
							col("results.user.dob").alias("dob"),
							col("results.user.email").alias("email"),
							col("results.user.gender").alias("gender"),
							col("results.user.location.city").alias("city"),
							col("results.user.location.state").alias("state"),
							col("results.user.location.street").alias("street"),
							col("results.user.location.zip").alias("zip"),
							col("results.user.md5").alias("md5"),
							col("results.user.name.first").alias("first"),
							col("results.user.name.last").alias("last"),
							col("results.user.name.title").alias("title"),
							col("results.user.password").alias("password"),
							col("results.user.phone").alias("phone"),
							col("results.user.picture.large").alias("large"),
							col("results.user.picture.medium").alias("medium"),
							col("results.user.picture.thumbnail").alias("thumbnail"),
							col("results.user.registered").alias("registered"),
							col("results.user.salt").alias("salt"),
							col("results.user.sha1").alias("sha1"),
							col("results.user.sha256").alias("sha256"),
							col("results.user.username").alias("username"),
							col("seed"),
							col("version")
							)

					finaldf.show()
					finaldf.printSchema()

					println("============ Count of final flattened dataframes =====================")
					println(finaldf.count())

					println("==================== Final Reverted dataframe =================================")
					val complexdf1 =  finaldf.select (

							col("nationality"),
							struct(
									col("cell"),
									col("dob"),
									col("email"),
									col("gender"),
									struct(
											col("city"),
											col("state"),
											col("street"),
											col("zip")
											).alias("location"),
									col("md5"),
									struct(
											col("first"),
											col("last"),
											col("title")
											).alias("name"),
									col("password"),
									col("phone"),
									struct(
											col("large"),
											col("medium"),
											col("thumbnail")
											).alias("picture"),
									col("registered"),
									col("salt"),
									col("sha1"),
									col("sha256"),
									col("username")
									).alias("user"),
							col("seed"),
							col("version")

							)

					val complexdf = complexdf1.groupBy(col("nationality"),col("seed"),col("version"))
					.agg(collect_list(struct(col("user"))).alias("results"))

					complexdf.show()
					complexdf.printSchema()

	}
}