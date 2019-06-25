package com.csv.sample;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.posexplode;
import static org.apache.spark.sql.functions.split;
import static org.apache.spark.sql.functions.sum;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

public class csvOperation {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("Test").setMaster("local");
		SparkContext sc = new SparkContext(conf);
		SQLContext sqlContext = SQLContext.getOrCreate(sc);
		SparkSession session = SparkSession.builder().appName("Test").config(conf).getOrCreate();

		String file1 = "F:\\deloitte_test\\data.csv";
		String outputLocation = "F:\\deloitte_test\\result";

		UDF1<Object, String> aa = UserDefine.toString;
		session.udf().register("aa", aa, DataTypes.StringType);

		Dataset<Row> dataset = sqlContext.read().option("delimiter", ",").option("header", "true")
				.option("mode", "DROPMALFORMED").csv(file1);
		dataset = dataset.select(col("Country"), posexplode(split(col("Values"), ";")));
		dataset = dataset.groupBy(col("Country"), col("pos")).agg(sum("col").as("value")).sort(col("Country"),
				col("pos"));

		dataset = dataset.groupBy(col("Country")).agg(collect_list(col("value")).as("Values")).sort(col("Country"));
		dataset = dataset.withColumn("Values", callUDF("aa", col("Values")));
		dataset.coalesce(1).write().option("delimiter", ",").option("header", "true").parquet(outputLocation);
//		dataset.printSchema();
	}

}

class UserDefine {
	public static UDF1<Object, String> toString = new UDF1<Object, String>() {

		/**
		 * 
		 */
		private static final long serialVersionUID = -1978851533544217004L;

		public String call(Object t1) throws Exception {

			String str = t1.toString();
			str = str.substring(str.indexOf("(")).replaceAll("[()]", "").replace(",", ";");
			return str;
		}
	};

}
