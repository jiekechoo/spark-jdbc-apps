package com.sectong;

import java.io.Serializable;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

public class Main implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8513279306224995844L;
	private static final String MYSQL_USERNAME = "demo";
	private static final String MYSQL_PWD = "demo";
	private static final String MYSQL_CONNECTION_URL = "jdbc:mysql://192.168.1.91:3306/demo";

	private static final JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("SparkSaveToDb").setMaster("local[*]"));

	private static final SQLContext sqlContext = new SQLContext(sc);

	public static void main(String[] args) {
		// Sample data-frame loaded from a JSON file
		DataFrame usersDf = sqlContext.read().json("users.json");

		// Save data-frame to MySQL (or any other JDBC supported databases)
		Properties connectionProperties = new Properties();
		connectionProperties.put("user", MYSQL_USERNAME);
		connectionProperties.put("password", MYSQL_PWD);

		// write dataframe to jdbc mysql
		usersDf.write().mode(SaveMode.Append).jdbc(MYSQL_CONNECTION_URL, "users", connectionProperties);
	}
}
