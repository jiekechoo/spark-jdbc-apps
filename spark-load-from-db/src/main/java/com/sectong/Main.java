package com.sectong;

import java.io.Serializable;
import java.util.List;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8513279306224995844L;

	private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

	private static final String MYSQL_USERNAME = "demo";
	private static final String MYSQL_PWD = "demo";
	private static final String MYSQL_CONNECTION_URL = "jdbc:mysql://192.168.1.91:3306/demo";

	private static final JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("SparkJdbcFromDb").setMaster("local[*]"));

	private static final SQLContext sqlContext = new SQLContext(sc);

	public static void main(String[] args) {

		Properties properties = new Properties();
		properties.put("user", MYSQL_USERNAME);
		properties.put("password", MYSQL_PWD);
		// Load MySQL query result as DataFrame
		DataFrame jdbcDF = sqlContext.read().jdbc(MYSQL_CONNECTION_URL, "users", properties);

		List<Row> employeeFullNameRows = jdbcDF.collectAsList();

		for (Row employeeFullNameRow : employeeFullNameRows) {
			LOGGER.info(employeeFullNameRow.toString());
		}
	}
}
