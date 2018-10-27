package com.myCompany.javaExample.DataFrame;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.bson.Document;

import com.google.gson.Gson;
import com.mongodb.spark.MongoSpark;
import com.myCompany.javaExample.Utils.PaymentType;
import com.myCompany.javaExample.Utils.ProductType;
import com.myCompany.javaExample.Utils.TestSales;

public class SparkToDataframeSaleAnalysis {

	@SuppressWarnings("serial")
	public static void main(String[] args) {

		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("SparkToDataframe");
		
		sparkConf.set("spark.mongodb.output.uri", "mongodb://127.0.0.1/");
		sparkConf.set("spark.mongodb.output.database", "testMongodb");
		sparkConf.set("spark.mongodb.output.collection", "testSales");
		sparkConf.set("spark.mongodb.output.writeConcern.w", "majority");
		
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		SQLContext sqlContext = new org.apache.spark.sql.SQLContext(jsc);

		// Load a text file and convert each line to a JavaBean.
		JavaRDD<String> transactionRDD = jsc.textFile("./input/SalesJan2009.csv");

		// The schema is encoded in a string
		String headerString = transactionRDD.first();
		
		// Generate the schema based on the string of schema
		List<StructField> fields = new ArrayList<StructField>();
		
		
		for (String fieldName : headerString.split(",")) {
			fields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
		}
		StructType schema = DataTypes.createStructType(fields);

		// Convert records of the RDD (transactionRDD) to Rows.
		JavaRDD<String> rowRDDFiltured = transactionRDD.filter(row -> !row.equals(headerString));

		JavaRDD<Row> rowRDD = rowRDDFiltured.map(new Function<String, Row>() {
			public Row call(String record) throws Exception {
				
				String[] fields = record.split(",(?=([^\"]|\"[^\"]*\")*$)");
				
				List<String> columns = new ArrayList<String>();
				for (String field : fields) {
					if (field.trim().contains("\"")) {
						field = field.trim().replaceAll("\"", "").replaceAll(",", "");
					}
					columns.add(field.trim());
				}
				return RowFactory.create(columns.toArray());
			}
		});

		// Apply the schema to the RDD.
		DataFrame transactionDataFrame = sqlContext.createDataFrame(rowRDD, schema);
		
		// Register the DataFrame as a table.
		transactionDataFrame.registerTempTable("transaction");

		// SQL can be run over RDDs that have been registered as tables.
		DataFrame results = sqlContext.sql("SELECT * FROM transaction");

		DataFrame results2 = sqlContext.sql("SELECT Name, Product, Payment_Type, sum(Price) as total_price "
										+ "FROM transaction "
										+ "Group by Name, Product, Payment_Type");
		
		DataFrame results3 = sqlContext.sql("SELECT Payment_Type, sum(Price) as total_price "
				+ "FROM transaction "
				+ "Group by Payment_Type");
		
		DataFrame results4 = sqlContext.sql("SELECT Product, sum(Price) as total_price "
				+ "FROM transaction "
				+ "Group by Product");

		
		List<TestSales> testSalesList = getTestSalesList(results3, results4);
		
		JavaRDD<Document> sparkDocuments = jsc.parallelize(testSalesList).map(new Function<TestSales, Document>() {
		    public Document call(final TestSales testSales) throws Exception {
		    	Gson gson = new Gson();
		        return Document.parse(gson.toJson(testSales));
		    }
		});
		
		MongoSpark.save(sparkDocuments);
	}

	private static List<TestSales> getTestSalesList(DataFrame results3, DataFrame results4) {
		List<PaymentType> paymentTypeList = new ArrayList<>();
		
		results3.collectAsList().forEach(x-> {
			PaymentType paymentType = new PaymentType();
			paymentType.setPayment(x.getString(0));
			paymentType.setTotal_price(Double.toString(x.getDouble(1)));
			paymentTypeList.add(paymentType);
		});
		
		List<ProductType> productTypeList = new ArrayList<>();
		
		results4.collectAsList().forEach(x-> {
			ProductType productType = new ProductType();
			productType.setProduct(x.getString(0));
			productType.setTotal_price(Double.toString(x.getDouble(1)));
			productTypeList.add(productType);
		});
		
		List<TestSales> testSalesList = new ArrayList<>();
		TestSales testSales = new TestSales();
		testSales.setPayment_Type(paymentTypeList);
		testSales.setProduct_Type(productTypeList);
		testSalesList.add(testSales);
		return testSalesList;
	}
}
