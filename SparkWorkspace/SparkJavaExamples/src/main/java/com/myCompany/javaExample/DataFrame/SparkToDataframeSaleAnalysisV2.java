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
import com.myCompany.javaExample.Utils.CustomerPayment;
import com.myCompany.javaExample.Utils.PaymentType;
import com.myCompany.javaExample.Utils.ProductType;
import com.myCompany.javaExample.Utils.TestSales;

public class SparkToDataframeSaleAnalysisV2 {

	@SuppressWarnings("serial")
	public static void main(String[] args) {

		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("SparkToDataframe");
		
		sparkConf.set("spark.mongodb.output.uri", "mongodb://127.0.0.1/");
		sparkConf.set("spark.mongodb.output.database", "testMongodb");
		sparkConf.set("spark.mongodb.output.collection", "customerPayment");
		sparkConf.set("spark.mongodb.output.writeConcern.w", "majority");
		
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		SQLContext sqlContext = new org.apache.spark.sql.SQLContext(jsc);

		// Load a text file and convert each line to a JavaBean.
		JavaRDD<String> transactionRDD = jsc.textFile("./input/SalesJan2009.csv");

		JavaRDD<String> accountRDD = jsc.textFile("./input/SalesJan2009Account.csv");
		
		DataFrame transactionDataFrame = getDataFrame(sqlContext, transactionRDD);
		
		DataFrame accountDataFrame = getDataFrame(sqlContext, accountRDD);
		
		// Register the DataFrame as a table.
		transactionDataFrame.registerTempTable("transaction");
		
		accountDataFrame.registerTempTable("account");

		// SQL can be run over RDDs that have been registered as tables.
		/*DataFrame results = sqlContext.sql("SELECT `Account Number` FROM account "
											+ "Group by `Account Number` "  
											+ "Having COUNT(*) > 1 ");*/

		DataFrame results = sqlContext.sql("SELECT * FROM account");
		
		DataFrame results2 = sqlContext.sql("SELECT Name, Product, Payment_Type, sum(Price) as total_price "
										+ "FROM transaction "
										+ "Group by Name, Product, Payment_Type");
		
		/*DataFrame joinResults = transactionDataFrame.join(accountDataFrame, transactionDataFrame.col("Name").equalTo(
				accountDataFrame.col("Name"))).drop(accountDataFrame.col("Name"));*/
		
		DataFrame joinResults = (transactionDataFrame.as("A")).join(accountDataFrame.withColumnRenamed("Name", "NameN").as("B"))
								.where("A.Name = B.NameN").drop("NameN");
		
		
		DataFrame joinResultsNew = joinResults.withColumn("Price", joinResults.col("Price").cast(DataTypes.IntegerType));
		
		DataFrame joinResultsNew2 =joinResultsNew.groupBy("Name", "Account Number").sum("Price");
		
		//joinResultsNew2.withColumn("Total", joinResultsNew2.col("sum(Price)")).show();
		
		DataFrame customerPaymentSummaryDF = joinResultsNew2.withColumn("Total", joinResultsNew2.col("sum(Price)")).drop("sum(Price)");

		List<CustomerPayment> customerPaymentList = new ArrayList<>();

		customerPaymentSummaryDF.collectAsList().forEach(x-> {
			CustomerPayment customerPayment = new CustomerPayment();
			customerPayment.setCustomerName(x.getString(0));
			customerPayment.setAccountNumber(x.getString(1));
			customerPayment.setTotal(Long.toString(x.getLong(2)));
			customerPaymentList.add(customerPayment);
		});
		JavaRDD<Document> sparkDocuments = jsc.parallelize(customerPaymentList).map(new Function<CustomerPayment, Document>() {
		    public Document call(final CustomerPayment customerPayment) throws Exception {
		    	Gson gson = new Gson();
		        return Document.parse(gson.toJson(customerPayment));
		    }
		});
		
		MongoSpark.save(sparkDocuments);
	}

	private static DataFrame getDataFrame(SQLContext sqlContext, JavaRDD<String> inputRDD) {
		// The schema is encoded in a string
		String headerString = inputRDD.first();
		
		// Generate the schema based on the string of schema
		List<StructField> fields = new ArrayList<StructField>();
		
		
		for (String fieldName : headerString.split(",")) {
			fields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
		}
		StructType schema = DataTypes.createStructType(fields);

		// Convert records of the RDD (transactionRDD) to Rows.
		JavaRDD<String> rowRDDFiltured = inputRDD.filter(row -> !row.equals(headerString));

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
		return sqlContext.createDataFrame(rowRDD, schema);
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
