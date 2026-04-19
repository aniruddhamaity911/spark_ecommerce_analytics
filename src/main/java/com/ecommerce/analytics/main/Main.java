package com.ecommerce.analytics.main;

import com.ecommerce.analytics.transform.ProcessData;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;


import static org.apache.spark.sql.functions.*;


public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "D:\\Java\\Spark\\hadoop");
//        String customerDataPath = "src/main/resources/customers.csv";
//        String orderItemPath = "src/main/resources/order_items.csv";
//        String orderPath = "src/main/resources/orders.csv";
//        String schemaPath = "src/main/resources/schema.json";
//        String outputfilePath = "src/main/resources/output";

        Map<String, String> params = new HashMap<>();
        for (int i = 0; i < args.length; i += 1) {
            String[] argument = args[i].split("=");
            params.put(argument[0], argument[1]);
        }

        String customerDataPath = params.get("--customers");
        String orderItemPath    = params.get("--orderItems");
        String orderPath        = params.get("--orders");
        String schemaPath       = params.get("--schema");
        String outputfilePath   = params.get("--output");

        try(SparkSession spark = SparkSession.builder().appName("ECommerceAnalytics").master("local[*]")
                .config("spark.driver.bindAddress","127.0.0.1").config("spark.driver.host","127.0.0.1").config("spark.sql.warehouse.dir","file:///c:/tmp").getOrCreate()){

            /*
            Reading the data files
             */
            log.info("reading file {}",customerDataPath);
            Dataset<Row> customerData = readWithHeader(spark,customerDataPath);
            log.info("reading file {}",orderItemPath);
            Dataset<Row> orderItemData = readWithHeader(spark,orderItemPath);
            log.info("reading file {}",orderPath);
            Dataset<Row> orderData = readWithHeader(spark,orderPath);
            log.info("reading schema file {}",schemaPath);
            StructType schema = (StructType) DataType.fromJson(readSchema(schemaPath));

            /*
            Removing duplicates/null/empty values
             */

            log.info("Removing duplicates/null/empty values");
            customerData = ProcessData.removeDuplicate(customerData,new String[]{"customer_id"});
            customerData = ProcessData.selectColumns(customerData,new String[]{"customer_id"});
            orderItemData = ProcessData.removeDuplicate(orderItemData,new String[]{"order_id","product_id"});
            orderItemData = ProcessData.removeNullOrEmpty(orderItemData,new String[]{"quantity","item_price"});

            /*
            filtering order data which are delivered but not returned.
             */
            orderData = orderData.filter(lower(col("status")).equalTo("delivered"))
                    .filter(lower(col("status")).notEqual("returned"));

            log.info("joining customer data with order data");
            Dataset<Row> customerOrderData = customerData.join(orderData,customerData.col("customer_id").equalTo(orderData.col("customer_id")))
                    .drop(orderData.col("customer_id"));

            log.info("joining customer order data with product data");
            Dataset<Row> customerOrder_orderItem = customerOrderData.join(orderItemData,customerOrderData.col("order_id").equalTo(orderItemData.col("order_id")))
                    .drop(orderItemData.col("order_id"));

            log.info("converting and selected the column from schema");
            Dataset<Row> outputDataSet = ProcessData.dataTypeConversion(customerOrder_orderItem,schema);

            log.info("writing as parquet file in path {}",outputfilePath);
            outputDataSet.write().mode("overwrite").parquet(outputfilePath);


        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    static Dataset<Row>  readWithHeader(SparkSession sparkSession,String path){
        return sparkSession.read().option("header",true).csv(path);
    }
    static String readSchema(String filePath){
        try{
            return Files.readString(Paths.get(filePath));
        } catch (IOException e) {
            System.out.println("Error reading schema"+e);
            throw new RuntimeException(e);
        }
    }
}
