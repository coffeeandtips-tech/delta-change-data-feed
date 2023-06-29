package com.coffeeandtips.delta.changedatafeed;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DeltaChangeDataFeedTutorial {

    public static SparkSession getSparkSession(){

        String val_ext="io.delta.sql.DeltaSparkSessionExtension";
        String val_ctl="org.apache.spark.sql.delta.catalog.DeltaCatalog";

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("spark-delta");
        sparkConf.setMaster("local[1]");
        sparkConf.set("spark.sql.extensions", val_ext);
        sparkConf.set("spark.sql.catalog.spark_catalog",val_ctl);

        return SparkSession.builder()
                .config(sparkConf)
                .enableHiveSupport()
                .getOrCreate();
    }

    public static void createDataSet(SparkSession sparkSession){
        Dataset<Row> raw = sparkSession.read()
                .json("productDataSet.json");
        raw.createOrReplaceGlobalTempView("raw_product");
    }

    public static void createDeltaTable(SparkSession sparkSession){
        String creatingTableDDL = "CREATE OR REPLACE TABLE product " +
                "(id INT, nome STRING, preco DOUBLE, qtde INT, purchase_date STRING) " +
                "USING DELTA PARTITIONED BY (purchase_date) " +
                "TBLPROPERTIES (delta.enableChangeDataFeed = true)";
        sparkSession.sql(creatingTableDDL);
    }

    public static void doMerge(SparkSession sparkSession){
        String mergeDML = "MERGE INTO product USING global_temp.raw_product as raw_product " +
                "ON raw_product.id = product.id " +
                "WHEN MATCHED THEN " +
                "UPDATE SET product.nome = raw_product.nome," +
                "product.preco = raw_product.preco, " +
                "product.qtde = raw_product.qtde, " +
                "product.purchase_date = raw_product.purchase_date " +
                "WHEN NOT MATCHED THEN INSERT * ";
        sparkSession.sql(mergeDML);
    }

    public static void doUpdate(SparkSession sparkSession, int id){
        String updateDDL = "UPDATE product set nome = 'Ruffles' WHERE id = " + id;
        sparkSession.sql(updateDDL);
    }

    public static void doDelete(SparkSession sparkSession, int id){
        String deleteDDL = "DELETE from product WHERE id = " + id;
        sparkSession.sql(deleteDDL);
    }

    public static void readDeltaTable(SparkSession sparkSession){
        sparkSession.read()
                .option("startingVersion", 0)
                .option("readChangeFeed", "true")
                .format("delta")
                .table("product")
                .orderBy("id")
                .show();
    }


    public static void main(String[] args) {

        SparkSession sparkSession = getSparkSession();

        /*Creating and loading Dataset*/
        createDataSet(sparkSession);

        /*Creating Delta Table*/
        createDeltaTable(sparkSession);

        /*Merging*/
        doMerge(sparkSession);

        /*Reading table*/
        readDeltaTable(sparkSession);

        /*Deleting a record*/
        doDelete(sparkSession, 6);

        /*Reading table (again)*/
        readDeltaTable(sparkSession);

        /*Updating a record*/
        doUpdate(sparkSession, 2);

        /*Reading table (one more time)*/
        readDeltaTable(sparkSession);
    }
}
