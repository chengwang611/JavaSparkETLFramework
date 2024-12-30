package org.example;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;

public abstract class BaseETLJob {
    protected SparkSession spark;
    protected String s3Path;
    protected String esIndex;

    public BaseETLJob(SparkSession spark, String s3Path, String esIndex) {
        this.spark = spark;
        this.s3Path = s3Path;
        this.esIndex = esIndex;
    }

    // Abstract method to load data based on the format
    public abstract Dataset<Row> loadData();

    // Abstract method for transforming the data
    public abstract Dataset<Row> transformData(Dataset<Row> df);

    // Method to save data to Elasticsearch
    public void saveToElasticsearch(Dataset<Row> df) {
        df.write()
                .format("org.elasticsearch.spark.sql")
                .option("es.resource", esIndex)
                .save();
    }

    // Main ETL process
    public void execute() {
        Dataset<Row> rawData = loadData();
        Dataset<Row> transformedData = transformData(rawData);
        transformedData.printSchema();
        transformedData.show(false);
        saveToElasticsearch(transformedData);
    }
}

