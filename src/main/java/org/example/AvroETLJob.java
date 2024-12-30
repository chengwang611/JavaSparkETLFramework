package org.example;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;

public class AvroETLJob extends BaseETLJob {

    public AvroETLJob(SparkSession spark, String s3Path, String esIndex) {
        super(spark, s3Path, esIndex);
    }

    @Override
    public Dataset<Row> loadData() {
        return spark.read().format("avro").load(s3Path);
    }

    @Override
    public Dataset<Row> transformData(Dataset<Row> df) {
        // Example transformation: Filter rows based on a condition
        return df.filter("status = 'active'");
    }
}
