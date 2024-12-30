package org.example;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;

public class XmlETLJob extends BaseETLJob {

    public XmlETLJob(SparkSession spark, String s3Path, String esIndex) {
        super(spark, s3Path, esIndex);
    }

    @Override
    public Dataset<Row> loadData() {
        return spark.read()
                .format("com.databricks.spark.xml")
                .option("rowTag", "record")
                .load(s3Path);
    }

    @Override
    public Dataset<Row> transformData(Dataset<Row> df) {
        // Example transformation: Clean up null values
        return df.na().fill("unknown");
    }
}
