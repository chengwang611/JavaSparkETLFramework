package org.example;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;

public class JsonETLJob extends BaseETLJob {

    public JsonETLJob(SparkSession spark, String s3Path, String esIndex) {
        super(spark, s3Path, esIndex);
    }

    @Override
    public Dataset<Row> loadData() {
        return spark.read()
                .option("multiline", "true")
                .json(s3Path);
    }

    @Override
    public Dataset<Row> transformData(Dataset<Row> df) {
        // Example transformation: Flatten JSON nested structures
        return df.selectExpr("explode(nested_field) as flattened_field");
    }
}

