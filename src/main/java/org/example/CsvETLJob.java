package org.example;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;

public class CsvETLJob extends BaseETLJob {

    public CsvETLJob(SparkSession spark, String s3Path, String esIndex) {
        super(spark, s3Path, esIndex);
    }

    @Override
    public Dataset<Row> loadData() {
        return spark.read()
                .option("header", "true")
                .csv(s3Path);
    }

    @Override
    public Dataset<Row> transformData(Dataset<Row> df) {
        // Example transformation: Convert all column names to lowercase
        for (String colName : df.columns()) {
            df = df.withColumnRenamed(colName, colName.toLowerCase());
        }
        return df;
    }
}
