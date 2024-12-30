package org.example;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.functions;

public class ParquetETLJob extends BaseETLJob {

    public ParquetETLJob(SparkSession spark, String s3Path, String esIndex) {
        super(spark, s3Path, esIndex);
    }

    @Override
    public Dataset<Row> loadData() {
        return spark.read().parquet(s3Path);
    }

    @Override
    public Dataset<Row> transformData(Dataset<Row> df) {
        // Example transformation: Add a timestamp column
        return df.withColumn("processed_at", functions.current_timestamp());
    }
}
