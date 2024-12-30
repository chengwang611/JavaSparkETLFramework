package org.example;

import org.apache.spark.sql.SparkSession;

public class ETLDriver {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("SparkBatchETLFramework")

    //            .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.es.nodes", "192.168.1.14")
                .config("spark.es.port", "9200")
                .config("spark.es.nodes.wan.only", "true")
                .config("es.index.auto.create", "true")
                .master("local[*]")
                .getOrCreate();

        String s3Path ="s3a://etlsources/parquet/202410/yellow_tripdata_2024-10.parquet";// args[0];
        String esIndex = "yellowtrip";
        String fileFormat ="parquet";// args[2].toLowerCase();

        BaseETLJob etlJob = null;

        switch (fileFormat) {
            case "csv":
                etlJob = new CsvETLJob(spark, s3Path, esIndex);
                break;
            case "json":
                etlJob = new JsonETLJob(spark, s3Path, esIndex);
                break;
            case "xml":
                etlJob = new XmlETLJob(spark, s3Path, esIndex);
                break;
            case "parquet":
                etlJob = new ParquetETLJob(spark, s3Path, esIndex);
                break;
            case "avro":
                etlJob = new AvroETLJob(spark, s3Path, esIndex);
                break;
            default:
                throw new IllegalArgumentException("Unsupported file format: " + fileFormat);
        }

        // Run the ETL job
        etlJob.execute();

        spark.stop();
    }
}
