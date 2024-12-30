package org.example;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.*;

public class CsvETLJobTest {
//
//    private SparkSession spark;
//    private CsvETLJob csvETLJob;
//
//    //@BeforeEach
//    public void setUp() {
//        spark = mock(SparkSession.class);
//        csvETLJob = new CsvETLJob(spark, "s3://path/to/csv", "test_index");
//    }
//
//    //@Test
//    public void testLoadData() {
//        // Mock the dataframe
////        Dataset<Row> mockDf = mock(Dataset<Row>.class);
////        when(spark.read()).thenReturn(mockCsvReader);
////        when(mockCsvReader.csv("s3://path/to/csv")).thenReturn(mockDf);
//
//        // Load data
//        Dataset<Row> df = csvETLJob.loadData();
//
//        // Assert that the dataframe is not null
//        assertNotNull(df);
//    }
}
