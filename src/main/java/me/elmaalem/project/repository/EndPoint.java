package me.elmaalem.project.repository;

import me.elmaalem.project.model.Orders;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class EndPoint {

    public EndPoint(){}

    public static void displayDatasetWithOrders(Dataset<Orders> dataset, int numberRows) throws StreamingQueryException {
        dataset.writeStream()
                .format("console")
                .outputMode("append")
                .option("numRows",numberRows)
                .start()
                .awaitTermination();
    }

    public static void displayDatasetWithRows(Dataset<Row> dataset, int numberRows , String outputMode) throws StreamingQueryException {
        dataset.writeStream()
                .format("console")
                .outputMode(outputMode)
                .option("numRows",numberRows)
                .start()
                .awaitTermination();
    }
}
