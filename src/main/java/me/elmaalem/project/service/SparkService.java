package me.elmaalem.project.service;

import me.elmaalem.project.model.Orders;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQueryException;

import static me.elmaalem.project.repository.EntryPoint.getDataset;
import static me.elmaalem.project.repository.EndPoint.*;

public class SparkService {

    public void listOrders(int numberRows) throws StreamingQueryException {
        displayDatasetWithOrders(getDataset(),numberRows);
    }

    public void listOrdersMatchCustomerName(String customerName) throws StreamingQueryException {

        Dataset<Orders> orders = getDataset().filter((FilterFunction<Orders>) order -> order.getCustomerName().equals(customerName));

        displayDatasetWithOrders(orders,1008);

    }

    public void listOrdersMatchCustomerNameAndOrderDate(String customerName, String orderDate) throws StreamingQueryException {
        Dataset<Row> orders = getDataset()
                .filter((FilterFunction<Orders>) order -> order.getCustomerName().equals(customerName))
                .select("customerName","date","sales","profit","productCategory")
                .where("date == \"" + orderDate + "\"");

        displayDatasetWithRows(orders,1008, "append");
    }

    public void listOrdersMatchCategoryAndProfitPositiveAndSortByCustomerName(String productCategory) throws StreamingQueryException {
        Dataset<Row> orders = getDataset()
                .filter((FilterFunction<Orders>) order -> order.getProductCategory().equals(productCategory))
                .filter("profit > 0.0")
                .groupBy("customerName","date","sales","profit")
                .count()
                .sort("customerName");

        displayDatasetWithRows(orders,1008, "complete");
    }

    public void listOrdersMatchCategoryAndPeriodDateAndSortBySales(String productCategory, String startDate, String endDate) throws StreamingQueryException {
        Dataset<Row> orders = getDataset()
                .filter((FilterFunction<Orders>) order -> order.getProductCategory().equals(productCategory))
                .where("date < \""+ endDate +"\" and date > \""+ startDate +"\"")
                .groupBy("customerName","date","sales","quantity","profit","unitPrice","customerSegment")
                .count()
                .sort("customerName","date");

        displayDatasetWithRows(orders,1008, "complete");
    }
}
