import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import java.net.URL;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class BigDataAssignment {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder()
                .appName("SaleDataAnalysis")
                .master("local[*]")
                .getOrCreate();

        URL resource = BigDataAssignment.class.getClassLoader().getResource("SaleData.xlsx");
        Dataset<Row> salesData = spark.read()
                .format("com.crealytics.spark.excel")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(resource.getPath());

        // show all data
        salesData.show();

        // call every method for solutions
        calculateTotalSalesByRegion(salesData);
        filterCentralRegionSales(salesData);
        findTopProductsByUnits(salesData);
        calculateRevenueBreakdownByProductAndRegion(salesData);

//        combineWith2020Data(spark, salesData, "SaleData_2020.xlsx");

        // stop SparkSession and release resources
        spark.stop();
    }

    // Question 1：count the all sale amount for every region
    public static void calculateTotalSalesByRegion(Dataset<Row> salesData) {
        Dataset<Row> totalSalesByRegion = salesData.groupBy("Region")
                .sum("Sale_amt")
                .withColumnRenamed("sum(Sale_amt)", "TotalSale_amt");
        totalSalesByRegion.show();
    }

    // Question 2：filter the sale record of region “Central”
    public static void filterCentralRegionSales(Dataset<Row> salesData) {
        Dataset<Row> centralSales = salesData.filter(salesData.col("Region").equalTo("Central"));
        centralSales.show();
    }

    // Question 3：find the top products by units
    public static void findTopProductsByUnits(Dataset<Row> salesData) {
        Dataset<Row> topProductsByUnits = salesData.groupBy("Item")
                .sum("Units")
                .withColumnRenamed("sum(Units)", "TotalUnitsSold")
                .orderBy(functions.desc("TotalUnitsSold"));
        topProductsByUnits.show();
    }

    // Question 4：calculate revenue breakdown by product and region
    public static void calculateRevenueBreakdownByProductAndRegion(Dataset<Row> salesData) {
        Dataset<Row> revenueBreakdown = salesData.groupBy("Region", "Item")
                .sum("Sale_amt")
                .withColumnRenamed("sum(Sale_amt)", "Revenue");
        revenueBreakdown.show();
    }

    // Question 5：combine with data of 2020
    public static void combineWith2020Data(SparkSession spark, Dataset<Row> salesData, String newDataPath) {
        Dataset<Row> salesData2020 = spark.read()
                .format("com.crealytics.spark.excel")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(newDataPath);

        Dataset<Row> combinedData = salesData.union(salesData2020);
        combinedData.show();
    }
}