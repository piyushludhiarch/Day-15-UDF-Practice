import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Day15App {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day 15 - UDF Practice")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    import spark.implicits._

    println("===== DAY 15: UDF PRACTICE =====")

    val customers = Seq(
      (1, "Amit", 2500.0),
      (2, "Rahul", 15000.0),
      (3, "Sara", 45000.0),
      (4, "Neha", 75000.0),
      (5, "John", 3000.0)
    ).toDF(
      "customer_id",
      "customer",
      "transaction_value"
    )

    println("\nCUSTOMERS:")
    customers.show(false)

    val salaryBandUDF = udf { salary: Double =>
      if (salary >= 50000) "HIGH"
      else if (salary >= 20000) "MEDIUM"
      else "LOW"
    }

    println("\nUDF SALARY CLASSIFICATION:")

    val classified = customers
      .withColumn(
        "salary_band",
        salaryBandUDF(col("transaction_value") * 2)
      )

    classified.show(false)

    println("\nBUILT-IN SPARK FUNCTION:")

    val builtIn = customers
      .withColumn(
        "value_category",
        when(col("transaction_value") >= 40000, "HIGH")
          .when(col("transaction_value") >= 10000, "MEDIUM")
          .otherwise("LOW")
      )

    builtIn.show(false)

    println("\nREGISTER UDF:")

    spark.udf.register(
      "customerRisk",
      (value: Double) => {
        if (value >= 50000) "HIGH_RISK"
        else if (value >= 20000) "MEDIUM_RISK"
        else "LOW_RISK"
      }
    )

    customers.createOrReplaceTempView("customers")

    spark.sql(
      """
        SELECT
          customer_id,
          customer,
          transaction_value,
          customerRisk(transaction_value) AS risk_category
        FROM customers
      """
    ).show(false)

    println("\nUDF VS BUILT-IN:")
    println("UDF allows custom business logic.")
    println("Built-in Spark functions are generally preferred because Spark can")
    println("optimize them more effectively.")

    spark.stop()
  }
}
