import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._

// ------------------------------------------------------------
// 0. Project App
// ------------------------------------------------------------
object App {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Codespaces Spark Hello")
      .master("local[*]")
      .config("spark.ui.showConsoleProgress", "false")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val df = Seq(
      ("Alice", 34),
      ("Bob", 28),
      ("Carol", 41)
    ).toDF("name", "age")

    df.show()

    // Keep job alive briefly so you can view the Spark UI on port 4040
    println("Open the Spark UI on port 4040 (Ports panel). Sleeping 10s...")
    Thread.sleep(10000)

    spark.stop()
  }
}

// ------------------------------------------------------------
// 0. Project Setup & Warm-Up
// ------------------------------------------------------------
object WarmUpAppExercises {
  def main(args: Array[String]): Unit = {
    //   1. Create a SparkSession named "WarmUpApp" (local[*]).
    val spark = SparkSession.builder()
      .appName("WarmUpApp")
      .master("local[*]")
      .config("spark.ui.showConsoleProgress", "false")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    //   2. Read data/warmup.csv with header and inferred schema as a DataFrame.
    val csvPath = "src/main/resources/warmup.csv"
    val dfwarmup = spark.read
      .option("header", "true")        // Usa la primera fila como nombres de columnas
      .option("inferSchema", "true")   // Detecta automáticamente tipos de datos
      .csv(csvPath)                    // Ruta al archivo CSV

    //   3. Print schema and show first 5 rows.
    dfwarmup.printSchema()
    dfwarmup.show(5)

    //   4. Filter rows where age >= 18 and show them.
    val dfadults = dfwarmup.filter($"age" >= 18)
    dfadults.show()

    spark.stop()
  }
}

// ------------------------------------------------------------
// 1. DataFrame Fundamentals – Online Retail Analysis
// ------------------------------------------------------------
object RetailAnalysisAppExercises {
  def main(args: Array[String]): Unit = {
    // 1. Create SparkSession.
    val spark = SparkSession.builder()
      .appName("RetailAnalysisApp")
      .master("local[*]")
      .config("spark.ui.showConsoleProgress", "false")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._   //Permite usar ($"customer_id"), sino col("customer_id")
    import org.apache.spark.sql.types._

    // 2. Define StructType retailSchema.
    val schema = StructType(Array(      // Definición explícita del esquema
      StructField("order_id", LongType, nullable = false),
      StructField("customer_id", LongType, nullable = false),
      StructField("country", StringType, nullable = false),
      StructField("product", StringType, nullable = false),
      StructField("category", StringType, nullable = false),
      StructField("unit_price", DoubleType, nullable = false),
      StructField("quantity", IntegerType, nullable = false),
      StructField("order_timestamp", TimestampType, nullable = false)
    ))

    // 3. Read CSV with that schema.
    val csvPath = "src/main/resources/online_retail.csv"
    val dfretail = spark.read
      .option("header", "true")        // Usa la primera fila como nombres de columnas
      .schema(schema)                  // Aquí aplicamos el esquema explícito
      .csv(csvPath)                    // Ruta al archivo CSV

    // 4. Clean data (filter conditions).
    // 5. Add total_amount column.
    val dffiltered = dfretail.filter($"customer_id".isNotNull)
      .filter($"quantity" > 0 && $"unit_price" > 0)
      .withColumn("total_amount", $"quantity" * $"unit_price")
    dffiltered.show()

    // 6. Implement each query (a–d) and show results.
    //        a) Top 10 countries by total revenue (sum total_amount).
    dffiltered.createOrReplaceTempView("retail")
    var result = spark.sql("""SELECT country, SUM(total_amount) AS amount 
                              FROM retail GROUP BY country ORDER BY amount DESC LIMIT 10""")
    result.show()
    //        b) For each country, average order value.
    result = spark.sql("""
                  WITH order_totals AS (
                    SELECT order_id, country, SUM(total_amount) AS order_total 
                    FROM retail 
                    GROUP BY order_id, country
                  )
                  SELECT country, AVG(order_total) AS avg_order_value
                  FROM order_totals
                  GROUP BY country
                  """)
    result.show()
    //        c) Top 5 products by total quantity sold globally.
    result = spark.sql("""
                  SELECT product, SUM(quantity) AS total_quantity
                  FROM retail
                  GROUP BY product
                  ORDER BY total_quantity DESC LIMIT 5
                  """)
    result.show()
    //        d) Per category.
    result = spark.sql("""
                  SELECT category, 
                      SUM(quantity) AS total_quantity,
                      SUM(total_amount) AS revenue,
                      AVG(unit_price) AS avg_price
                  FROM retail
                  GROUP BY category
                  """)
    result.show()

    spark.stop()
  }
}


