import org.apache.spark.sql.{SparkSession, DataFrame, Dataset}
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

// ------------------------------------------------------------
// 2. Joins & Window Functions – Customers and Orders
// ------------------------------------------------------------
object JoinsAndWindowsExercises {

  def main(args: Array[String]): Unit = {
    // 1. Create SparkSession.
    val spark = SparkSession.builder()
      .appName("JoinsAndWindowsExercises")
      .master("local[*]")
      .config("spark.ui.showConsoleProgress", "false")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._   //Permite usar ($"customer_id"), sino col("customer_id")

    // 2. Read customers.csv and orders.csv.
    val customersPath = "src/main/resources/customers.csv"
    val dfcusto = spark.read
      .option("header", "true")        // Usa la primera fila como nombres de columnas
      .option("inferSchema", "true")   // Detecta automáticamente tipos de datos
      .csv(customersPath)              // Ruta al archivo CSV
    
    val ordersPath = "src/main/resources/orders.csv"
    val dforder = spark.read
      .option("header", "true")        // Usa la primera fila como nombres de columnas
      .option("inferSchema", "true")   // Detecta automáticamente tipos de datos
      .csv(ordersPath)                 // Ruta al archivo CSV

    // 3. Inner join by customer_id.
    val joinedDf = dforder.join(dfcusto, Seq("customer_id"), "inner")
    joinedDf.printSchema()

    // 4. Compute total spent per customer.
    joinedDf.createOrReplaceTempView("order")
    val totalspentcustomer = spark.sql("""
                  SELECT customer_id, SUM(order_total) AS total
                  FROM order
                  GROUP BY customer_id ORDER BY total DESC
                  """)
    totalspentcustomer.show()

    // 5. Add customer_total_spent to each order row.
    // Paso 1: calcular gasto total por cliente
    val customerTotals = joinedDf.groupBy("customer_id").agg(sum("order_total").alias("customer_total_spent"))
    // Paso 2: unir con la tabla de pedidos
    val ordersWithTotal = joinedDf.join(customerTotals, Seq("customer_id"), "left")

    // 6. Rank customers per country and get top 3.
    // Definir la ventana: particionamos por cliente y ordenamos por amount descendente
    val especificaciondDeVentana = Window.partitionBy("country").orderBy(desc("customer_total_spent"))
    // Añadir columna con dense_rank
    val rankedCustomers = customerTotals.join(dfcusto, Seq("customer_id"))
      .withColumn("rank_in_country", row_number().over(especificaciondDeVentana))
    rankedCustomers.show()
    // Top 3 customers per country
    val top3PerCountry = rankedCustomers.filter(col("rank_in_country") <= 3)
    top3PerCountry.show(false)

    // 7. Compute order_seq and running_total per customer.
    //   7. For each customer, compute:
    //        - order sequence number (1st, 2nd, 3rd...) by order_timestamp.
    //        - running total of order_total over that ordering.
    val especificaciondDeVentana2 = Window.partitionBy("customer_id").orderBy(col("order_timestamp").cast("timestamp"))
    val ordersWithSeqAndRunningTotal = dforder
      .withColumn("order_seq", row_number().over(especificaciondDeVentana2))
      .withColumn("running_total", sum("order_total").over(especificaciondDeVentana2))
    ordersWithSeqAndRunningTotal.show()

    spark.stop()
  }

}

// ------------------------------------------------------------
// 3. Datasets & Typed Transformations
// ------------------------------------------------------------
object TypedDatasetsExercises {

  case class Customer(
    customer_id: Long,
    name: String,
    email: String,
    country: String,
    signup_date: String
  )

  case class Order(
    order_id: Long,
    customer_id: Long,
    order_timestamp: String,
    order_total: Double
  )

  case class CustomerStats(
    customer_id: Long,
    name: String,
    country: String,
    total_orders: Long,
    total_spent: Double,
    first_order_ts: Option[String],
    last_order_ts: Option[String]
  )

  def main(args: Array[String]): Unit = {
    // 1. Create SparkSession and import spark.implicits._.
    val spark = SparkSession.builder()
      .appName("TypedDatasetsExercises")
      .master("local[*]")
      .config("spark.ui.showConsoleProgress", "false")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._   //Permite usar ($"customer_id"), sino col("customer_id")

    // 2. Load customers.csv and orders.csv; convert to Dataset[Customer] and Dataset[Order].
    val customersPath = "src/main/resources/customers.csv"
    val customersDf = spark.read
      .option("header", "true")        // Usa la primera fila como nombres de columnas
      .option("inferSchema", "true")   // Detecta automáticamente tipos de datos
      .csv(customersPath)              // Ruta al archivo CSV
    
    val ordersPath = "src/main/resources/orders.csv"
    val ordersDf = spark.read
      .option("header", "true")        // Usa la primera fila como nombres de columnas
      .option("inferSchema", "true")   // Detecta automáticamente tipos de datos
      .csv(ordersPath)                 // Ruta al archivo CSV

    val customersDs: Dataset[Customer] = customersDf.as[Customer]
    val ordersDs: Dataset[Order] = ordersDf.as[Order]

    // 3. Use typed operations (e.g., groupByKey + mapGroups) to compute CustomerStats.
    // Group orders by customer_id using a Dataset
    val ordersByCustomer = ordersDs.groupByKey(_.customer_id)

    // Join customers with grouped orders and compute stats
    // joinWith Devuelve un Dataset[(T, U)], es decir, un Dataset de tuplas con los objetos originales de cada Dataset.
    val ordersAggDs = ordersByCustomer.mapGroups { (cid, ordersIter) =>
      val orders = ordersIter.toList.sortBy(_.order_timestamp)
      val totalOrders = orders.size.toLong
      val totalSpent = orders.map(_.order_total).sum
      val firstTs = orders.headOption.map(_.order_timestamp)
      val lastTs = orders.lastOption.map(_.order_timestamp)

      CustomerStats(
        customer_id = cid,
        name = "",            // se rellenará después con el join
        country = "",
        total_orders = totalOrders,
        total_spent = totalSpent,
        first_order_ts = firstTs,
        last_order_ts = lastTs
      )
    }

    // Join por columna:
    val statsDs = customersDs.joinWith(
      ordersAggDs,
      customersDs("customer_id") === ordersAggDs("customer_id"),
      "left_outer"
    ).map {
      case (customer, statsOpt) =>
        if (statsOpt == null) {
          // Cliente sin pedidos → valores por defecto
          CustomerStats(
            customer_id = customer.customer_id,
            name = customer.name,
            country = customer.country,
            total_orders = 0L,
            total_spent = 0.0,
            first_order_ts = None,
            last_order_ts = None
          )
        } else {
          // Cliente con pedidos → rellenamos con datos del join
          statsOpt.copy(
            name = customer.name,
            country = customer.country
          )
        }
    }

    println("CustomerStats:")
    statsDs.show(false)

    // 4. Filter total_spent > 1000 and map to String summaries.
    val bigSpenders = statsDs.filter(_.total_spent > 1000.0)

    val summaries = bigSpenders.map { cs =>
      f"${cs.name} (${cs.country}) spent ${cs.total_spent}%.2f in ${cs.total_orders} orders"
    }

    println("Big spender summaries:")
    summaries.show(false)

    spark.stop()
  }

}

// ------------------------------------------------------------
// 4. Spark SQL & UDFs
// ------------------------------------------------------------
object SqlAndUdfExercises {

  def spamRisk(email: String): Int = {
    if (email == null) 2
    else {
      val parts = email.split("@")
      val domain = if (parts.length == 2) parts(1) else ""
      domain match {
        case "gmail.com"  => 0
        case "outlook.com" => 0
        case "yahoo.com"  => 1
        case _            => 2
      }
    }
  }

  def main(args: Array[String]): Unit = {
    // 1. Create SparkSession.
    val spark = SparkSession.builder()
      .appName("SqlAndUdfSolution")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._   //Permite usar ($"customer_id"), sino col("customer_id")
	
    val retailSchema = StructType(Seq(
      StructField("order_id", LongType, nullable = false),
      StructField("customer_id", LongType, nullable = true),
      StructField("country", StringType, nullable = true),
      StructField("product", StringType, nullable = true),
      StructField("category", StringType, nullable = true),
      StructField("unit_price", DoubleType, nullable = true),
      StructField("quantity", IntegerType, nullable = true),
      StructField("order_timestamp", StringType, nullable = true)
    ))

    // 2. Load + clean retail CSV, add total_amount.
    val csvPath = "src/main/resources/online_retail.csv"
    val dfretail = spark.read
      .option("header", "true")        
      .option("inferSchema", "true")   // Detecta automáticamente tipos de datos            
      .csv(csvPath)                    
    val dffiltered = dfretail.filter($"customer_id".isNotNull)
      .filter($"quantity" > 0 && $"unit_price" > 0)
      .withColumn("total_amount", $"quantity" * $"unit_price")
    dffiltered.show()

    // 3. createOrReplaceTempView("retail").
    dffiltered.createOrReplaceTempView("retail")

    // 4. Implement SQL queries for metrics and top categories.
    //    SQL query 1: per country,
    //        - num_orders (count distinct order_id)
    //        - num_customers (count distinct customer_id)
    //        - total_revenue (sum quantity * unit_price)
    val query1 = spark.sql("""
                SELECT country,
                  COUNT(DISTINCT order_id) AS num_orders,
                  COUNT(DISTINCT customer_id) AS num_customers,
                  SUM(total_amount) AS total_revenue
                FROM retail
                GROUP BY country
                ORDER BY total_revenue DESC
                """)
    query1.show()
    //   SQL query 2: top 5 categories per country by total_revenue,
    //      using ROW_NUMBER OVER (PARTITION BY country ORDER BY total_revenue DESC).
    val query2 = spark.sql("""
                WITH SQRY AS (
                  SELECT country, category,
                  SUM(total_amount) AS total_revenue
                FROM retail
                GROUP BY country, category
                )
                SELECT country, category,
                  ROW_NUMBER() OVER (PARTITION BY country ORDER BY total_revenue DESC) AS RK,
                  total_revenue
                FROM SQRY
                """)
    query2.show()
    //   SQL query 2: Hecha de otra forma
    val topCategoriesSql =
                "WITH category_revenue AS (" +
                  "  SELECT country, category, SUM(quantity * unit_price) AS total_revenue " +
                  "  FROM retail " +
                  "  GROUP BY country, category" +
                  "), ranked AS (" +
                  "  SELECT country, category, total_revenue, " +
                  "         ROW_NUMBER() OVER (PARTITION BY country ORDER BY total_revenue DESC) AS rn " +
                  "  FROM category_revenue" +
                  ") " +
                  "SELECT country, category, total_revenue " +
                  "FROM ranked " +
                  "WHERE rn <= 5 " +
                  "ORDER BY country, total_revenue DESC"
    val topCategories = spark.sql(topCategoriesSql)
    topCategories.show(false)

    // 5. Implement spamRisk UDF and register it.
    // UDF registration
    val spamRiskUdf = udf(spamRisk _)
    spark.udf.register("spam_risk", spamRisk _)

    // 6. Apply spam_risk in DataFrame API and SQL.
    // Example: apply to a customers DataFrame if available
    val customersDf = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/customers.csv")

    val customersWithRiskDf = customersDf.withColumn(
      "spam_risk",
      spamRiskUdf(col("email"))
    )

    println("Customers with spam_risk (DataFrame API):")
    customersWithRiskDf.show(false)

    customersDf.createOrReplaceTempView("customers")

    val riskSql =
      "SELECT *, spam_risk(email) AS spam_risk FROM customers"

    val customersWithRiskSql = spark.sql(riskSql)
    println("Customers with spam_risk (SQL):")
    customersWithRiskSql.show(false)
	
    spark.stop()
  }

}

// ------------------------------------------------------------
// 5. Structured Streaming – Real-Time Word Count
// ------------------------------------------------------------
object StreamingWordCountAppExercises {

  def main(args: Array[String]): Unit = {
    // 1. Create SparkSession (with spark.sql.shuffle.partitions etc. if desired).
    val spark = SparkSession.builder()
      .appName("StreamingWordCountAppExercises")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "8") // optional tuning
      .config("spark.executor.memory", "2g")       // example extra config
      .getOrCreate()
    // Verify SparkSession
    println(s"Spark version: ${spark.version}")

    import spark.implicits._   //Permite usar ($"customer_id"), sino col("customer_id")

    // 2. Build streaming query for global word counts.
    // Read streaming data from socket
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()
    // Show schema
    lines.printSchema()

    // Split into words
    val words = lines
      .select(explode(split(col("value"), "\\s+")).as("word"))
      .filter(length(col("word")) > 0)
      .withColumn("word", lower(col("word")))

    // Global running count
    val globalCounts = words
      .groupBy("word")
      .count()

    val globalQuery = globalCounts.writeStream
      .outputMode("update")
      .format("console")
      .option("checkpointLocation", "checkpoint/global_wordcount")
      .start()

    // 3. Build second query with windowed word counts.
    // Windowed counts using processing time as timestamp
    val withTs = words.withColumn("timestamp", current_timestamp())

    val windowedCounts = withTs
      .withWatermark("timestamp", "10 minutes")
      .groupBy(
        window(col("timestamp"), "10 minutes", "5 minutes"),
        col("word")
      )
      .count()

    val windowedQuery = windowedCounts.writeStream
      .outputMode("update")
      .format("console")
      .option("truncate", "false")
      .option("checkpointLocation", "checkpoint/windowed_wordcount")
      .start()

    globalQuery.awaitTermination()
    windowedQuery.awaitTermination()

    // 4. Start queries and awaitTermination.
    spark.stop()
  }

}

// ------------------------------------------------------------
// 6. Performance Tuning & Partitioning
// ------------------------------------------------------------


// ------------------------------------------------------------
// 7. Mini Project – Log Analytics End-to-End
// ------------------------------------------------------------


// git status
// git add src/main/scala/App.scala
// git commit -m "commit changes"
// git push
