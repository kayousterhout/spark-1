/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * This harness is based on (and mostly unchanged from) a TPCDS harness written by Michael Ambrust.
 */

// scalastyle:off
package org.apache.spark.sql.parquet // This is a hack until parquet has better support for partitioning.

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

import _root_.parquet.hadoop.ParquetOutputFormat
import _root_.parquet.hadoop.util.ContextUtil
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapred.{FileOutputFormat, JobConf}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat => NewFileInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat => NewFileOutputFormat}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration

import org.apache.spark.Logging
import org.apache.spark.SerializableWritable
import org.apache.spark.mapreduce.SparkHadoopMapReduceUtil
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.{UnresolvedRelation, Star}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.hive.HiveShim
import org.apache.spark.sql.hive.HiveMetastoreTypes
import org.apache.spark.storage.StorageLevel

import org.apache.spark.sql.catalyst.{ScalaReflection, planning}

import scala.sys.process._
import scala.concurrent._
import scala.concurrent.duration._
import ExecutionContext.Implicits.global
import org.apache.spark.util.Utils

abstract class TableType
case object DimensionTable extends TableType
case class FactTable(partitionColumn: String) extends TableType

case class BenchmarkConfiguration(
    scaleFactor: Int,
    useDecimal: Boolean,
    partitionFactTables: Boolean,
    sqlConf: Map[String, String],
    sparkConf: Map[String,String],
    cores: Int)

case class BenchmarkResult(
    name: String,
    joinTypes: Seq[String],
    tables: Seq[String],
    parsingTime: Double,
    analysisTime: Double,
    optimizationTime: Double,
    planningTime: Double,
    executionTime: Double)

case class Variation[T](name: String, options: Seq[T])(val setup: T => Unit)

case class ExperimentRun(
    timestamp: Long,
    iteration: Int,
    tags: Map[String, String],
    configuration: BenchmarkConfiguration,
    results: Seq[BenchmarkResult])

/**
 * A harness for running TPC-DS with Spark SQL.
 */
class TPCDS(
    @transient sqlContext: SQLContext,
    scaleFactor: Int = 1,
    dataLocation: String = "/data/tpcds",
    resultsLocation: String = "/perf/tpcds/",
    partitionFactTables: Boolean = true,
    useDecimal: Boolean = true,
    maxRowsPerPartitions: Int = 20 * 1000 * 1000) extends Serializable with Logging with SparkHadoopMapReduceUtil {
  import sqlContext._

  def baseDir = s"$dataLocation/scaleFactor=$scaleFactor/useDecimal=$useDecimal"

  val toolsDir = "/root/tpcds-kit/tools"
  val dsdgen = s"$toolsDir/dsdgen"

  def allResults =
    jsonRDD(
      sparkContext.textFile(s"$resultsLocation/*/"))
      //ScalaReflection.schemaFor[ExperimentRun].dataType.asInstanceOf[StructType])

  def currentConfiguration = BenchmarkConfiguration(
    scaleFactor = scaleFactor,
    useDecimal = useDecimal,
    partitionFactTables = partitionFactTables,
    sqlConf = sqlContext.getAllConfs,
    sparkConf = sparkContext.conf.getAll.toMap,
    cores = sparkContext.defaultMinPartitions)


  case class CommandResult(exitCode: Int, stdout: String, stderr: String)

  def bashCommand(cmd: String): CommandResult = {
    val command = Seq("bash", "-c", cmd)
    val strOut = new StringBuilder
    val strErr = new StringBuilder
    val logge = ProcessLogger(
      (o: String) => strOut.append(o + "\n"),
      (e: String) => strErr.append(e + "\n"))
    val exitCode = command ! logge
    CommandResult(exitCode, strOut.toString, strErr.toString)
  }

  def bashCommands(cmds: String*) = bashCommand(cmds.mkString(" && "))

  val storedAsParquet = "0.12.0" match { // Change based on HiveVersion.
    case "0.12.0" =>
      """ROW FORMAT SERDE 'parquet.hive.serde.ParquetHiveSerDe'
        |STORED AS INPUTFORMAT 'parquet.hive.DeprecatedParquetInputFormat'
        |OUTPUTFORMAT 'parquet.hive.DeprecatedParquetOutputFormat'""".stripMargin
    case "0.13.1" =>
      "STORED AS PARQUET"
  }

  def allStats = tables.map(_.stats).reduceLeft(_.unionAll(_))

  /**
   * This should be called before running any experiments, to make sure that the data is loaded,
   * the temporary tables have been created, and the small tables have been set to broadcast.
   */
  def setupExperiment(): Unit = {
    logInfo(s"Setting up experiment with scale factor $scaleFactor")
    checkData()
    parquetTables()
    setupBroadcast()
  }

  /**
   * Runs numUsers concurrent query streams that each subset TPC-DS queries in a random order.
   *
   * You should ideally run a single experiment before doing this one, to warmup the JVM and
   * get some of the code to run these queries JIT-ed.
   *
   * The TPC-DS spec defines the appropriate amount of concurrency based on the scale factor as
   * follows:
   * SF 100: 7 users
   * SF 300: 9 users
   * SF 1000: 11 users
   * SF 3000: 13 users
   * SF 10000: 15 users
   * SF 30000: 17 users
   * SF 100000: 19 users
   */
  def runConcurrentUserStreams(numUsers: Int, iterationsPerUser: Int = 1) {
    val experimentExecutor = Utils.newDaemonFixedThreadPool(
      numUsers, "tpcds-executor")
    experimentExecutor.execute(new Runnable {
      override def run() {
        val shuffled = scala.util.Random.shuffle(queries)
        (1 to iterationsPerUser).flatMap { i =>
          ExperimentRun(
            timestamp = timestamp,
            iteration = i,
            tags = currentOptions.toMap ++ tags,
            configuration = currentConfiguration,
            shuffled.flatMap { q =>
              val setup = s"iteration: $i, ${currentOptions.map { case (k, v) => s"$k=$v"}.mkString(", ")}"
              currentMessages += s"Running query ${q.name} $setup"

              currentQuery = q.name
              val singleResult = try q.benchmark(setup, userId) :: Nil catch {
                case e: Exception =>
                  currentMessages += s"Failed to run query ${q.name}: $e"
                  Nil
              }
              currentResults ++= singleResult
              singleResult
            }
          )
        }
      }
    })
  }

  def runExperiment(
      iterations: Int = 3,
      queriesToRun: Seq[Query] = queries,
      variations: Seq[Variation[_]] = Seq(Variation("StandardRun", Seq("")) { _ => {} }),
      tags: Map[String, String] = Map.empty,
      userId: String = "TPCDS-user") = {

    class ExperimentStatus {
      val currentResults = new collection.mutable.ArrayBuffer[BenchmarkResult]()
      val currentRuns = new collection.mutable.ArrayBuffer[ExperimentRun]()
      val currentMessages = new collection.mutable.ArrayBuffer[String]()

      @volatile
      var currentQuery = ""

      val timestamp = System.currentTimeMillis()
      val combinations = cartesianProduct(variations.map(l => (0 until l.options.size).toList).toList)
      val resultsFuture = future {
        val results = (1 to iterations).flatMap { i =>
          combinations.map { setup =>
            val currentOptions = variations.asInstanceOf[Seq[Variation[Any]]].zip(setup).map {
              case (v, idx) =>
                v.setup(v.options(idx))
                v.name -> v.options(idx).toString
            }

            val result = ExperimentRun(
              timestamp = timestamp,
              iteration = i,
              tags = currentOptions.toMap ++ tags,
              configuration = currentConfiguration,
              queriesToRun.flatMap { q =>
                val setup = s"iteration: $i, ${currentOptions.map { case (k, v) => s"$k=$v"}.mkString(", ")}"
                currentMessages += s"Running query ${q.name} $setup"

                currentQuery = q.name
                val singleResult = try q.benchmark(setup, userId) :: Nil catch {
                  case e: Exception =>
                    currentMessages += s"Failed to run query ${q.name}: $e"
                    Nil
                }
                currentResults ++= singleResult
                singleResult
              })
            currentRuns += result

            result
          }
        }

        val resultsTable = sparkContext.parallelize(results).toSchemaRDD
        currentMessages +=  s"Results stored to: $resultsLocation/$timestamp"
        resultsTable.toJSON.coalesce(1).saveAsTextFile(s"$resultsLocation/$timestamp")
        resultsTable
      }

      /** Returns results from an actively running experiment. */
      def getCurrentResults() = {
        val tbl = sparkContext.parallelize(currentResults)
        tbl.registerTempTable("currentResults")
        tbl
      }

      /** Returns full iterations from an actively running experiment. */
      def getCurrentRuns() = {
        val tbl = sparkContext.parallelize(currentRuns)
        tbl.registerTempTable("currentRuns")
        tbl
      }

      def tail(n: Int = 5) = {
        currentMessages.takeRight(n).mkString("\n")
      }

      def status =
        if (resultsFuture.isCompleted) {
          if (resultsFuture.value.get.isFailure) "Failed" else "Successful"
        } else {
          "Running"
        }

      override def toString =
        s"""
           |=== $status Experiment ===
           |Permalink: table("allResults").where('timestamp === ${timestamp}L)
           |Queries: ${queriesToRun.map(_.name).map(n => if(n == currentQuery) s"|$n|" else n).mkString(" ")}
           |Iterations complete: ${currentRuns.size / combinations.size} / $iterations
           |Queries run: ${currentResults.size} / ${iterations * combinations.size * queriesToRun.size}
           |Run time: ${(System.currentTimeMillis() - timestamp) / 1000}s
           |
           |== Logs ==
           |${tail()}
         """.stripMargin
    }
    new ExperimentStatus
  }

  def cartesianProduct[T](xss: List[List[T]]): List[List[T]] = xss match {
    case Nil => List(Nil)
    case h :: t => for(xh <- h; xt <- cartesianProduct(t)) yield xh :: xt
  }

  def checkData(): Unit = {
    tables.foreach { table =>
      val fs = FileSystem.get(new java.net.URI(table.outputDir), new Configuration())
      val exists = fs.exists(new Path(table.outputDir))
      val wasSuccessful = fs.exists(new Path(s"${table.outputDir}/_SUCCESS"))

      if (!wasSuccessful) {
        if (exists) {
          println(s"Table '${table.name}' not generated successfully, regenerating.")
        } else {
          println(s"Table '${table.name}' does not exist, generating.")
        }
        fs.delete(new Path(table.outputDir), true)
        table.saveToParquet()
      } else {
        println(s"Table ${table.name} already exists.")
      }
    }
  }

  /**
   * Registers all of the tables (should only be called after data has been loaded using
   * checkData()).
   */
  def parquetTables(): Unit = {
    tables.foreach(_.createTempTable())
  }

  case class Table(name: String, tableType: TableType, attributes: Attribute*) {
    val outputDir = s"$baseDir/parquet/$name"

    def parquetFile = sqlContext.parquetFile(outputDir)

    val partitions = tableType match {
      case FactTable(_) if partitionFactTables => scaleFactor
      case _ => 1
    }

    val generatedData = {
      sparkContext.parallelize(1 to partitions, partitions).flatMap { i =>
        val localToolsDir = if (new java.io.File(dsdgen).exists) {
          toolsDir
        } else if (new java.io.File(s"/$dsdgen").exists) {
          s"/$toolsDir"
        } else {
          sys.error(s"Could not find dsdgen at $dsdgen or /$dsdgen. Run install")
        }

        val parallel = if (partitions > 1) s"-parallel $partitions -child $i" else ""
        val commands = Seq(
          "bash", "-c",
          s"cd $localToolsDir && ./dsdgen -table $name -filter Y -scale $scaleFactor $parallel")
        println(commands)
        commands.lines
      }
    }

    generatedData.setName(s"$name, sf=$scaleFactor, strings")
    //generatedData.persist(StorageLevel.DISK_ONLY)

    val schema =
      StructType(attributes.map(a =>
        StructField(
          a.name,
          if (a.dataType.isInstanceOf[DecimalType] && !useDecimal) FloatType else a.dataType)))

    val rows = generatedData.mapPartitions { iter =>
      val currentRow = new GenericMutableRow(schema.fields.size)
      iter.map { l =>
        (0 until schema.fields.length).foreach(currentRow.setNullAt)
        l.split("\\|", -1).zipWithIndex.dropRight(1).foreach { case (f, i) => currentRow(i) = f}
        currentRow: Row
      }
    }

    val stringData =
      sqlContext.applySchema(
        rows,
        StructType(schema.fields.map(a => StructField(a.name, StringType))))

    val convertedData =
      stringData.select(schema.fields.map(a => a.name.attr.cast(a.dataType).as(Symbol(a.name))): _*)

    def fromCatalog = table(name)

    def stats =
      fromCatalog.select(
        name as 'tableName,
        count(1) as 'numRows,
        Literal(fromCatalog.queryExecution.optimizedPlan.statistics.sizeInBytes.toLong) as 'sizeInBytes)

    def createHiveTable(): Unit = {
      tableType match {
        case FactTable(partitioningColumn) if partitionFactTables =>
          val schemaString =
            schema.fields
              .filterNot(_.name == partitioningColumn)
              .map(f => s"${f.name} ${HiveMetastoreTypes.toMetastoreType(f.dataType)}")
              .mkString(",\n")

          val ddl = s"""
          |CREATE EXTERNAL TABLE $name (
          |  $schemaString
          |)
          |PARTITIONED BY ($partitioningColumn int)
          |$storedAsParquet
          |LOCATION '$outputDir'""".stripMargin

          sql(ddl)

          val fs = FileSystem.get(new java.net.URI(outputDir), new Configuration())
          val partitionNames =
            fs.listStatus(new Path(outputDir))
              .map(_.getPath.toString.stripSuffix("/"))
              .filterNot(_ contains "null")

          partitionNames.foreach { part =>
            sql( s"""
          ALTER TABLE store_sales ADD PARTITION (${part.split("\\/").last})
          location '$part'
          """)
          }
        case _ =>
          val schemaString =
            schema.fields
              .map(f => s"${f.name} ${HiveMetastoreTypes.toMetastoreType(f.dataType)}")
              .mkString(",\n")

          val ddl = s"""
          |CREATE EXTERNAL TABLE $name (
          |  $schemaString
          |)
          |$storedAsParquet
          |LOCATION '$outputDir'""".stripMargin

          sql(ddl)
      }

      sql(s"ANALYZE TABLE $name COMPUTE STATISTICS noscan")
    }

    /** Registers the given table. */
    def createTempTable(): Unit = {
      sql(
        s"""
          |CREATE TEMPORARY TABLE ${name}
          |USING org.apache.spark.sql.parquet
          |OPTIONS (
          |  path '${outputDir}'
          |)
        """.stripMargin)
    }

    def saveToParquet(): Unit = {
      tableType match {
        // This is an awful hack... spark sql parquet should support this natively.
        case FactTable(partitioningColumn) if partitionFactTables =>
          setConf("spark.sql.planner.externalSort", "true")
          val output = convertedData.queryExecution.analyzed.output
          val job = new Job(sqlContext.sparkContext.hadoopConfiguration)

          val writeSupport =
            if (schema.fields.map(_.dataType).forall(_.isPrimitive)) {
              classOf[org.apache.spark.sql.parquet.MutableRowWriteSupport]
            } else {
              classOf[org.apache.spark.sql.parquet.RowWriteSupport]
            }

          ParquetOutputFormat.setWriteSupportClass(job, writeSupport)

          val conf = new SerializableWritable(ContextUtil.getConfiguration(job))
          org.apache.spark.sql.parquet.RowWriteSupport.setSchema(schema.toAttributes, conf.value)

          val partColumnAttr =
            BindReferences.bindReference[Expression](
              output.find(_.name == partitioningColumn).get,
              output)

          val formatter = new SimpleDateFormat("yyyyMMddHHmm")
          val jobTrackerId = formatter.format(new Date())
          // TODO: this 0 is a bad idea -- need a unique stage id?
          val stageId = 15
          val jobAttemptId = newTaskAttemptID(jobTrackerId, stageId, isMap=true, 0, 0)
          val outputFormat = job.getOutputFormatClass.newInstance()
          val jobTaskContext = newTaskAttemptContext(conf.value, jobAttemptId)
          val jobCommitter = outputFormat.getOutputCommitter(jobTaskContext)
          jobCommitter.setupJob(jobTaskContext)


          // TODO: clusterBy would be faster than orderBy
          convertedData.where(IsNotNull(partitioningColumn.attr)).orderBy(partitioningColumn.attr.asc).queryExecution.toRdd.foreachPartition { iter =>
            var writer: RecordWriter[Void, Row] = null
            val getPartition = new InterpretedMutableProjection(Seq(partColumnAttr))
            var currentPartition: Row = null
            var hadoopContext: TaskAttemptContext = null
            var committer: OutputCommitter = null

            var rowCount = 0
            var partition = 0

            while (iter.hasNext) {
              val currentRow = iter.next()

              rowCount += 1
              if (rowCount >= maxRowsPerPartitions) {
                rowCount = 0
                partition += 1
                println(s"Starting partition $partition")
                if (writer != null) {
                  writer.close(hadoopContext)
                  logInfo("KMI Committing task")
                  committer.commitTask(hadoopContext)
                }
                writer = null
              }

              if ((getPartition(currentRow) != currentPartition || writer == null) &&
                !getPartition.currentValue.isNullAt(0)) {
                rowCount = 0
                currentPartition = getPartition.currentValue.copy()
                if (writer != null) {
                  writer.close(hadoopContext)
                  logInfo("KMI Committing task 2")
                  committer.commitTask(hadoopContext)
                }

                val job = new JobConf(conf.value)
                val keyType = classOf[Void]
                job.setOutputKeyClass(keyType)
                job.setOutputValueClass(classOf[Row])
                FileOutputFormat.setOutputPath(
                  job,
                  new Path(s"$outputDir/$partitioningColumn=${currentPartition(0)}"))
                val formatter = new SimpleDateFormat("yyyyMMddHHmm")
                val jobtrackerID = formatter.format(new Date())

                val attemptNumber = 1
                /* "reduce task" <split #> <attempt # = spark task #> */
                val attemptId = newTaskAttemptID("kay", 15, isMap=false, partition, attemptNumber)
                hadoopContext = newTaskAttemptContext(job, attemptId)
                val format = new ParquetOutputFormat[Row]
                committer = format.getOutputCommitter(hadoopContext)
                committer.setupTask(hadoopContext)
                writer = format.getRecordWriter(hadoopContext)

              }
              if (!getPartition.currentValue.isNullAt(0)) {
                writer.write(null, currentRow)
              }
            }
            if (writer != null) {
              writer.close(hadoopContext)
              logInfo("KMI committing task 3")
              committer.commitTask(hadoopContext)
            }
          }

          val fs = FileSystem.get(new java.net.URI(outputDir), new Configuration())
          fs.listStatus(new Path(outputDir)).map { dir =>
            val job = new JobConf(conf.value)
            FileOutputFormat.setOutputPath(job, dir.getPath)
            logInfo(s"KMI committing output for job ${dir.getPath}")
            val attemptId = newTaskAttemptID("kay", 15, isMap = true, 0, 0)
            val hadoopContext = newTaskAttemptContext(job, attemptId)
            val format = new ParquetOutputFormat[Row]
            val committer = format.getOutputCommitter(hadoopContext)
            val jobTaskContext = newTaskAttemptContext(conf.value, attemptId)
            committer.commitJob(jobTaskContext)
          }
          logInfo("KMI committing job")
          jobCommitter.commitJob(jobTaskContext)
          fs.create(new Path(s"$outputDir/_SUCCESS")).close()
        case _ => convertedData.saveAsParquetFile(outputDir)
      }
    }
  }

  def setupBroadcast(skipTables: Seq[String] = Seq("store_sales", "customer")) = {
    val skipExpr = skipTables.map(t => !('tableName === t)).reduceLeft[Expression](_ && _)
    val threshold =
      allStats
        .where(skipExpr)
        .select(max('sizeInBytes))
        .first()
        .getLong(0)
    val setQuery = s"SET spark.sql.autoBroadcastJoinThreshold=$threshold"

    println(setQuery)
    sql(setQuery)
  }

  @transient
  val tables = Seq(
    /* This is another large table that we don't build yet.
    Table("inventory",
      FactTable("inv_date_sk"),
      'inv_date_sk          .int,
      'inv_item_sk          .int,
      'inv_warehouse_sk     .int,
      'inv_quantity_on_hand .int),*/
    Table("store_sales",
      FactTable("ss_sold_date_sk"),
      'ss_sold_date_sk      .int,
      'ss_sold_time_sk      .int,
      'ss_item_sk           .int,
      'ss_customer_sk       .int,
      'ss_cdemo_sk          .int,
      'ss_hdemo_sk          .int,
      'ss_addr_sk           .int,
      'ss_store_sk          .int,
      'ss_promo_sk          .int,
      'ss_ticket_number     .int,
      'ss_quantity          .int,
      'ss_wholesale_cost    .decimal(7,2),
      'ss_list_price        .decimal(7,2),
      'ss_sales_price       .decimal(7,2),
      'ss_ext_discount_amt  .decimal(7,2),
      'ss_ext_sales_price   .decimal(7,2),
      'ss_ext_wholesale_cost.decimal(7,2),
      'ss_ext_list_price    .decimal(7,2),
      'ss_ext_tax           .decimal(7,2),
      'ss_coupon_amt        .decimal(7,2),
      'ss_net_paid          .decimal(7,2),
      'ss_net_paid_inc_tax  .decimal(7,2),
      'ss_net_profit        .decimal(7,2)),
    Table("customer",
      DimensionTable,
      'c_customer_sk             .int,
      'c_customer_id             .string,
      'c_current_cdemo_sk        .int,
      'c_current_hdemo_sk        .int,
      'c_current_addr_sk         .int,
      'c_first_shipto_date_sk    .int,
      'c_first_sales_date_sk     .int,
      'c_salutation              .string,
      'c_first_name              .string,
      'c_last_name               .string,
      'c_preferred_cust_flag     .string,
      'c_birth_day               .int,
      'c_birth_month             .int,
      'c_birth_year              .int,
      'c_birth_country           .string,
      'c_login                   .string,
      'c_email_address           .string,
      'c_last_review_date        .string),
    Table("customer_address",
      DimensionTable,
      'ca_address_sk             .int,
      'ca_address_id             .string,
      'ca_street_number          .string,
      'ca_street_name            .string,
      'ca_street_type            .string,
      'ca_suite_number           .string,
      'ca_city                   .string,
      'ca_county                 .string,
      'ca_state                  .string,
      'ca_zip                    .string,
      'ca_country                .string,
      'ca_gmt_offset             .decimal(5,2),
      'ca_location_type          .string),
    Table("customer_demographics",
      DimensionTable,
      'cd_demo_sk                .int,
      'cd_gender                 .string,
      'cd_marital_status         .string,
      'cd_education_status       .string,
      'cd_purchase_estimate      .int,
      'cd_credit_rating          .string,
      'cd_dep_count              .int,
      'cd_dep_employed_count     .int,
      'cd_dep_college_count      .int),
    Table("date_dim",
      DimensionTable,
      'd_date_sk                 .int,
      'd_date_id                 .string,
      'd_date                    .string,
      'd_month_seq               .int,
      'd_week_seq                .int,
      'd_quarter_seq             .int,
      'd_year                    .int,
      'd_dow                     .int,
      'd_moy                     .int,
      'd_dom                     .int,
      'd_qoy                     .int,
      'd_fy_year                 .int,
      'd_fy_quarter_seq          .int,
      'd_fy_week_seq             .int,
      'd_day_name                .string,
      'd_quarter_name            .string,
      'd_holiday                 .string,
      'd_weekend                 .string,
      'd_following_holiday       .string,
      'd_first_dom               .int,
      'd_last_dom                .int,
      'd_same_day_ly             .int,
      'd_same_day_lq             .int,
      'd_current_day             .string,
      'd_current_week            .string,
      'd_current_month           .string,
      'd_current_quarter         .string,
      'd_current_year            .string),
    Table("household_demographics",
      DimensionTable,
      'hd_demo_sk                .int,
      'hd_income_band_sk         .int,
      'hd_buy_potential          .string,
      'hd_dep_count              .int,
      'hd_vehicle_count          .int),
    Table("item",
      DimensionTable,
      'i_item_sk                 .int,
      'i_item_id                 .string,
      'i_rec_start_date          .string,
      'i_rec_end_date            .string,
      'i_item_desc               .string,
      'i_current_price           .decimal(7,2),
      'i_wholesale_cost          .decimal(7,2),
      'i_brand_id                .int,
      'i_brand                   .string,
      'i_class_id                .int,
      'i_class                   .string,
      'i_category_id             .int,
      'i_category                .string,
      'i_manufact_id             .int,
      'i_manufact                .string,
      'i_size                    .string,
      'i_formulation             .string,
      'i_color                   .string,
      'i_units                   .string,
      'i_container               .string,
      'i_manager_id              .int,
      'i_product_name            .string),
    Table("promotion",
      DimensionTable,
      'p_promo_sk                .int,
      'p_promo_id                .string,
      'p_start_date_sk           .int,
      'p_end_date_sk             .int,
      'p_item_sk                 .int,
      'p_cost                    .decimal(15,2),
      'p_response_target         .int,
      'p_promo_name              .string,
      'p_channel_dmail           .string,
      'p_channel_email           .string,
      'p_channel_catalog         .string,
      'p_channel_tv              .string,
      'p_channel_radio           .string,
      'p_channel_press           .string,
      'p_channel_event           .string,
      'p_channel_demo            .string,
      'p_channel_details         .string,
      'p_purpose                 .string,
      'p_discount_active         .string),
    Table("store",
      DimensionTable,
      's_store_sk                .int,
      's_store_id                .string,
      's_rec_start_date          .string,
      's_rec_end_date            .string,
      's_closed_date_sk          .int,
      's_store_name              .string,
      's_number_employees        .int,
      's_floor_space             .int,
      's_hours                   .string,
      's_manager                 .string,
      's_market_id               .int,
      's_geography_class         .string,
      's_market_desc             .string,
      's_market_manager          .string,
      's_division_id             .int,
      's_division_name           .string,
      's_company_id              .int,
      's_company_name            .string,
      's_street_number           .string,
      's_street_name             .string,
      's_street_type             .string,
      's_suite_number            .string,
      's_city                    .string,
      's_county                  .string,
      's_state                   .string,
      's_zip                     .string,
      's_country                 .string,
      's_gmt_offset              .decimal(5,2),
      's_tax_precentage          .decimal(5,2)),
    Table("time_dim",
      DimensionTable,
      't_time_sk                 .int,
      't_time_id                 .string,
      't_time                    .int,
      't_hour                    .int,
      't_minute                  .int,
      't_second                  .int,
      't_am_pm                   .string,
      't_shift                   .string,
      't_sub_shift               .string,
      't_meal_time               .string))

  def inventory = tables.find(_.name == "inventory").get
  def store_sales = tables.find(_.name == "store_sales").get
  def customer = tables.find(_.name == "customer").get
  def customer_address = tables.find(_.name == "customer_address").get
  def customer_demographics = tables.find(_.name == "customer_demographics").get
  def date_dim = tables.find(_.name == "date_dim").get
  def household_demographics = tables.find(_.name == "household_demographics").get
  def item = tables.find(_.name == "item").get
  def promotion = tables.find(_.name == "promotion").get
  def store = tables.find(_.name == "store").get
  def time_dim = tables.find(_.name == "time_dim").get

  def benchmarkMs[A](f: => A): Double = {
    val startTime = System.nanoTime()
    val ret = f
    val endTime = System.nanoTime()
    (endTime - startTime).toDouble / 1000000
  }

  case class Query(name: String, sqlText: String) {
    def schemaRDD = sql(sqlText)

    def joinTypes = schemaRDD.queryExecution.executedPlan.collect {
      case k if k.nodeName contains "Join" => k.nodeName
    }

    val tablesInvolved = schemaRDD.queryExecution.logical.collect {
      case UnresolvedRelation(_, tableNames) => tableNames
    }.flatten

    /*
    Rewrite the queries by doing the join with data_dim before computing the partitions to prune.
    Turned off as long as we are using the rewritten queries.
    def getQueryExecution = {

      val rewritten = schemaRDD.queryExecution.optimizedPlan transform {
        case ExtractEquiJoinKeys(
               Inner,
               Seq(leftKey: AttributeReference),
               Seq(rightKey: AttributeReference),
               condition,
               left,
               right) if leftKey.name == "d_date_sk" || rightKey.name == "d_date_sk" =>

          val (smallKey, largeKey) = if (leftKey.name == "d_date_sk") (leftKey, rightKey) else (rightKey, leftKey)
          val (smallSide, largeSide) = if (leftKey.name == "d_date_sk") (left, right) else (right, left)

          val matchingKeys = smallSide.select('d_date_sk).collect.map(_(0)).toSet
          smallSide.join(largeSide).where(leftKey === rightKey && InSet(rightKey, matchingKeys)).queryExecution.logical
      }

      sqlContext.executePlan(rewritten)
    }
    */

    def benchmark(description: String = "", userId: String) = {
      try {
        sparkContext.setLocalProperty("spark.scheduler.pool", userId)
        sparkContext.setJobDescription(s"TPCDS ($userId): $name, $description")
        val queryExecution = schemaRDD.queryExecution
        BenchmarkResult(
          name = name,
          joinTypes = joinTypes,
          tables = tablesInvolved,
          parsingTime = benchmarkMs { queryExecution.logical },
          analysisTime = benchmarkMs { queryExecution.analyzed },
          optimizationTime = benchmarkMs { queryExecution.optimizedPlan },
          planningTime = benchmarkMs { queryExecution.executedPlan },
          executionTime = benchmarkMs { queryExecution.toRdd.map(_.copy()).collect() })
      } catch {
        case e: Exception =>
          throw new RuntimeException(
            s"Failed to benchmark query $name\n${schemaRDD.queryExecution}", e)
      }
    }
  }

  val interactiveQueries = Seq("q19", "q42", "q52", "q55", "q63", "q68", "q73", "q98")
  val reportingQueries = Seq("q3","q7","q27","q43", "q53", "q89")
  val deepAnalyticQueries = Seq("q34", "q46", "q59", "q65",  "q79", "ss_max")

  val allCloudera = interactiveQueries ++ reportingQueries ++ deepAnalyticQueries

  def buildCode(files: Seq[File]) = {
    val quotes = "\"\"\""
    files.filter(_.getName.endsWith(".sql")).foreach { q =>
      println(
        s"""
          |Query("${q.getName.stripSuffix(".sql")}", $quotes
          ${org.apache.spark.sql.catalyst.util.fileToString(q).split("\n").map(l => "|  |" + l).mkString("\n")}
          |  $quotes.stripMargin),""".stripMargin)
    }
  }

  val queries = Seq(
    Query("q19", """
      |-- start query 1 in stream 0 using template query19.tpl
      |select
      |  i_brand_id,
      |  i_brand,
      |  i_manufact_id,
      |  i_manufact,
      |  sum(ss_ext_sales_price) ext_price
      |from
      |  store_sales
      |  join item on (store_sales.ss_item_sk = item.i_item_sk)
      |  join customer on (store_sales.ss_customer_sk = customer.c_customer_sk)
      |  join customer_address on (customer.c_current_addr_sk = customer_address.ca_address_sk)
      |  join store on (store_sales.ss_store_sk = store.s_store_sk)
      |  join date_dim on (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
      |where
      |  --ss_date between '1999-11-01' and '1999-11-30'
      |  ss_sold_date_sk between 2451484 and 2451513
      |  and d_moy = 11
      |  and d_year = 1999
      |  and i_manager_id = 7
      |  and substr(ca_zip, 1, 5) <> substr(s_zip, 1, 5)
      |group by
      |  i_brand,
      |  i_brand_id,
      |  i_manufact_id,
      |  i_manufact
      |order by
      |  ext_price desc,
      |  i_brand,
      |  i_brand_id,
      |  i_manufact_id,
      |  i_manufact
      |limit 100
      |-- end query 1 in stream 0 using template query19.tpl
      """.stripMargin),

    Query("q27", """
      |-- start query 1 in stream 0 using template query27.tpl
      |select
      |  i_item_id,
      |  s_state,
      |  -- grouping(s_state) g_state,
      |  avg(ss_quantity) agg1,
      |  avg(ss_list_price) agg2,
      |  avg(ss_coupon_amt) agg3,
      |  avg(ss_sales_price) agg4
      |from
      |  store_sales
      |  join store on (store_sales.ss_store_sk = store.s_store_sk)
      |  join customer_demographics on (store_sales.ss_cdemo_sk = customer_demographics.cd_demo_sk)
      |  join item on (store_sales.ss_item_sk = item.i_item_sk)
      |  join date_dim on (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
      |where
      |  -- ss_date between '1998-01-01' and '1998-12-31'
      |  ss_sold_date_sk between 2450815 and 2451179  -- partition key filter
      |  and d_year = 1998
      |  and s_state in ('WI', 'CA', 'TX', 'FL', 'WA', 'TN')
      |  and cd_gender = 'F'
      |  and cd_marital_status = 'W'
      |  and cd_education_status = 'Primary'
      |group by
      |  -- rollup(i_item_id, s_state)
      |  i_item_id,
      |  s_state
      |order by
      |  i_item_id,
      |  s_state
      |limit 100
      |-- end query 1 in stream 0 using template query27.tpl
      """.stripMargin),

    Query("q3", """
      |-- start query 1 in stream 0 using template query3.tpl
      |select
      |  dt.d_year,
      |  -- year(ss_date) as d_year,
      |  -- case
      |  --   when ss_sold_date_sk between 2451149 and 2451179 then 1998
      |  --   when ss_sold_date_sk between 2451514 and 2451544 then 1999
      |  --   when ss_sold_date_sk between 2451880 and 2451910 then 2000
      |  --   when ss_sold_date_sk between 2452245 and 2452275 then 2001
      |  --   when ss_sold_date_sk between 2452610 and 2452640 then 2002
      |  -- end as d_year,
      |  item.i_brand_id brand_id,
      |  item.i_brand brand,
      |  sum(ss_ext_sales_price) sum_agg
      |from
      |  store_sales
      |  join item on (store_sales.ss_item_sk = item.i_item_sk)
      |  join date_dim dt on (dt.d_date_sk = store_sales.ss_sold_date_sk)
      |where
      |  item.i_manufact_id = 436
      |  and dt.d_moy = 12
      |  -- and (ss_date between '1998-12-01' and '1998-12-31'
      |  --   or ss_date between '1999-12-01' and '1999-12-31'
      |  --   or ss_date between '2000-12-01' and '2000-12-31'
      |  --   or ss_date between '2001-12-01' and '2001-12-31'
      |  --   or ss_date between '2002-12-01' and '2002-12-31')
      |  and (ss_sold_date_sk between 2451149 and 2451179
      |    or ss_sold_date_sk between 2451514 and 2451544
      |    or ss_sold_date_sk between 2451880 and 2451910
      |    or ss_sold_date_sk between 2452245 and 2452275
      |    or ss_sold_date_sk between 2452610 and 2452640)
      |group by
      |  d_year,
      |  item.i_brand,
      |  item.i_brand_id
      |order by
      |  d_year,
      |  sum_agg desc,
      |  brand_id
      |-- end query 1 in stream 0 using template query3.tpl
      |limit 100
      """.stripMargin),

    Query("q34", """
      |-- start query 1 in stream 0 using template query34.tpl
      |select
      |  c_last_name,
      |  c_first_name,
      |  c_salutation,
      |  c_preferred_cust_flag,
      |  ss_ticket_number,
      |  cnt
      |from
      |  (select
      |    ss_ticket_number,
      |    ss_customer_sk,
      |    count(*) cnt
      |  from
      |    store_sales
      |    join household_demographics on (store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk)
      |    join store on (store_sales.ss_store_sk = store.s_store_sk)
      |    join date_dim on (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
      |  where
      |    date_dim.d_year in (1998, 1998 + 1, 1998 + 2)
      |    and (date_dim.d_dom between 1 and 3
      |      or date_dim.d_dom between 25 and 28)
      |    and (household_demographics.hd_buy_potential = '>10000'
      |      or household_demographics.hd_buy_potential = 'unknown')
      |    and household_demographics.hd_vehicle_count > 0
      |    and (case when household_demographics.hd_vehicle_count > 0 then household_demographics.hd_dep_count / household_demographics.hd_vehicle_count else null end) > 1.2
      |    and store.s_county in ('Saginaw County', 'Sumner County', 'Appanoose County', 'Daviess County', 'Fairfield County', 'Raleigh County', 'Ziebach County', 'Williamson County')
      |    and ss_sold_date_sk between 2450816 and 2451910 -- partition key filter
      |  group by
      |    ss_ticket_number,
      |    ss_customer_sk
      |  ) dn
      |join customer on (dn.ss_customer_sk = customer.c_customer_sk)
      |where
      |  cnt between 15 and 20
      |order by
      |  c_last_name,
      |  c_first_name,
      |  c_salutation,
      |  c_preferred_cust_flag desc
      |limit 1000
      |-- end query 1 in stream 0 using template query34.tpl
      """.stripMargin),

    Query("q42", """
      |-- start query 1 in stream 0 using template query42.tpl
      |select
      |  d_year,
      |  i_category_id,
      |  i_category,
      |  sum(ss_ext_sales_price) as total_price
      |from
      |  store_sales
      |  join item on (store_sales.ss_item_sk = item.i_item_sk)
      |  join date_dim dt on (dt.d_date_sk = store_sales.ss_sold_date_sk)
      |where
      |  item.i_manager_id = 1
      |  and dt.d_moy = 12
      |  and dt.d_year = 1998
      |  -- and ss_date between '1998-12-01' and '1998-12-31'
      |  and ss_sold_date_sk between 2451149 and 2451179  -- partition key filter
      |group by
      |  d_year,
      |  i_category_id,
      |  i_category
      |order by
      |  -- sum(ss_ext_sales_price) desc,
      |  total_price desc,
      |  d_year,
      |  i_category_id,
      |  i_category
      |limit 100
      |-- end query 1 in stream 0 using template query42.tpl
      """.stripMargin),

    Query("q43", """
      |-- start query 1 in stream 0 using template query43.tpl
      |select
      |  s_store_name,
      |  s_store_id,
      |  sum(case when (d_day_name = 'Sunday') then ss_sales_price else null end) sun_sales,
      |  sum(case when (d_day_name = 'Monday') then ss_sales_price else null end) mon_sales,
      |  sum(case when (d_day_name = 'Tuesday') then ss_sales_price else null end) tue_sales,
      |  sum(case when (d_day_name = 'Wednesday') then ss_sales_price else null end) wed_sales,
      |  sum(case when (d_day_name = 'Thursday') then ss_sales_price else null end) thu_sales,
      |  sum(case when (d_day_name = 'Friday') then ss_sales_price else null end) fri_sales,
      |  sum(case when (d_day_name = 'Saturday') then ss_sales_price else null end) sat_sales
      |from
      |  store_sales
      |  join store on (store_sales.ss_store_sk = store.s_store_sk)
      |  join date_dim on (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
      |where
      |  s_gmt_offset = -5
      |  and d_year = 1998
      |  -- and ss_date between '1998-01-01' and '1998-12-31'
      |  and ss_sold_date_sk between 2450816 and 2451179  -- partition key filter
      |group by
      |  s_store_name,
      |  s_store_id
      |order by
      |  s_store_name,
      |  s_store_id,
      |  sun_sales,
      |  mon_sales,
      |  tue_sales,
      |  wed_sales,
      |  thu_sales,
      |  fri_sales,
      |  sat_sales
      |limit 100
      |-- end query 1 in stream 0 using template query43.tpl
      """.stripMargin),

    Query("q46", """
      |-- start query 1 in stream 0 using template query46.tpl
      |select
      |  c_last_name,
      |  c_first_name,
      |  ca_city,
      |  bought_city,
      |  ss_ticket_number,
      |  amt,
      |  profit
      |from
      |  (select
      |    ss_ticket_number,
      |    ss_customer_sk,
      |    ca_city bought_city,
      |    sum(ss_coupon_amt) amt,
      |    sum(ss_net_profit) profit
      |  from
      |    store_sales
      |    join store on (store_sales.ss_store_sk = store.s_store_sk)
      |    join household_demographics on (store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk)
      |    join date_dim on (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
      |    join customer_address on (store_sales.ss_addr_sk = customer_address.ca_address_sk)
      |  where
      |    store.s_city in ('Midway', 'Concord', 'Spring Hill', 'Brownsville', 'Greenville')
      |    and (household_demographics.hd_dep_count = 5
      |      or household_demographics.hd_vehicle_count = 3)
      |    and date_dim.d_dow in (6, 0)
      |    and date_dim.d_year in (1999, 1999 + 1, 1999 + 2)
      |    -- and ss_date between '1999-01-01' and '2001-12-31'
      |    -- and ss_sold_date_sk between 2451180 and 2452275  -- partition key filter
      |    and ss_sold_date_sk in (2451181, 2451182, 2451188, 2451189, 2451195, 2451196, 2451202, 2451203, 2451209, 2451210, 2451216, 2451217,
      |                            2451223, 2451224, 2451230, 2451231, 2451237, 2451238, 2451244, 2451245, 2451251, 2451252, 2451258, 2451259,
      |                            2451265, 2451266, 2451272, 2451273, 2451279, 2451280, 2451286, 2451287, 2451293, 2451294, 2451300, 2451301,
      |                            2451307, 2451308, 2451314, 2451315, 2451321, 2451322, 2451328, 2451329, 2451335, 2451336, 2451342, 2451343,
      |                            2451349, 2451350, 2451356, 2451357, 2451363, 2451364, 2451370, 2451371, 2451377, 2451378, 2451384, 2451385,
      |                            2451391, 2451392, 2451398, 2451399, 2451405, 2451406, 2451412, 2451413, 2451419, 2451420, 2451426, 2451427,
      |                            2451433, 2451434, 2451440, 2451441, 2451447, 2451448, 2451454, 2451455, 2451461, 2451462, 2451468, 2451469,
      |                            2451475, 2451476, 2451482, 2451483, 2451489, 2451490, 2451496, 2451497, 2451503, 2451504, 2451510, 2451511,
      |                            2451517, 2451518, 2451524, 2451525, 2451531, 2451532, 2451538, 2451539, 2451545, 2451546, 2451552, 2451553,
      |                            2451559, 2451560, 2451566, 2451567, 2451573, 2451574, 2451580, 2451581, 2451587, 2451588, 2451594, 2451595,
      |                            2451601, 2451602, 2451608, 2451609, 2451615, 2451616, 2451622, 2451623, 2451629, 2451630, 2451636, 2451637,
      |                            2451643, 2451644, 2451650, 2451651, 2451657, 2451658, 2451664, 2451665, 2451671, 2451672, 2451678, 2451679,
      |                            2451685, 2451686, 2451692, 2451693, 2451699, 2451700, 2451706, 2451707, 2451713, 2451714, 2451720, 2451721,
      |                            2451727, 2451728, 2451734, 2451735, 2451741, 2451742, 2451748, 2451749, 2451755, 2451756, 2451762, 2451763,
      |                            2451769, 2451770, 2451776, 2451777, 2451783, 2451784, 2451790, 2451791, 2451797, 2451798, 2451804, 2451805,
      |                            2451811, 2451812, 2451818, 2451819, 2451825, 2451826, 2451832, 2451833, 2451839, 2451840, 2451846, 2451847,
      |                            2451853, 2451854, 2451860, 2451861, 2451867, 2451868, 2451874, 2451875, 2451881, 2451882, 2451888, 2451889,
      |                            2451895, 2451896, 2451902, 2451903, 2451909, 2451910, 2451916, 2451917, 2451923, 2451924, 2451930, 2451931,
      |                            2451937, 2451938, 2451944, 2451945, 2451951, 2451952, 2451958, 2451959, 2451965, 2451966, 2451972, 2451973,
      |                            2451979, 2451980, 2451986, 2451987, 2451993, 2451994, 2452000, 2452001, 2452007, 2452008, 2452014, 2452015,
      |                            2452021, 2452022, 2452028, 2452029, 2452035, 2452036, 2452042, 2452043, 2452049, 2452050, 2452056, 2452057,
      |                            2452063, 2452064, 2452070, 2452071, 2452077, 2452078, 2452084, 2452085, 2452091, 2452092, 2452098, 2452099,
      |                            2452105, 2452106, 2452112, 2452113, 2452119, 2452120, 2452126, 2452127, 2452133, 2452134, 2452140, 2452141,
      |                            2452147, 2452148, 2452154, 2452155, 2452161, 2452162, 2452168, 2452169, 2452175, 2452176, 2452182, 2452183,
      |                            2452189, 2452190, 2452196, 2452197, 2452203, 2452204, 2452210, 2452211, 2452217, 2452218, 2452224, 2452225,
      |                            2452231, 2452232, 2452238, 2452239, 2452245, 2452246, 2452252, 2452253, 2452259, 2452260, 2452266, 2452267,
      |                            2452273, 2452274)
      |      group by
      |    ss_ticket_number,
      |    ss_customer_sk,
      |    ss_addr_sk,
      |    ca_city
      |  ) dn
      |  join customer on (dn.ss_customer_sk = customer.c_customer_sk)
      |  join customer_address current_addr on (customer.c_current_addr_sk = current_addr.ca_address_sk)
      |where
      |  current_addr.ca_city <> bought_city
      |order by
      |  c_last_name,
      |  c_first_name,
      |  ca_city,
      |  bought_city,
      |  ss_ticket_number
      |limit 100
      |-- end query 1 in stream 0 using template query46.tpl
      """.stripMargin),

    Query("q52", """
      |-- start query 1 in stream 0 using template query52.tpl
      |select
      |  d_year,
      |  i_brand_id,
      |  i_brand,
      |  sum(ss_ext_sales_price) ext_price
      |from
      |  store_sales
      |  join item on (store_sales.ss_item_sk = item.i_item_sk)
      |  join date_dim dt on (store_sales.ss_sold_date_sk = dt.d_date_sk)
      |where
      |  i_manager_id = 1
      |  and d_moy = 12
      |  and d_year = 1998
      |  -- and ss_date between '1998-12-01' and '1998-12-31'
      |  and ss_sold_date_sk between 2451149 and 2451179 -- partition key filter
      |group by
      |  d_year,
      |  i_brand,
      |  i_brand_id
      |order by
      |  d_year,
      |  ext_price desc,
      |  i_brand_id
      |limit 100
      |-- end query 1 in stream 0 using template query52.tpl
      """.stripMargin),

    Query("q53", """
      |-- start query 1 in stream 0 using template query53.tpl
      |select
      |  *
      |from
      |  (select
      |    i_manufact_id,
      |    sum(ss_sales_price) sum_sales
      |    -- avg(sum(ss_sales_price)) over(partition by i_manufact_id) avg_quarterly_sales
      |  from
      |    store_sales
      |    join item on (store_sales.ss_item_sk = item.i_item_sk)
      |    join store on (store_sales.ss_store_sk = store.s_store_sk)
      |    join date_dim on (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
      |  where
      |    ss_sold_date_sk between 2451911 and 2452275 -- partition key filter
      |    -- ss_date between '2001-01-01' and '2001-12-31'
      |    and d_month_seq in(1212, 1212 + 1, 1212 + 2, 1212 + 3, 1212 + 4, 1212 + 5, 1212 + 6, 1212 + 7, 1212 + 8, 1212 + 9, 1212 + 10, 1212 + 11)
      |    and (
      |  	    	(i_category in('Books', 'Children', 'Electronics')
      |    		    and i_class in('personal', 'portable', 'reference', 'self-help')
      |    		    and i_brand in('scholaramalgamalg #14', 'scholaramalgamalg #7', 'exportiunivamalg #9', 'scholaramalgamalg #9')
      |  		    )
      |  		    or
      |  		    (i_category in('Women', 'Music', 'Men')
      |    		    and i_class in('accessories', 'classical', 'fragrances', 'pants')
      |    		    and i_brand in('amalgimporto #1', 'edu packscholar #1', 'exportiimporto #1', 'importoamalg #1')
      |  		    )
      |  	    )
      |  group by
      |    i_manufact_id,
      |    d_qoy
      |  ) tmp1
      |-- where
      |--   case when avg_quarterly_sales > 0 then abs(sum_sales - avg_quarterly_sales) / avg_quarterly_sales else null end > 0.1
      |order by
      |  -- avg_quarterly_sales,
      |  sum_sales,
      |  i_manufact_id
      |limit 100
      |-- end query 1 in stream 0 using template query53.tpl
      """.stripMargin),

    Query("q55", """
      |-- start query 1 in stream 0 using template query55.tpl
      |select
      |  i_brand_id,
      |  i_brand,
      |  sum(ss_ext_sales_price) ext_price
      |from
      |  store_sales
      |  join item on (store_sales.ss_item_sk = item.i_item_sk)
      |  join date_dim on (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
      |where
      |  i_manager_id = 36
      |  and d_moy = 12
      |  and d_year = 2001
      |  -- and ss_date between '2001-12-01' and '2001-12-31'
      |  and ss_sold_date_sk between 2452245 and 2452275 -- partition key filter
      |group by
      |  i_brand,
      |  i_brand_id
      |order by
      |  ext_price desc,
      |  i_brand_id
      |limit 100
      |-- end query 1 in stream 0 using template query55.tpl
      """.stripMargin),

    Query("q59", """
      |-- start query 1 in stream 0 using template query59.tpl
      |select
      |  s_store_name1,
      |  s_store_id1,
      |  d_week_seq1,
      |  sun_sales1 / sun_sales2,
      |  mon_sales1 / mon_sales2,
      |  tue_sales1 / tue_sales2,
      |  wed_sales1 / wed_sales2,
      |  thu_sales1 / thu_sales2,
      |  fri_sales1 / fri_sales2,
      |  sat_sales1 / sat_sales2
      |from
      |  (select
      |    s_store_name s_store_name1,
      |    wss.d_week_seq d_week_seq1,
      |    s_store_id s_store_id1,
      |    sun_sales sun_sales1,
      |    mon_sales mon_sales1,
      |    tue_sales tue_sales1,
      |    wed_sales wed_sales1,
      |    thu_sales thu_sales1,
      |    fri_sales fri_sales1,
      |    sat_sales sat_sales1
      |  from
      |    (select
      |      d_week_seq,
      |      ss_store_sk,
      |      sum(case when(d_day_name = 'Sunday') then ss_sales_price else null end) sun_sales,
      |      sum(case when(d_day_name = 'Monday') then ss_sales_price else null end) mon_sales,
      |      sum(case when(d_day_name = 'Tuesday') then ss_sales_price else null end) tue_sales,
      |      sum(case when(d_day_name = 'Wednesday') then ss_sales_price else null end) wed_sales,
      |      sum(case when(d_day_name = 'Thursday') then ss_sales_price else null end) thu_sales,
      |      sum(case when(d_day_name = 'Friday') then ss_sales_price else null end) fri_sales,
      |      sum(case when(d_day_name = 'Saturday') then ss_sales_price else null end) sat_sales
      |    from
      |      store_sales
      |      join date_dim on (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
      |    where
      |      -- ss_date between '1998-10-01' and '1999-09-30'
      |      ss_sold_date_sk between 2451088 and 2451452
      |    group by
      |      d_week_seq,
      |      ss_store_sk
      |    ) wss
      |    join store on (wss.ss_store_sk = store.s_store_sk)
      |    join date_dim d on (wss.d_week_seq = d.d_week_seq)
      |  where
      |    d_month_seq between 1185 and 1185 + 11
      |  ) y
      |  join
      |  (select
      |    s_store_name s_store_name2,
      |    wss.d_week_seq d_week_seq2,
      |    s_store_id s_store_id2,
      |    sun_sales sun_sales2,
      |    mon_sales mon_sales2,
      |    tue_sales tue_sales2,
      |    wed_sales wed_sales2,
      |    thu_sales thu_sales2,
      |    fri_sales fri_sales2,
      |    sat_sales sat_sales2
      |  from
      |    (select
      |      d_week_seq,
      |      ss_store_sk,
      |      sum(case when(d_day_name = 'Sunday') then ss_sales_price else null end) sun_sales,
      |      sum(case when(d_day_name = 'Monday') then ss_sales_price else null end) mon_sales,
      |      sum(case when(d_day_name = 'Tuesday') then ss_sales_price else null end) tue_sales,
      |      sum(case when(d_day_name = 'Wednesday') then ss_sales_price else null end) wed_sales,
      |      sum(case when(d_day_name = 'Thursday') then ss_sales_price else null end) thu_sales,
      |      sum(case when(d_day_name = 'Friday') then ss_sales_price else null end) fri_sales,
      |      sum(case when(d_day_name = 'Saturday') then ss_sales_price else null end) sat_sales
      |    from
      |      store_sales
      |      join date_dim on (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
      |    where
      |      -- ss_date between '1999-10-01' and '2000-09-30'
      |      ss_sold_date_sk between 2451088 and 2451452
      |    group by
      |      d_week_seq,
      |      ss_store_sk
      |    ) wss
      |    join store on (wss.ss_store_sk = store.s_store_sk)
      |    join date_dim d on (wss.d_week_seq = d.d_week_seq)
      |  where
      |    d_month_seq between 1185 + 12 and 1185 + 23
      |  ) x
      |  on (y.s_store_id1 = x.s_store_id2)
      |where
      |  d_week_seq1 = d_week_seq2 - 52
      |order by
      |  s_store_name1,
      |  s_store_id1,
      |  d_week_seq1
      |limit 100
      |-- end query 1 in stream 0 using template query59.tpl
      """.stripMargin),

    Query("q63", """
      |-- start query 1 in stream 0 using template query63.tpl
      |select
      |  *
      |from
      |  (select
      |    i_manager_id,
      |    sum(ss_sales_price) sum_sales
      |    -- avg(sum(ss_sales_price)) over(partition by i_manager_id) avg_monthly_sales
      |  from
      |    store_sales
      |    join item on (store_sales.ss_item_sk = item.i_item_sk)
      |    join store on (store_sales.ss_store_sk = store.s_store_sk)
      |    join date_dim on (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
      |  where
      |    ss_sold_date_sk between 2451911 and 2452275  -- partition key filter
      |    -- ss_date between '2001-01-01' and '2001-12-31'
      |    and d_month_seq in (1212, 1212 + 1, 1212 + 2, 1212 + 3, 1212 + 4, 1212 + 5, 1212 + 6, 1212 + 7, 1212 + 8, 1212 + 9, 1212 + 10, 1212 + 11)
      |    and (
      |          (i_category in('Books', 'Children', 'Electronics')
      |            and i_class in('personal', 'portable', 'refernece', 'self-help')
      |            and i_brand in('scholaramalgamalg #14', 'scholaramalgamalg #7', 'exportiunivamalg #9', 'scholaramalgamalg #9')
      |          )
      |          or
      |          (i_category in('Women', 'Music', 'Men')
      |            and i_class in('accessories', 'classical', 'fragrances', 'pants')
      |            and i_brand in('amalgimporto #1', 'edu packscholar #1', 'exportiimporto #1', 'importoamalg #1')
      |          )
      |        )
      |  group by
      |    i_manager_id,
      |    d_moy
      |  ) tmp1
      |-- where
      |--   case when avg_monthly_sales > 0 then abs(sum_sales - avg_monthly_sales) / avg_monthly_sales else null end > 0.1
      |order by
      |  i_manager_id,
      |  -- avg_monthly_sales,
      |  sum_sales
      |limit 100
      |-- end query 1 in stream 0 using template query63.tpl
      """.stripMargin),

    Query("q65", """
      |--q65
      |-- start query 1 in stream 0 using template query65.tpl
      |select
      |  s_store_name,
      |  i_item_desc,
      |  sc.revenue,
      |  i_current_price,
      |  i_wholesale_cost,
      |  i_brand
      |from
      |  (select
      |    ss_store_sk,
      |    ss_item_sk,
      |    sum(ss_sales_price) as revenue
      |  from
      |    store_sales
      |    join date_dim on (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
      |  where
      |    -- ss_date between '2001-01-01' and '2001-12-31'
      |    ss_sold_date_sk between 2451911 and 2452275  -- partition key filter
      |    and d_month_seq between 1212 and 1212 + 11
      |  group by
      |    ss_store_sk,
      |    ss_item_sk
      |  ) sc
      |  join item on (sc.ss_item_sk = item.i_item_sk)
      |  join store on (sc.ss_store_sk = store.s_store_sk)
      |  join
      |  (select
      |    ss_store_sk,
      |    avg(revenue) as ave
      |  from
      |    (select
      |      ss_store_sk,
      |      ss_item_sk,
      |      sum(ss_sales_price) as revenue
      |    from
      |      store_sales
      |      join date_dim on (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
      |    where
      |      -- ss_date between '2001-01-01' and '2001-12-31'
      |      ss_sold_date_sk between 2451911 and 2452275  -- partition key filter
      |      and d_month_seq between 1212 and 1212 + 11
      |    group by
      |      ss_store_sk,
      |      ss_item_sk
      |    ) sa
      |  group by
      |    ss_store_sk
      |  ) sb on (sc.ss_store_sk = sb.ss_store_sk) -- 676 rows
      |where
      |  sc.revenue <= 0.1 * sb.ave
      |order by
      |  s_store_name,
      |  i_item_desc
      |limit 100
      |-- end query 1 in stream 0 using template query65.tpl
      """.stripMargin),

    Query("q68", """
      |-- start query 1 in stream 0 using template query68.tpl
      |select
      |  c_last_name,
      |  c_first_name,
      |  ca_city,
      |  bought_city,
      |  ss_ticket_number,
      |  extended_price,
      |  extended_tax,
      |  list_price
      |from
      |  (select
      |    ss_ticket_number,
      |    ss_customer_sk,
      |    ca_city bought_city,
      |    sum(ss_ext_sales_price) extended_price,
      |    sum(ss_ext_list_price) list_price,
      |    sum(ss_ext_tax) extended_tax
      |  from
      |    store_sales
      |    join store on (store_sales.ss_store_sk = store.s_store_sk)
      |    join household_demographics on (store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk)
      |    join date_dim on (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
      |    join customer_address on (store_sales.ss_addr_sk = customer_address.ca_address_sk)
      |  where
      |    store.s_city in('Midway', 'Fairview')
      |    --and date_dim.d_dom between 1 and 2
      |    --and date_dim.d_year in(1999, 1999 + 1, 1999 + 2)
      |    -- and ss_date between '1999-01-01' and '2001-12-31'
      |    -- and dayofmonth(ss_date) in (1,2)
      |    -- and ss_sold_date_sk in (2451180, 2451181, 2451211, 2451212, 2451239, 2451240, 2451270, 2451271, 2451300, 2451301, 2451331,
      |    --                         2451332, 2451361, 2451362, 2451392, 2451393, 2451423, 2451424, 2451453, 2451454, 2451484, 2451485,
      |    --                         2451514, 2451515, 2451545, 2451546, 2451576, 2451577, 2451605, 2451606, 2451636, 2451637, 2451666,
      |    --                         2451667, 2451697, 2451698, 2451727, 2451728, 2451758, 2451759, 2451789, 2451790, 2451819, 2451820,
      |    --                         2451850, 2451851, 2451880, 2451881, 2451911, 2451912, 2451942, 2451943, 2451970, 2451971, 2452001,
      |    --                         2452002, 2452031, 2452032, 2452062, 2452063, 2452092, 2452093, 2452123, 2452124, 2452154, 2452155,
      |    --                         2452184, 2452185, 2452215, 2452216, 2452245, 2452246)
      |        and (household_demographics.hd_dep_count = 5
      |      or household_demographics.hd_vehicle_count = 3)
      |    and d_date between '1999-01-01' and '1999-03-31'
      |    and ss_sold_date_sk between 2451180 and 2451269 -- partition key filter (3 months)
      |  group by
      |    ss_ticket_number,
      |    ss_customer_sk,
      |    ss_addr_sk,
      |    ca_city
      |  ) dn
      |  join customer on (dn.ss_customer_sk = customer.c_customer_sk)
      |  join customer_address current_addr on (customer.c_current_addr_sk = current_addr.ca_address_sk)
      |where
      |  current_addr.ca_city <> bought_city
      |order by
      |  c_last_name,
      |  ss_ticket_number
      |limit 100
      |-- end query 1 in stream 0 using template query68.tpl
      """.stripMargin),

    Query("q7", """
      |-- start query 1 in stream 0 using template query7.tpl
      |select
      |  i_item_id,
      |  avg(ss_quantity) agg1,
      |  avg(ss_list_price) agg2,
      |  avg(ss_coupon_amt) agg3,
      |  avg(ss_sales_price) agg4
      |from
      |  store_sales
      |  join customer_demographics on (store_sales.ss_cdemo_sk = customer_demographics.cd_demo_sk)
      |  join item on (store_sales.ss_item_sk = item.i_item_sk)
      |  join promotion on (store_sales.ss_promo_sk = promotion.p_promo_sk)
      |  join date_dim on (ss_sold_date_sk = d_date_sk)
      |where
      |  cd_gender = 'F'
      |  and cd_marital_status = 'W'
      |  and cd_education_status = 'Primary'
      |  and (p_channel_email = 'N'
      |    or p_channel_event = 'N')
      |  and d_year = 1998
      |  -- and ss_date between '1998-01-01' and '1998-12-31'
      |  and ss_sold_date_sk between 2450815 and 2451179 -- partition key filter
      |group by
      |  i_item_id
      |order by
      |  i_item_id
      |limit 100
      |-- end query 1 in stream 0 using template query7.tpl
      """.stripMargin),

    Query("q73", """
      |-- start query 1 in stream 0 using template query73.tpl
      |select
      |  c_last_name,
      |  c_first_name,
      |  c_salutation,
      |  c_preferred_cust_flag,
      |  ss_ticket_number,
      |  cnt
      |from
      |  (select
      |    ss_ticket_number,
      |    ss_customer_sk,
      |    count(*) cnt
      |  from
      |    store_sales
      |    join household_demographics on (store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk)
      |    join store on (store_sales.ss_store_sk = store.s_store_sk)
      |    -- join date_dim on (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
      |  where
      |    store.s_county in ('Saginaw County', 'Sumner County', 'Appanoose County', 'Daviess County')
      |    -- and date_dim.d_dom between 1 and 2
      |    -- and date_dim.d_year in(1998, 1998 + 1, 1998 + 2)
      |    -- and ss_date between '1999-01-01' and '2001-12-02'
      |    -- and dayofmonth(ss_date) in (1,2)
      |    -- partition key filter
      |    -- and ss_sold_date_sk in (2450816, 2450846, 2450847, 2450874, 2450875, 2450905, 2450906, 2450935, 2450936, 2450966, 2450967,
      |    --                         2450996, 2450997, 2451027, 2451028, 2451058, 2451059, 2451088, 2451089, 2451119, 2451120, 2451149,
      |    --                         2451150, 2451180, 2451181, 2451211, 2451212, 2451239, 2451240, 2451270, 2451271, 2451300, 2451301,
      |    --                         2451331, 2451332, 2451361, 2451362, 2451392, 2451393, 2451423, 2451424, 2451453, 2451454, 2451484,
      |    --                         2451485, 2451514, 2451515, 2451545, 2451546, 2451576, 2451577, 2451605, 2451606, 2451636, 2451637,
      |    --                         2451666, 2451667, 2451697, 2451698, 2451727, 2451728, 2451758, 2451759, 2451789, 2451790, 2451819,
      |    --                         2451820, 2451850, 2451851, 2451880, 2451881)
      |    and (household_demographics.hd_buy_potential = '>10000'
      |      or household_demographics.hd_buy_potential = 'unknown')
      |    and household_demographics.hd_vehicle_count > 0
      |    and case when household_demographics.hd_vehicle_count > 0 then household_demographics.hd_dep_count / household_demographics.hd_vehicle_count else null end > 1
      |    and ss_sold_date_sk between 2451180 and 2451269 -- partition key filter (3 months)
      |  group by
      |    ss_ticket_number,
      |    ss_customer_sk
      |  ) dj
      |  join customer on (dj.ss_customer_sk = customer.c_customer_sk)
      |where
      |  cnt between 1 and 5
      |order by
      |  cnt desc
      |limit 1000
      |-- end query 1 in stream 0 using template query73.tpl
      """.stripMargin),

    Query("q79", """
      |-- start query 1 in stream 0 using template query79.tpl
      |select
      |  c_last_name,
      |  c_first_name,
      |  substr(s_city, 1, 30) as city,
      |  ss_ticket_number,
      |  amt,
      |  profit
      |from
      |  (select
      |    ss_ticket_number,
      |    ss_customer_sk,
      |    s_city,
      |    sum(ss_coupon_amt) amt,
      |    sum(ss_net_profit) profit
      |  from
      |    store_sales
      |    join household_demographics on (store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk)
      |    join date_dim on (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
      |    join store on (store_sales.ss_store_sk = store.s_store_sk)
      |  where
      |    store.s_number_employees between 200 and 295
      |    and (household_demographics.hd_dep_count = 8
      |      or household_demographics.hd_vehicle_count > 0)
      |    and date_dim.d_dow = 1
      |    and date_dim.d_year in (1998, 1998 + 1, 1998 + 2)
      |    -- and ss_date between '1998-01-01' and '2000-12-25'
      |    -- 156 days
      |  and d_date between '1999-01-01' and '1999-03-31'
      |  and ss_sold_date_sk between 2451180 and 2451269  -- partition key filter
      |  group by
      |    ss_ticket_number,
      |    ss_customer_sk,
      |    ss_addr_sk,
      |    s_city
      |  ) ms
      |  join customer on (ms.ss_customer_sk = customer.c_customer_sk)
      |order by
      |  c_last_name,
      |  c_first_name,
      |  -- substr(s_city, 1, 30),
      |  city,
      |  profit
      |limit 100
      |-- end query 1 in stream 0 using template query79.tpl
      """.stripMargin),

    /*
    Query("q8", """
      |-- start query 1 in stream 0 using template query8.tpl
      |select
      |  s_store_name,
      |  sum(ss_net_profit)
      |from
      |  store_sales
      |  join store on (store_sales.ss_store_sk = store.s_store_sk)
      |  -- join date_dim on (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
      |  join
      |  (select
      |    a.ca_zip
      |  from
      |    (select
      |      substr(ca_zip, 1, 5) ca_zip,
      |      count( *) cnt
      |    from
      |      customer_address
      |      join customer on (customer_address.ca_address_sk = customer.c_current_addr_sk)
      |    where
      |      c_preferred_cust_flag = 'Y'
      |    group by
      |      ca_zip
      |    having
      |      count( *) > 10
      |    ) a
      |    left semi join
      |    (select
      |      substr(ca_zip, 1, 5) ca_zip
      |    from
      |      customer_address
      |    where
      |      substr(ca_zip, 1, 5) in ('89436', '30868', '65085', '22977', '83927', '77557', '58429', '40697', '80614', '10502', '32779',
      |      '91137', '61265', '98294', '17921', '18427', '21203', '59362', '87291', '84093', '21505', '17184', '10866', '67898', '25797',
      |      '28055', '18377', '80332', '74535', '21757', '29742', '90885', '29898', '17819', '40811', '25990', '47513', '89531', '91068',
      |      '10391', '18846', '99223', '82637', '41368', '83658', '86199', '81625', '26696', '89338', '88425', '32200', '81427', '19053',
      |      '77471', '36610', '99823', '43276', '41249', '48584', '83550', '82276', '18842', '78890', '14090', '38123', '40936', '34425',
      |      '19850', '43286', '80072', '79188', '54191', '11395', '50497', '84861', '90733', '21068', '57666', '37119', '25004', '57835',
      |      '70067', '62878', '95806', '19303', '18840', '19124', '29785', '16737', '16022', '49613', '89977', '68310', '60069', '98360',
      |      '48649', '39050', '41793', '25002', '27413', '39736', '47208', '16515', '94808', '57648', '15009', '80015', '42961', '63982',
      |      '21744', '71853', '81087', '67468', '34175', '64008', '20261', '11201', '51799', '48043', '45645', '61163', '48375', '36447',
      |      '57042', '21218', '41100', '89951', '22745', '35851', '83326', '61125', '78298', '80752', '49858', '52940', '96976', '63792',
      |      '11376', '53582', '18717', '90226', '50530', '94203', '99447', '27670', '96577', '57856', '56372', '16165', '23427', '54561',
      |      '28806', '44439', '22926', '30123', '61451', '92397', '56979', '92309', '70873', '13355', '21801', '46346', '37562', '56458',
      |      '28286', '47306', '99555', '69399', '26234', '47546', '49661', '88601', '35943', '39936', '25632', '24611', '44166', '56648',
      |      '30379', '59785', '11110', '14329', '93815', '52226', '71381', '13842', '25612', '63294', '14664', '21077', '82626', '18799',
      |      '60915', '81020', '56447', '76619', '11433', '13414', '42548', '92713', '70467', '30884', '47484', '16072', '38936', '13036',
      |      '88376', '45539', '35901', '19506', '65690', '73957', '71850', '49231', '14276', '20005', '18384', '76615', '11635', '38177',
      |      '55607', '41369', '95447', '58581', '58149', '91946', '33790', '76232', '75692', '95464', '22246', '51061', '56692', '53121',
      |      '77209', '15482', '10688', '14868', '45907', '73520', '72666', '25734', '17959', '24677', '66446', '94627', '53535', '15560',
      |      '41967', '69297', '11929', '59403', '33283', '52232', '57350', '43933', '40921', '36635', '10827', '71286', '19736', '80619',
      |      '25251', '95042', '15526', '36496', '55854', '49124', '81980', '35375', '49157', '63512', '28944', '14946', '36503', '54010',
      |      '18767', '23969', '43905', '66979', '33113', '21286', '58471', '59080', '13395', '79144', '70373', '67031', '38360', '26705',
      |      '50906', '52406', '26066', '73146', '15884', '31897', '30045', '61068', '45550', '92454', '13376', '14354', '19770', '22928',
      |      '97790', '50723', '46081', '30202', '14410', '20223', '88500', '67298', '13261', '14172', '81410', '93578', '83583', '46047',
      |      '94167', '82564', '21156', '15799', '86709', '37931', '74703', '83103', '23054', '70470', '72008', '49247', '91911', '69998',
      |      '20961', '70070', '63197', '54853', '88191', '91830', '49521', '19454', '81450', '89091', '62378', '25683', '61869', '51744',
      |      '36580', '85778', '36871', '48121', '28810', '83712', '45486', '67393', '26935', '42393', '20132', '55349', '86057', '21309',
      |      '80218', '10094', '11357', '48819', '39734', '40758', '30432', '21204', '29467', '30214', '61024', '55307', '74621', '11622',
      |      '68908', '33032', '52868', '99194', '99900', '84936', '69036', '99149', '45013', '32895', '59004', '32322', '14933', '32936',
      |      '33562', '72550', '27385', '58049', '58200', '16808', '21360', '32961', '18586', '79307', '15492')
      |    ) b
      |  on (a.ca_zip = b.ca_zip)
      |  ) v1 on (substr(store.s_zip, 1, 2) = substr(v1.ca_zip, 1, 2))
      |where
      |  ss_date between '2002-01-01' and '2002-04-01'
      |  -- and d_qoy = 1
      |  -- and d_year = 2002
      |group by
      |  s_store_name
      |order by
      |  s_store_name
      |limit 100
      |-- end query 1 in stream 0 using template query8.tpl
      """.stripMargin),
      */

    Query("q82", """
      |-- start query 1 in stream 0 using template query82.tpl
      |select
      |  i_item_id,
      |  i_item_desc,
      |  i_current_price
      |from
      |  store_sales
      |  join item on (store_sales.ss_item_sk = item.i_item_sk)
      |  join inventory on (item.i_item_sk = inventory.inv_item_sk)
      |  -- join date_dim on (inventory.inv_date_sk = date_dim.d_date_sk)
      |where
      |  i_current_price between 30 and 30 + 30
      |  and i_manufact_id in (437, 129, 727, 663)
      |  and inv_quantity_on_hand between 100 and 500
      |  and inv_date between '2002-05-30' and '2002-07-29'
      |  -- and d_date between cast('2002-05-30' as date) and (cast('2002-05-30' as date) + 60)
      |group by
      |  i_item_id,
      |  i_item_desc,
      |  i_current_price
      |order by
      |  i_item_id
      |limit 100
      |-- end query 1 in stream 0 using template query82.tpl
      """.stripMargin),

    Query("q89", """
      |-- start query 1 in stream 0 using template query89.tpl
      |select
      |  *
      |from
      |  (select
      |    i_category,
      |    i_class,
      |    i_brand,
      |    s_store_name,
      |    s_company_name,
      |    d_moy,
      |    sum(ss_sales_price) sum_sales
      |    -- avg(sum(ss_sales_price)) over (partition by i_category, i_brand, s_store_name, s_company_name) avg_monthly_sales
      |  from
      |    store_sales
      |    join item on (store_sales.ss_item_sk = item.i_item_sk)
      |    join store on (store_sales.ss_store_sk = store.s_store_sk)
      |    join date_dim on (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
      |  where
      |    -- ss_date between '2000-01-01' and '2000-12-31'
      |    ss_sold_date_sk between 2451545 and 2451910  -- partition key filter
      |    and d_year in (2000)
      |    and ((i_category in('Home', 'Books', 'Electronics')
      |          and i_class in('wallpaper', 'parenting', 'musical'))
      |        or (i_category in('Shoes', 'Jewelry', 'Men')
      |            and i_class in('womens', 'birdal', 'pants'))
      |        )
      |  group by
      |    i_category,
      |    i_class,
      |    i_brand,
      |    s_store_name,
      |    s_company_name,
      |    d_moy
      |  ) tmp1
      |-- where
      |--   case when(avg_monthly_sales <> 0) then(abs(sum_sales - avg_monthly_sales) / avg_monthly_sales) else null end > 0.1
      |order by
      |  -- sum_sales - avg_monthly_sales,
      |  sum_sales,
      |  s_store_name
      |limit 100
      |-- end query 1 in stream 0 using template query89.tpl
      """.stripMargin),

    Query("q98", """
      |-- start query 1 in stream 0 using template query98.tpl
      |select
      |  i_item_desc,
      |  i_category,
      |  i_class,
      |  i_current_price,
      |  sum(ss_ext_sales_price) as itemrevenue
      |  -- sum(ss_ext_sales_price) * 100 / sum(sum(ss_ext_sales_price)) over (partition by i_class) as revenueratio
      |from
      |  store_sales
      |  join item on (store_sales.ss_item_sk = item.i_item_sk)
      |  join date_dim on (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
      |where
      |  i_category in('Jewelry', 'Sports', 'Books')
      |  -- and d_date between cast('2001-01-12' as date) and (cast('2001-01-12' as date) + 30)
      |  -- and d_date between '2001-01-12' and '2001-02-11'
      |  -- and ss_date between '2001-01-12' and '2001-02-11'
      |  -- and ss_sold_date_sk between 2451922 and 2451952  -- partition key filter
      |  and ss_sold_date_sk between 2451911 and 2451941  -- partition key filter (1 calendar month)
      |  and d_date between '2001-01-01' and '2001-01-31'
      |group by
      |  i_item_id,
      |  i_item_desc,
      |  i_category,
      |  i_class,
      |  i_current_price
      |order by
      |  i_category,
      |  i_class,
      |  i_item_id,
      |  i_item_desc
      |  -- revenueratio
      |limit 1000
      |-- end query 1 in stream 0 using template query98.tpl
      """.stripMargin),

    Query("ss_max", """
      |select
      |  count(*) as total,
      |  count(ss_sold_date_sk) as not_null_total,
      |  count(distinct ss_sold_date_sk) as unique_days,
      |  max(ss_sold_date_sk) as max_ss_sold_date_sk,
      |  max(ss_sold_time_sk) as max_ss_sold_time_sk,
      |  max(ss_item_sk) as max_ss_item_sk,
      |  max(ss_customer_sk) as max_ss_customer_sk,
      |  max(ss_cdemo_sk) as max_ss_cdemo_sk,
      |  max(ss_hdemo_sk) as max_ss_hdemo_sk,
      |  max(ss_addr_sk) as max_ss_addr_sk,
      |  max(ss_store_sk) as max_ss_store_sk,
      |  max(ss_promo_sk) as max_ss_promo_sk
      |from store_sales
      """.stripMargin),

    Query("ss_max_presto", """
      |select
      |  count(*) as total,
      |  count(ss_sold_date_sk) as not_null_total,
      |  approx_distinct(ss_sold_date_sk) as unique_days,
      |  max(ss_sold_date_sk) as max_ss_sold_date_sk,
      |  max(ss_sold_time_sk) as max_ss_sold_time_sk,
      |  max(ss_item_sk) as max_ss_item_sk,
      |  max(ss_customer_sk) as max_ss_customer_sk,
      |  max(ss_cdemo_sk) as max_ss_cdemo_sk,
      |  max(ss_hdemo_sk) as max_ss_hdemo_sk,
      |  max(ss_addr_sk) as max_ss_addr_sk,
      |  max(ss_store_sk) as max_ss_store_sk,
      |  max(ss_promo_sk) as max_ss_promo_sk
      |from store_sales
      """.stripMargin)
  )

  val originalQueries = Seq(
    Query("q3", """
      select  d_year
      ,item.i_brand_id brand_id
      ,item.i_brand brand
      ,sum(ss_ext_sales_price) sum_agg
        from  date_dim dt
        JOIN store_sales on dt.d_date_sk = store_sales.ss_sold_date_sk
        JOIN item on store_sales.ss_item_sk = item.i_item_sk
        where
        item.i_manufact_id = 436
        and dt.d_moy=12
        group by d_year
      ,item.i_brand
      ,item.i_brand_id
        order by d_year
      ,sum_agg desc
      ,brand_id
        limit 100"""),

    Query("q7", """
      select  i_item_id,
      avg(ss_quantity) agg1,
      avg(ss_list_price) agg2,
      avg(ss_coupon_amt) agg3,
      avg(ss_sales_price) agg4
        from store_sales
        JOIN customer_demographics ON store_sales.ss_cdemo_sk = customer_demographics.cd_demo_sk
    JOIN date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
    JOIN item ON store_sales.ss_item_sk = item.i_item_sk
    JOIN promotion ON store_sales.ss_promo_sk = promotion.p_promo_sk
    where
    cd_gender = 'F' and
      cd_marital_status = 'W' and
      cd_education_status = 'Primary' and
    (p_channel_email = 'N' or p_channel_event = 'N') and
      d_year = 1998
    group by i_item_id
    order by i_item_id
    limit 100"""),

    Query("q19", """
      select  i_brand_id, i_brand, i_manufact_id, i_manufact,
      sum(ss_ext_sales_price) as ext_price
        from date_dim
        JOIN store_sales ON date_dim.d_date_sk = store_sales.ss_sold_date_sk
        JOIN item ON store_sales.ss_item_sk = item.i_item_sk
        JOIN customer ON store_sales.ss_customer_sk = customer.c_customer_sk
        JOIN customer_address ON customer.c_current_addr_sk = customer_address.ca_address_sk
        JOIN store ON store_sales.ss_store_sk = store.s_store_sk
        where
        i_manager_id=7
        and d_moy=11
        and d_year=1999
        and substr(ca_zip,1,5) <> substr(s_zip,1,5)
        group by i_brand
      ,i_brand_id
      ,i_manufact_id
      ,i_manufact
        order by ext_price desc
      ,i_brand
      ,i_brand_id
      ,i_manufact_id
      ,i_manufact
        limit 100"""),

  /*  WHERE IS ss_sold_date?
    Query("q27partitioned", """
      select  i_item_id,
      s_state,
      avg(ss_quantity) agg1,
      avg(ss_list_price) agg2,
      avg(ss_coupon_amt) agg3,
      avg(ss_sales_price) agg4
        from store_sales
        JOIN customer_demographics ON store_sales.ss_cdemo_sk = customer_demographics.cd_demo_sk
    JOIN date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
    JOIN store ON store_sales.ss_store_sk = store.s_store_sk
    JOIN item ON store_sales.ss_item_sk = item.i_item_sk
    where
    cd_gender = 'F' and
      cd_marital_status = 'W' and
      cd_education_status = 'Primary' and
    d_year = 1998 and
      s_state = 'TN' and
    ss_sold_date between '1998-01-01' and '1998-12-31'
    group by i_item_id, s_state
    order by i_item_id
    ,s_state
    limit 100"""),    */

    Query("q27", """
      select  i_item_id,
      s_state,
      avg(ss_quantity) agg1,
      avg(ss_list_price) agg2,
      avg(ss_coupon_amt) agg3,
      avg(ss_sales_price) agg4
        from store_sales
        JOIN customer_demographics ON store_sales.ss_cdemo_sk = customer_demographics.cd_demo_sk
    JOIN date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
    JOIN store ON store_sales.ss_store_sk = store.s_store_sk
    JOIN item ON store_sales.ss_item_sk = item.i_item_sk
    where
    cd_gender = 'F' and
      cd_marital_status = 'W' and
      cd_education_status = 'Primary' and
    d_year = 1998 and
      s_state = 'TN'
    group by i_item_id, s_state
    order by i_item_id
    ,s_state
    limit 100"""),

    Query("q34", """
      select c_last_name
      ,c_first_name
      ,c_salutation
      ,c_preferred_cust_flag
      ,ss_ticket_number
      ,cnt from
        (select ss_ticket_number
          ,ss_customer_sk
          ,count(*) cnt
          from store_sales
          JOIN date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
        JOIN store ON store_sales.ss_store_sk = store.s_store_sk
        JOIN household_demographics ON store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
        where
        (date_dim.d_dom between 1 and 3 or date_dim.d_dom between 25 and 28)
        and (household_demographics.hd_buy_potential = '>10000' or
    household_demographics.hd_buy_potential = 'unknown')
    and household_demographics.hd_vehicle_count > 0
    and (case when household_demographics.hd_vehicle_count > 0
    then household_demographics.hd_dep_count/ household_demographics.hd_vehicle_count
    else null
    end)  > 1.2
    and date_dim.d_year in (1998,1998+1,1998+2)
    and store.s_county in ('Williamson County','Williamson County','Williamson County','Williamson County',
    'Williamson County','Williamson County','Williamson County','Williamson County')
    group by ss_ticket_number,ss_customer_sk) dn
    JOIN customer ON dn.ss_customer_sk = customer.c_customer_sk
    WHERE
    cnt between 15 and 20
    order by c_last_name,c_first_name,c_salutation,c_preferred_cust_flag desc"""),

    Query("q42", """
      select  d_year
      ,item.i_category_id
      ,item.i_category
      ,sum(ss_ext_sales_price) as s
        from 	date_dim dt
        JOIN store_sales ON dt.d_date_sk = store_sales.ss_sold_date_sk
    JOIN item ON store_sales.ss_item_sk = item.i_item_sk
    where
    item.i_manager_id = 1
    and dt.d_moy=12
    and dt.d_year=1998
    group by 	d_year
    ,item.i_category_id
    ,item.i_category
    order by       s desc,d_year
    ,i_category_id
    ,i_category
    limit 100"""),

    Query("q43", """
      select  s_store_name, s_store_id,
      sum(case when (d_day_name='Sunday') then ss_sales_price else null end) sun_sales,
    sum(case when (d_day_name='Monday') then ss_sales_price else null end) mon_sales,
    sum(case when (d_day_name='Tuesday') then ss_sales_price else  null end) tue_sales,
    sum(case when (d_day_name='Wednesday') then ss_sales_price else null end) wed_sales,
    sum(case when (d_day_name='Thursday') then ss_sales_price else null end) thu_sales,
    sum(case when (d_day_name='Friday') then ss_sales_price else null end) fri_sales,
    sum(case when (d_day_name='Saturday') then ss_sales_price else null end) sat_sales
    from date_dim
      JOIN store_sales ON date_dim.d_date_sk = store_sales.ss_sold_date_sk
    JOIN store ON store.s_store_sk = store_sales.ss_store_sk
    where
    s_gmt_offset = -5 and
      d_year = 1998
    group by s_store_name, s_store_id
    order by s_store_name, s_store_id,sun_sales,mon_sales,tue_sales,wed_sales,thu_sales,fri_sales,sat_sales
    limit 100"""),

    Query("q46", """
      select  c_last_name
      ,c_first_name
      ,ca_city
      ,bought_city
      ,ss_ticket_number
      ,amt,profit
        from
        (select ss_ticket_number
          ,ss_customer_sk
          ,ca_city as bought_city
          ,sum(ss_coupon_amt) as amt
          ,sum(ss_net_profit) as profit
          from store_sales
          JOIN date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
          JOIN store ON store_sales.ss_store_sk = store.s_store_sk
          JOIN household_demographics ON store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
          JOIN customer_address ON store_sales.ss_addr_sk = customer_address.ca_address_sk
          where
          (household_demographics.hd_dep_count = 5 or
            household_demographics.hd_vehicle_count= 3)
          and date_dim.d_dow in (6,0)
          and date_dim.d_year in (1999,1999+1,1999+2)
          and store.s_city in ('Midway','Fairview','Fairview','Fairview','Fairview')
    group by ss_ticket_number,ss_customer_sk,ss_addr_sk,ca_city) dn
    JOIN customer ON dn.ss_customer_sk = customer.c_customer_sk
    JOIN customer_address ON customer.c_current_addr_sk = customer_address.ca_address_sk
    where
    customer_address.ca_city <> dn.bought_city
    order by c_last_name
    ,c_first_name
    ,ca_city
    ,bought_city
    ,ss_ticket_number
    limit 100"""),

    Query("q52", """
      select  d_year
      ,item.i_brand_id brand_id
      ,item.i_brand brand
      ,sum(ss_ext_sales_price) as ext_price
        from date_dim
        JOIN store_sales ON date_dim.d_date_sk = store_sales.ss_sold_date_sk
        JOIN item ON store_sales.ss_item_sk = item.i_item_sk
        where
        item.i_manager_id = 1
        and date_dim.d_moy=12
        and date_dim.d_year=1998
        group by d_year
      ,item.i_brand
      ,item.i_brand_id
        order by d_year
      ,ext_price desc
      ,brand_id
        limit 100"""),

    Query("q55", """
      select  i_brand_id as brand_id, i_brand as brand,
      sum(store_sales.ss_ext_sales_price) ext_price
        from date_dim
        JOIN store_sales ON date_dim.d_date_sk = store_sales.ss_sold_date_sk
    JOIN item ON store_sales.ss_item_sk = item.i_item_sk
    where
    i_manager_id=36
    and d_moy=12
    and d_year=2001
    group by i_brand, i_brand_id
    order by ext_price desc, brand_id
    limit 100 """),

    Query("q59",
      """
        |select
        |  s_store_name1,
        |  s_store_id1,
        |  d_week_seq1,
        |  sun_sales1 / sun_sales2,
        |  mon_sales1 / mon_sales2,
        |  tue_sales1 / tue_sales2,
        |  wed_sales1 / wed_sales2,
        |  thu_sales1 / thu_sales2,
        |  fri_sales1 / fri_sales2,
        |  sat_sales1 / sat_sales2
        |from
        |  (select
        |    /*+ MAPJOIN(store, date_dim) */
        |    s_store_name s_store_name1,
        |    wss.d_week_seq d_week_seq1,
        |    s_store_id s_store_id1,
        |    sun_sales sun_sales1,
        |    mon_sales mon_sales1,
        |    tue_sales tue_sales1,
        |    wed_sales wed_sales1,
        |    thu_sales thu_sales1,
        |    fri_sales fri_sales1,
        |    sat_sales sat_sales1
        |  from
        |    (select
        |      /*+ MAPJOIN(date_dim) */
        |      d_week_seq,
        |      ss_store_sk,
        |      sum(case when(d_day_name = 'Sunday') then ss_sales_price else null end) sun_sales,
        |      sum(case when(d_day_name = 'Monday') then ss_sales_price else null end) mon_sales,
        |      sum(case when(d_day_name = 'Tuesday') then ss_sales_price else null end) tue_sales,
        |      sum(case when(d_day_name = 'Wednesday') then ss_sales_price else null end) wed_sales,
        |      sum(case when(d_day_name = 'Thursday') then ss_sales_price else null end) thu_sales,
        |      sum(case when(d_day_name = 'Friday') then ss_sales_price else null end) fri_sales,
        |      sum(case when(d_day_name = 'Saturday') then ss_sales_price else null end) sat_sales
        |    from
        |      store_sales
        |      join date_dim on (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
        |    where
        |      -- ss_date between '1998-10-01' and '1999-09-30'
        |      ss_sold_date_sk between 2451088 and 2451452
        |    group by
        |      d_week_seq,
        |      ss_store_sk
        |    ) wss
        |    join store on (wss.ss_store_sk = store.s_store_sk)
        |    join date_dim d on (wss.d_week_seq = d.d_week_seq)
        |  where
        |    d_month_seq between 1185 and 1185 + 11
        |  ) y
        |  join
        |  (select
        |    /*+ MAPJOIN(store, date_dim) */
        |    s_store_name s_store_name2,
        |    wss.d_week_seq d_week_seq2,
        |    s_store_id s_store_id2,
        |    sun_sales sun_sales2,
        |    mon_sales mon_sales2,
        |    tue_sales tue_sales2,
        |    wed_sales wed_sales2,
        |    thu_sales thu_sales2,
        |    fri_sales fri_sales2,
        |    sat_sales sat_sales2
        |  from
        |    (select
        |      /*+ MAPJOIN(date_dim) */
        |      d_week_seq,
        |      ss_store_sk,
        |      sum(case when(d_day_name = 'Sunday') then ss_sales_price else null end) sun_sales,
        |      sum(case when(d_day_name = 'Monday') then ss_sales_price else null end) mon_sales,
        |      sum(case when(d_day_name = 'Tuesday') then ss_sales_price else null end) tue_sales,
        |      sum(case when(d_day_name = 'Wednesday') then ss_sales_price else null end) wed_sales,
        |      sum(case when(d_day_name = 'Thursday') then ss_sales_price else null end) thu_sales,
        |      sum(case when(d_day_name = 'Friday') then ss_sales_price else null end) fri_sales,
        |      sum(case when(d_day_name = 'Saturday') then ss_sales_price else null end) sat_sales
        |    from
        |      store_sales
        |      join date_dim on (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
        |    where
        |      -- ss_date between '1999-10-01' and '2000-09-30'
        |      ss_sold_date_sk between 2451088 and 2451452
        |    group by
        |      d_week_seq,
        |      ss_store_sk
        |    ) wss
        |    join store on (wss.ss_store_sk = store.s_store_sk)
        |    join date_dim d on (wss.d_week_seq = d.d_week_seq)
        |  where
        |    d_month_seq between 1185 + 12 and 1185 + 23
        |  ) x
        |  on (y.s_store_id1 = x.s_store_id2)
        |where
        |  d_week_seq1 = d_week_seq2 - 52
        |order by
        |  s_store_name1,
        |  s_store_id1,
        |  d_week_seq1
        |limit 100
      """.stripMargin),

    Query("q68", """
      select  c_last_name ,c_first_name ,ca_city
      ,bought_city ,ss_ticket_number ,extended_price
      ,extended_tax ,list_price
        from (select ss_ticket_number
        ,ss_customer_sk
        ,ca_city as bought_city
        ,sum(ss_ext_sales_price) as extended_price
        ,sum(ss_ext_list_price) as list_price
        ,sum(ss_ext_tax) as extended_tax
        from store_sales
        JOIN date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
        JOIN store ON store_sales.ss_store_sk = store.s_store_sk
        JOIN household_demographics ON store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
        JOIN customer_address ON store_sales.ss_addr_sk = customer_address.ca_address_sk
        where
        date_dim.d_dom between 1 and 2
        and (household_demographics.hd_dep_count = 5 or
        household_demographics.hd_vehicle_count= 3)
        and date_dim.d_year in (1999,1999+1,1999+2)
        and store.s_city in ('Midway','Fairview')
    group by ss_ticket_number
    ,ss_customer_sk
    ,ss_addr_sk,ca_city) dn
    JOIN customer ON dn.ss_customer_sk = customer.c_customer_sk
    JOIN customer_address ON customer.c_current_addr_sk = customer_address.ca_address_sk
    where
    customer_address.ca_city <> dn.bought_city
    order by c_last_name
    ,ss_ticket_number
    limit 100"""),

    Query("q73", """
      select c_last_name
      ,c_first_name
      ,c_salutation
      ,c_preferred_cust_flag
      ,ss_ticket_number
      ,cnt from
        (select ss_ticket_number
          ,ss_customer_sk
          ,count(*) cnt
          from store_sales
          JOIN date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
        JOIN store ON store_sales.ss_store_sk = store.s_store_sk
        JOIN household_demographics ON store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
        where
        date_dim.d_dom between 1 and 2
        and (household_demographics.hd_buy_potential = '>10000' or
    household_demographics.hd_buy_potential = 'unknown')
    and household_demographics.hd_vehicle_count > 0
    and case when household_demographics.hd_vehicle_count > 0 then
      household_demographics.hd_dep_count/ household_demographics.hd_vehicle_count else null end > 1
    and date_dim.d_year in (1998,1998+1,1998+2)
    and store.s_county in ('Williamson County','Williamson County','Williamson County','Williamson County')
    group by ss_ticket_number,ss_customer_sk) dj
    JOIN customer ON dj.ss_customer_sk = customer.c_customer_sk
    where
    cnt between 5 and 10
    order by cnt desc"""),

    Query("q79", """
      select
      c_last_name,c_first_name,substr(s_city,1,30) as s_city,ss_ticket_number,amt,profit
      from
      (select ss_ticket_number
        ,ss_customer_sk
        ,store.s_city
        ,sum(ss_coupon_amt) amt
        ,sum(ss_net_profit) profit
        from store_sales
        JOIN date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
      JOIN store ON store_sales.ss_store_sk = store.s_store_sk
      JOIN household_demographics ON store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
      where
      (household_demographics.hd_dep_count = 8 or household_demographics.hd_vehicle_count > 0)
      and date_dim.d_dow = 1
      and date_dim.d_year in (1998,1998+1,1998+2)
      and store.s_number_employees between 200 and 295
      group by ss_ticket_number,ss_customer_sk,ss_addr_sk,store.s_city) ms
      JOIN customer on ms.ss_customer_sk = customer.c_customer_sk
    order by c_last_name,c_first_name,s_city, profit
    limit 100"""),

    Query("qSsMax",
      """
        |select
        |  count(*) as total,
        |  count(ss_sold_date_sk) as not_null_total,
        |  count(distinct ss_sold_date_sk) as unique_days,
        |  max(ss_sold_date_sk) as max_ss_sold_date_sk,
        |  max(ss_sold_time_sk) as max_ss_sold_time_sk,
        |  max(ss_item_sk) as max_ss_item_sk,
        |  max(ss_customer_sk) as max_ss_customer_sk,
        |  max(ss_cdemo_sk) as max_ss_cdemo_sk,
        |  max(ss_hdemo_sk) as max_ss_hdemo_sk,
        |  max(ss_addr_sk) as max_ss_addr_sk,
        |  max(ss_store_sk) as max_ss_store_sk,
        |  max(ss_promo_sk) as max_ss_promo_sk
        |from store_sales
      """.stripMargin)
  )

  val query = queries.map(q => (q.name, q)).toMap
}
// scalatyle:off
