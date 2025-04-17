/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.spark

import com.alibaba.fluss.client.admin.Admin
import com.alibaba.fluss.config.ConfigOptions
import com.alibaba.fluss.metadata.TablePath
import com.alibaba.fluss.row.InternalRow
import com.alibaba.fluss.spark.FlussSparkTestBase.{FLUSS_CLUSTER_EXTENSION, row, rowWithPartition}
import com.alibaba.fluss.testutils.common.CommonTestUtils.waitUtil
import org.apache.spark.sql.catalyst.dsl.expressions.{DslAttr}
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.streaming.{StreamTest, StreamingQuery, StreamingQueryException, Trigger}
import org.junit.jupiter.api.Assertions

import java.{util => ju}
import java.time.Duration
class SparkTableSourceITCase extends FlussSparkTestBase with StreamTest {

  private var bootstrapServers: String = _

  test("Fluss Source: test NonPkTable Read") {
    withTempDir {
      checkpointDir =>
        bootstrapServers = String.join(",", flussConf.get(ConfigOptions.BOOTSTRAP_SERVERS))
        spark.sql("create table non_pk_table_test (a int, b String) using fluss")
        val tablePath: TablePath = TablePath.of(DEFAULT_DB, "non_pk_table_test")

        val rows: ju.List[InternalRow] = ju.Arrays.asList(row(1, "v1"), row(2, "v2"), row(3, "v3"))

        // write records
        writeRows(tablePath, rows, true)

        val reader = spark.readStream
          .format("fluss")
          .option("bootstrap.servers", bootstrapServers)
          .option("maxOffsetsPerTrigger", 5)
          .option("database", DEFAULT_DB)
          .option("table", "non_pk_table_test")
          .load()
        val query: StreamingQuery = reader.writeStream
          .format("memory")
          .queryName("non_pk_table_test")
          .outputMode("append")
          .start()
        try {
          query.processAllAvailable()
          val df: Dataset[Row] = spark.sql("select * from non_pk_table_test")
          val result: Array[Row] = df.collect()
          Assertions.assertEquals(3, result.length)
        } finally {
          query.stop()
        }
    }
  }

  test("Fluss Source: test PkTable ReadOnlySnapshot") {
    withTempDir {
      checkpointDir =>
        bootstrapServers = String.join(",", flussConf.get(ConfigOptions.BOOTSTRAP_SERVERS))
        spark.sql(
          "create table read_snapshot_test (a int , b String)  OPTIONS ( 'primary.key' = 'a')")
        val tablePath: TablePath = TablePath.of(DEFAULT_DB, "read_snapshot_test")

        val rows: ju.List[InternalRow] = ju.Arrays.asList(row(1, "v1"), row(2, "v2"), row(3, "v3"))

        // write records// write records
        writeRows(tablePath, rows, false)

        waitUtilAllBucketFinishSnapshot(admin, tablePath)

        val reader = spark.readStream
          .format("fluss")
          .option("bootstrap.servers", bootstrapServers)
          .option("maxOffsetsPerTrigger", 5)
          .option("database", DEFAULT_DB)
          .option("table", "read_snapshot_test")
          .load()
        val query: StreamingQuery = reader.writeStream
          .format("memory")
          .queryName("read_snapshot_test")
          .outputMode("append")
          .start()
        try {
          query.processAllAvailable()
          val df: Dataset[Row] = spark.sql("select * from read_snapshot_test")
          val expectedRows = Seq(Row(1, "v1"), Row(2, "v2"), Row(3, "v3"))
          checkAnswer(df, expectedRows)
        } finally {
          query.stop()
        }
    }
  }

  logFormats.foreach {
    logFormat =>
      test(s"Fluss Source: test AppendTable ProjectPushDown $logFormat") {
        withTempDir {
          checkpointDir =>
            bootstrapServers = String.join(",", flussConf.get(ConfigOptions.BOOTSTRAP_SERVERS))

            val tableName = s"append_table_project_push_down_$logFormat"

            spark.sql(
              s"""
                 |create table $tableName (
                 |a int,
                 |b String,
                 |c bigint,
                 |d int,
                 |e int,
                 |f bigint)
                 |using fluss
                 |options ('table.log.format'='$logFormat')
         """.stripMargin
            )

            val tablePath: TablePath = TablePath.of(DEFAULT_DB, tableName)

            val rows: ju.List[InternalRow] = ju.Arrays.asList(
              row(1, "v1", 100L, 1000, 100, 1000L),
              row(2, "v2", 200L, 2000, 200, 2000L),
              row(3, "v3", 300L, 3000, 300, 3000L),
              row(4, "v4", 400L, 4000, 400, 4000L),
              row(5, "v5", 500L, 5000, 500, 5000L),
              row(6, "v6", 600L, 6000, 600, 6000L),
              row(7, "v7", 700L, 7000, 700, 7000L),
              row(8, "v8", 800L, 8000, 800, 8000L),
              row(9, "v9", 900L, 9000, 900, 9000L),
              row(10, "v10", 1000L, 10000, 1000, 10000L)
            )

            writeRows(tablePath, rows, true)

            val queryStr = s"select b, d, c from $tableName"

            val expectedRows: Seq[Row] = Seq(
              Row("v1", 1000, 100),
              Row("v2", 2000, 200),
              Row("v3", 3000, 300),
              Row("v4", 4000, 400),
              Row("v5", 5000, 500),
              Row("v6", 6000, 600),
              Row("v7", 7000, 700),
              Row("v8", 8000, 800),
              Row("v9", 9000, 900),
              Row("v10", 10000, 1000)
            )

            val reader = spark.readStream
              .format("fluss")
              .option("bootstrap.servers", bootstrapServers)
              .option("maxOffsetsPerTrigger", 5)
              .option("database", DEFAULT_DB)
              .option("table", tableName)
              .load()

            val query: StreamingQuery = reader.writeStream
              .format("memory")
              .queryName(tableName)
              .outputMode("append")
              .start()

            try {
              query.processAllAvailable()

              val df: Dataset[Row] = spark.sql(queryStr)

              checkAnswer(df, expectedRows)
            } finally {
              query.stop()
            }
        }

      }
  }

  modes.foreach {
    mode =>
      test(s"Fluss Source: test Table Project Push Down $mode") {

        withTempDir {
          checkpointDir =>
            bootstrapServers = String.join(",", flussConf.get(ConfigOptions.BOOTSTRAP_SERVERS))

            val isPkTable = mode.startsWith("PK")
            val testPkLog = mode.equals("PK_LOG")
            val tableName = s"table_$mode"

            val pkDDL = if (isPkTable) ", 'primary.key' = 'a'" else ""

            spark.sql(
              s"""
                 |CREATE TABLE $tableName (
                 |a INT,
                 |b STRING,
                 |c BIGINT,
                 |d INT )
                 |USING FLUSS
                 |OPTIONS ('table.log.format'='ARROW' $pkDDL)
           """.stripMargin
            )

            val tablePath: TablePath = TablePath.of(DEFAULT_DB, tableName)

            val rows: ju.List[InternalRow] = ju.Arrays.asList(
              row(1, "v1", 100L, 1000),
              row(2, "v2", 200L, 2000),
              row(3, "v3", 300L, 3000),
              row(4, "v4", 400L, 4000),
              row(5, "v5", 500L, 5000),
              row(6, "v6", 600L, 6000),
              row(7, "v7", 700L, 7000),
              row(8, "v8", 800L, 8000),
              row(9, "v9", 900L, 9000),
              row(10, "v10", 1000L, 10000)
            )

            if (isPkTable) {
              if (!testPkLog) {
                // write records and wait snapshot before collect job start,
                // to make sure reading from kv snapshot
                writeRows(tablePath, rows, false)
                waitUtilAllBucketFinishSnapshot(admin, TablePath.of(DEFAULT_DB, tableName))
              }
            } else {
              writeRows(tablePath, rows, true)
            }

            val queryStr = s"SELECT b, a, c FROM $tableName"

            val expectedRows: Seq[Row] = Seq(
              Row("v1", 1, 100),
              Row("v2", 2, 200),
              Row("v3", 3, 300),
              Row("v4", 4, 400),
              Row("v5", 5, 500),
              Row("v6", 6, 600),
              Row("v7", 7, 700),
              Row("v8", 8, 800),
              Row("v9", 9, 900),
              Row("v10", 10, 1000)
            )

            val reader = spark.readStream
              .format("fluss")
              .option("bootstrap.servers", bootstrapServers)
              .option("maxOffsetsPerTrigger", 5)
              .option("database", DEFAULT_DB)
              .option("table", tableName)
              .load()

            val query: StreamingQuery = reader.writeStream
              .format("memory")
              .queryName(tableName)
              .outputMode("append")
              .start()
            if (testPkLog) {
              // delay the write after collect job start,
              // to make sure reading from log instead of snapshot
              writeRows(tablePath, rows, false)
            }
            try {
              query.processAllAvailable()

              val df: Dataset[Row] = spark.sql(queryStr)

              checkAnswer(df, expectedRows)
            } finally {
              query.stop()
            }

        }
      }
  }

  // -------------------------------------------------------------------------------------
  // Fluss scan start mode tests
  // -------------------------------------------------------------------------------------



  modes.foreach { isPartitioned => }

  test(s"Fluss Source: test Read Log Table With Different Scan Startup Mode isPartitioned") {
    var isPartitioned = true;
    withTempDir {
      checkpointDir =>
        bootstrapServers = String.join(",", flussConf.get(ConfigOptions.BOOTSTRAP_SERVERS))

        val tableFullName =
          s"fluss.$DEFAULT_DB.tab1_${if (isPartitioned) "partitioned" else "non_partitioned"}"
        val tableName = s"tab1_${if (isPartitioned) "partitioned" else "non_partitioned"}"
        var partitionName: Option[String] = None

        val tablePath: TablePath = TablePath.of(DEFAULT_DB, tableName)

        if (!isPartitioned) {
          spark.sql(
            s"""
               |CREATE TABLE $tableFullName (
               |a INT,
               |b STRING,
               |c BIGINT,
               |d INT)
               |USING FLUSS
             """.stripMargin
          )
        } else {
          spark.sql(
            s"""
               |CREATE TABLE $tableFullName (
               |a INT,
               |b STRING,
               |c BIGINT,
               |d INT,
               |p STRING )
               |PARTITIONED BY (p)
               |OPTIONS(
               |'table.auto-partition.enabled' = 'true',
               |'table.auto-partition.time-unit' = 'YEAR',
               |'table.auto-partition.num-precreate' = '1')
             """.stripMargin
          )
          val partitionNameById =
            waitUntilPartitions(FLUSS_CLUSTER_EXTENSION.getZooKeeperClient, tablePath, 1)
          partitionName = Some(partitionNameById.values.iterator.next())
        }

        val rows1: ju.List[InternalRow] = ju.Arrays.asList(
          rowWithPartition(Array(1, "v1", 100L, 1000), partitionName.orNull),
          rowWithPartition(Array(2, "v2", 200L, 2000), partitionName.orNull),
          rowWithPartition(Array(3, "v3", 300L, 3000), partitionName.orNull),
          rowWithPartition(Array(4, "v4", 400L, 4000), partitionName.orNull),
          rowWithPartition(Array(5, "v5", 500L, 5000), partitionName.orNull)
        )

        writeRows(tablePath, rows1, true)

        val rows2: ju.List[InternalRow] = ju.Arrays.asList(
          rowWithPartition(Array(6, "v6", 600L, 6000), partitionName.orNull),
          rowWithPartition(Array(7, "v7", 700L, 7000), partitionName.orNull),
          rowWithPartition(Array(8, "v8", 800L, 8000), partitionName.orNull),
          rowWithPartition(Array(9, "v9", 900L, 9000), partitionName.orNull),
          rowWithPartition(Array(10, "v10", 1000L, 10000), partitionName.orNull)
        )

        writeRows(tablePath, rows2, true)

        val expectedFull: Seq[Row] = Seq(
          Row(1, "v1", 100L, 1000),
          Row(2, "v2", 200L, 2000),
          Row(3, "v3", 300L, 3000),
          Row(4, "v4", 400L, 4000),
          Row(5, "v5", 500L, 5000),
          Row(6, "v6", 600L, 6000),
          Row(7, "v7", 700L, 7000),
          Row(8, "v8", 800L, 8000),
          Row(9, "v9", 900L, 9000),
          Row(10, "v10", 1000L, 10000)
        )

        // 1. read log table with scan.startup.mode='full'
        val queryStr = s"SELECT a, b, c, d FROM $tableName"
        var reader = spark.readStream
          .format("fluss")
          .option("bootstrap.servers", bootstrapServers)
          .option("maxOffsetsPerTrigger", 5)
          .option("database", DEFAULT_DB)
          .option("table", tableName)
          .option("scan.startup.mode", "full")
          .load()

        var query: StreamingQuery = reader.writeStream
          .format("memory")
          .queryName(tableName)
          .outputMode("append")
          .start()

        try {
          query.processAllAvailable()

          val df: Dataset[Row] = spark.sql(queryStr)

          checkAnswer(df, expectedFull)
        } finally {
          query.stop()
        }

        // 2. read log table with scan.startup.mode='earliest'
        reader = spark.readStream
          .format("fluss")
          .option("bootstrap.servers", bootstrapServers)
          .option("maxOffsetsPerTrigger", 5)
          .option("database", DEFAULT_DB)
          .option("table", tableName)
          .option("scan.startup.mode", "earliest")
          .load()
        query = reader.writeStream
          .format("memory")
          .queryName(tableName)
          .outputMode("append")
          .start()
        try {
          query.processAllAvailable()

          val df: Dataset[Row] = spark.sql(queryStr)

          checkAnswer(df, expectedFull)
        } finally {
          query.stop()
        }

        // 3. read log table with scan.startup.mode='timestamp'
        reader = spark.readStream
          .format("fluss")
          .option("bootstrap.servers", bootstrapServers)
          .option("maxOffsetsPerTrigger", 5)
          .option("database", DEFAULT_DB)
          .option("table", tableName)
          .option("scan.startup.mode", "timestamp")
          .option("scan.startup.timestamp", "1000")
          .load()
        query = reader.writeStream
          .format("memory")
          .queryName(tableName)
          .outputMode("append")
          .start()
        try {
          query.processAllAvailable()

          val df: Dataset[Row] = spark.sql(queryStr)

          checkAnswer(df, expectedFull)
        } finally {
          query.stop()
        }
    }
  }

  test("Fluss Source: test Read Kv Table With Scan Startup Mode Equals Full") {
    withTempDir { checkpointDir =>
      import testImplicits._
      bootstrapServers = String.join(",", flussConf.get(ConfigOptions.BOOTSTRAP_SERVERS))

      val tableName = "read_full_test"
      val tablePath: TablePath = TablePath.of(DEFAULT_DB, tableName)

      spark.sql(
        s"""
           |CREATE TABLE $tableName (
           |a INT ,
           |b STRING)
           |USING FLUSS
           |OPTIONS ('primary.key' = 'a')
         """.stripMargin
      )

      val rows1: ju.List[InternalRow] = ju.Arrays.asList(
        row(1, "v1"),
        row(2, "v2"),
        row(3, "v3"),
        row(3, "v33")
      )

      writeRows(tablePath, rows1, false)
      waitUtilAllBucketFinishSnapshot(admin, tablePath)

      val rows2: ju.List[InternalRow] = ju.Arrays.asList(
        row(1, "v11"),
        row(2, "v22"),
        row(4, "v4")
      )

      val queryStr = s"SELECT a, b FROM $tableName"
      val expected: Seq[Row] = Seq(
        Row( 1, "v1"),
        Row( 2, "v2"),
        Row( 3, "v33"),
        Row( 1, "v1"),
        Row( 1, "v11"),
        Row( 2, "v2"),
        Row( 2, "v22"),
        Row( 4, "v4")
      )

      val reader = spark.readStream
        .format("fluss")
        .option("bootstrap.servers", bootstrapServers)
        .option("maxOffsetsPerTrigger", 5)
        .option("database", DEFAULT_DB)
        .option("table", tableName)
        .option("scan.startup.mode", "full")
        .load()
        .select($"a".as[String], $"b".as[String])

      val query: StreamingQuery = reader.writeStream
        .format("memory")
        .queryName(tableName)
        .outputMode("append")
        .foreachBatch { (ds: Dataset[(String,String)], epochId: Long) =>
          if (epochId == 0) {
            // Send more message before the tasks of the current batch start reading the current batch
            // data
            writeRows(tablePath, rows2, false)
            val rows3 = Seq(
              Row(1, "v11"),
              Row(2, "v22"),
              Row(4, "v4")
            )
            val tuples = ds.collect()
            tuples.foreach(row => {
              assert(rows3.contains(row))
            })
          } else {
            val tuples = ds.collect()
            tuples.foreach(row => {
              assert(expected.contains(row))
            })
          }
        }
        .start()

      try {
        query.processAllAvailable()

      } finally {
        query.stop()
      }
    }
  }


  private def waitUtilAllBucketFinishSnapshot(admin: Admin, tablePath: TablePath): Unit = {
    waitUtil(
      () => {
        val snapshots = admin.getLatestKvSnapshots(tablePath).get
        import scala.collection.JavaConversions._

        val allBucketsHaveSnapshots = snapshots.getBucketIds.forall {
          bucketId => snapshots.getSnapshotId(bucketId).isPresent
        }

        allBucketsHaveSnapshots
      },
      Duration.ofMinutes(1),
      "Fail to wait until all bucket finish snapshot"
    )
  }

}
