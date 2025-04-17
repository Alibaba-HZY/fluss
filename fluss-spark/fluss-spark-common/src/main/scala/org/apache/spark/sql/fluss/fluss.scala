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

package org.apache.spark.sql

import com.alibaba.fluss.spark.TableBucketInfo

import org.apache.spark.internal.config.ConfigBuilder

import java.util.concurrent.TimeUnit

package object fluss {
  // scalastyle:ignore
  // ^^ scalastyle:ignore is for ignoring warnings about digits in package name
  type BucketOffsetMap = Map[TableBucketInfo, Long]

  private val PRODUCER_CACHE_TIMEOUT =
    ConfigBuilder("spark.fluss.producer.cache.timeout")
      .doc("The expire time to remove the unused producers.")
      .version("2.2.1")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("10m")

  private val PRODUCER_CACHE_EVICTOR_THREAD_RUN_INTERVAL =
    ConfigBuilder("spark.fluss.producer.cache.evictorThreadRunInterval")
      .doc("The interval of time between runs of the idle evictor thread for producer pool. " +
        "When non-positive, no idle evictor thread will be run.")
      .version("3.0.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("1m")

  private val CONSUMER_CACHE_CAPACITY =
    ConfigBuilder("spark.fluss.consumer.cache.capacity")
      .doc("The maximum number of consumers cached. Please note it's a soft limit" +
        " (check Structured Streaming fluss integration guide for further details).")
      .version("3.0.0")
      .intConf
      .createWithDefault(64)

  private val CONSUMER_CACHE_JMX_ENABLED =
    ConfigBuilder("spark.fluss.consumer.cache.jmx.enable")
      .doc("Enable or disable JMX for pools created with this configuration instance.")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(false)

  private val CONSUMER_CACHE_TIMEOUT =
    ConfigBuilder("spark.fluss.consumer.cache.timeout")
      .doc(
        "The minimum amount of time a consumer may sit idle in the pool before " +
          "it is eligible for eviction by the evictor. " +
          "When non-positive, no consumers will be evicted from the pool due to idle time alone.")
      .version("3.0.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("5m")

  private val CONSUMER_CACHE_EVICTOR_THREAD_RUN_INTERVAL =
    ConfigBuilder("spark.fluss.consumer.cache.evictorThreadRunInterval")
      .doc("The interval of time between runs of the idle evictor thread for consumer pool. " +
        "When non-positive, no idle evictor thread will be run.")
      .version("3.0.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("1m")

  private val FETCHED_DATA_CACHE_TIMEOUT =
    ConfigBuilder("spark.fluss.consumer.fetchedData.cache.timeout")
      .doc("The minimum amount of time a fetched data may sit idle in the pool before " +
        "it is eligible for eviction by the evictor. " +
        "When non-positive, no fetched data will be evicted from the pool due to idle time alone.")
      .version("3.0.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("5m")

  private val FETCHED_DATA_CACHE_EVICTOR_THREAD_RUN_INTERVAL =
    ConfigBuilder("spark.fluss.consumer.fetchedData.cache.evictorThreadRunInterval")
      .doc("The interval of time between runs of the idle evictor thread for fetched data pool. " +
        "When non-positive, no idle evictor thread will be run.")
      .version("3.0.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("1m")
}
