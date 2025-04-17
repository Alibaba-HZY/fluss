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

package org.apache.spark.sql.fluss

import org.apache.spark.sql.fluss.MockedSystemClock.currentMockSystemTime
import org.apache.spark.util.{ManualClock, SystemClock}

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */
trait FlussClock {
  def getTimeMillis(): Long
}
class ClockUtils {}
object ClockUtils {
  def systemClock(): FlussClock = {
    new WrapSystemClock(new SystemClock())
  }
}
class WrapSystemClock(systemClock: SystemClock) extends FlussClock {
  override def getTimeMillis(): Long = {
    systemClock.getTimeMillis()
  }
}

/** To return a mocked system clock for testing purposes */
class MockedSystemClock extends ManualClock with FlussClock {
  override def getTimeMillis(): Long = {
    currentMockSystemTime
  }
}

object MockedSystemClock {
  var currentMockSystemTime = 0L

  def advanceCurrentSystemTime(advanceByMillis: Long): Unit = {
    currentMockSystemTime += advanceByMillis
  }

  def reset(): Unit = {
    currentMockSystemTime = 0L
  }
}
