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

package org.apache.spark.scheduler

import java.util.Properties

import org.apache.spark.TaskContext
import org.apache.spark.util.CallSite

/**
 * Tracks information about an active job in the DAGScheduler.
 */
private[spark] class ActiveJob(
    val jobId: Int,
    val finalStage: Stage,
    val func: (TaskContext, Iterator[_]) => _,
    val partitions: Array[Int],
    val callSite: CallSite,
    val listener: JobListener,
    val properties: Properties) {
    //该Job需要计算的partitions个数
  val numPartitions = partitions.length
  /** 一个Boolean类型的数组，初始值为false
   * 数组长度为partitions个数，哪个partition被计算了，则对应的
   * 值标记为true
   */
  val finished = Array.fill[Boolean](numPartitions)(false)
  // 处理完成的partition个数
  var numFinished = 0
}
