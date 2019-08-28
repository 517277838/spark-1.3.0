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

package org.apache.spark.deploy.master

import java.io.FileNotFoundException
import java.net.URLEncoder
import java.text.SimpleDateFormat
import java.util.Date

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Random

import akka.actor._
import akka.pattern.ask
import akka.remote.{DisassociatedEvent, RemotingLifecycleEvent}
import akka.serialization.Serialization
import akka.serialization.SerializationExtension
import org.apache.hadoop.fs.Path

import org.apache.spark.{Logging, SecurityManager, SparkConf, SparkException}
import org.apache.spark.deploy.{ApplicationDescription, DriverDescription,ExecutorState, SparkHadoopUtil}
import org.apache.spark.deploy.DeployMessages._
import org.apache.spark.deploy.history.HistoryServer
import org.apache.spark.deploy.master.DriverState.DriverState
import org.apache.spark.deploy.master.MasterMessages._
import org.apache.spark.deploy.master.ui.MasterWebUI
import org.apache.spark.deploy.rest.StandaloneRestServer
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.scheduler.{EventLoggingListener, ReplayListenerBus}
import org.apache.spark.ui.SparkUI
import org.apache.spark.util.{ActorLogReceive, AkkaUtils, SignalLogger, Utils}

private[spark] class Master(
    host: String,
    port: Int,
    webUiPort: Int,
    val securityMgr: SecurityManager,
    val conf: SparkConf)
  extends Actor with ActorLogReceive with Logging with LeaderElectable {

  import context.dispatcher   // to use Akka's scheduler.schedule()

  val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)

  //定义变量
  def createDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")  // For application IDs
  val WORKER_TIMEOUT = conf.getLong("spark.worker.timeout", 60) * 1000
  val RETAINED_APPLICATIONS = conf.getInt("spark.deploy.retainedApplications", 200)
  val RETAINED_DRIVERS = conf.getInt("spark.deploy.retainedDrivers", 200)
  val REAPER_ITERATIONS = conf.getInt("spark.dead.worker.persistence", 15)
  val RECOVERY_MODE = conf.get("spark.deploy.recoveryMode", "NONE")

  val workers = new HashSet[WorkerInfo]
  val idToWorker = new HashMap[String, WorkerInfo]
  val addressToWorker = new HashMap[Address, WorkerInfo]

  val apps = new HashSet[ApplicationInfo]
  val idToApp = new HashMap[String, ApplicationInfo]
  val actorToApp = new HashMap[ActorRef, ApplicationInfo]
  val addressToApp = new HashMap[Address, ApplicationInfo]
  val waitingApps = new ArrayBuffer[ApplicationInfo]
  val completedApps = new ArrayBuffer[ApplicationInfo]
  var nextAppNumber = 0
  val appIdToUI = new HashMap[String, SparkUI]

  val drivers = new HashSet[DriverInfo]
  val completedDrivers = new ArrayBuffer[DriverInfo]
  val waitingDrivers = new ArrayBuffer[DriverInfo] 
  // Drivers currently spooled for scheduling
  var nextDriverNumber = 0

  Utils.checkHost(host, "Expected hostname")

  val masterMetricsSystem = MetricsSystem.createMetricsSystem("master", conf, securityMgr)
  val applicationMetricsSystem = MetricsSystem.createMetricsSystem("applications", conf,
    securityMgr)
  val masterSource = new MasterSource(this)

  val webUi = new MasterWebUI(this, webUiPort)
  
  val masterPublicAddress = {
    val envVar = conf.getenv("SPARK_PUBLIC_DNS")
    if (envVar != null) envVar else host
  }

  val masterUrl = "spark://" + host + ":" + port
  var masterWebUiUrl: String = _

  var state = RecoveryState.STANDBY

  var persistenceEngine: PersistenceEngine = _

  var leaderElectionAgent: LeaderElectionAgent = _

  private var recoveryCompletionTask: Cancellable = _

  // As a temporary workaround before better ways of configuring memory, we allow users to set
  // a flag that will perform round-robin scheduling across the nodes (spreading out each app
  // among all the nodes) instead of trying to consolidate each app onto a small # of nodes.
  val spreadOutApps = conf.getBoolean("spark.deploy.spreadOut", true)

  // Default maxCores for applications that don't specify it (i.e. pass Int.MaxValue)
  val defaultCores = conf.getInt("spark.deploy.defaultCores", Int.MaxValue)
  if (defaultCores < 1) {
    throw new SparkException("spark.deploy.defaultCores must be positive")
  }

  // Alternative application submission gateway that is stable across Spark versions
  private val restServerEnabled = conf.getBoolean("spark.master.rest.enabled", true)
  private val restServer =
    if (restServerEnabled) {
      val port = conf.getInt("spark.master.rest.port", 6066)
      Some(new StandaloneRestServer(host, port, self, masterUrl, conf))
    } else {
      None
    }
  private val restServerBoundPort = restServer.map(_.start())

  /**
    * ##################以上为Master类定义的变量########################
    */
  //开始启动master，并且根据配置文件获取持久化引擎,常用的(zookeeper持久化引擎，filesystem持久化引擎)
  override def preStart() {
    logInfo("Starting Spark master at " + masterUrl)
    logInfo(s"Running Spark version ${org.apache.spark.SPARK_VERSION}")
    // Listen for remote client disconnection events, since they don't go through Akka's watch()
    context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])
    webUi.bind()
    masterWebUiUrl = "http://" + masterPublicAddress + ":" + webUi.boundPort
    context.system.scheduler.schedule(0 millis, WORKER_TIMEOUT millis, self, CheckForWorkerTimeOut)

    masterMetricsSystem.registerSource(masterSource)
    masterMetricsSystem.start()
    applicationMetricsSystem.start()
    // Attach the master and app metrics servlet handler to the web ui after the metrics systems are
    // started.
    masterMetricsSystem.getServletHandlers.foreach(webUi.attachHandler)
    applicationMetricsSystem.getServletHandlers.foreach(webUi.attachHandler)
    /**
      * 根据配置文件获取持久化引擎,常用的(zookeeper持久化引擎，filesystem持久化引擎)
      */
    val (persistenceEngine_, leaderElectionAgent_) = RECOVERY_MODE match {
      case "ZOOKEEPER" =>
        logInfo("Persisting recovery state to ZooKeeper")
        val zkFactory =
          new ZooKeeperRecoveryModeFactory(conf, SerializationExtension(context.system))
        (zkFactory.createPersistenceEngine(), zkFactory.createLeaderElectionAgent(this))
      case "FILESYSTEM" =>
        val fsFactory =
          new FileSystemRecoveryModeFactory(conf, SerializationExtension(context.system))
        (fsFactory.createPersistenceEngine(), fsFactory.createLeaderElectionAgent(this))
      case "CUSTOM" =>
        val clazz = Class.forName(conf.get("spark.deploy.recoveryMode.factory"))
        val factory = clazz.getConstructor(conf.getClass, Serialization.getClass)
          .newInstance(conf, SerializationExtension(context.system))
          .asInstanceOf[StandaloneRecoveryModeFactory]
        (factory.createPersistenceEngine(), factory.createLeaderElectionAgent(this))
      case _ =>
        (new BlackHolePersistenceEngine(), new MonarchyLeaderAgent(this))
    }
    persistenceEngine = persistenceEngine_
    leaderElectionAgent = leaderElectionAgent_
  }

  override def preRestart(reason: Throwable, message: Option[Any]) {
    super.preRestart(reason, message) // calls postStop()!
    logError("Master actor restarted due to exception", reason)
  }

  override def postStop() {
    masterMetricsSystem.report()
    applicationMetricsSystem.report()
    // prevent the CompleteRecovery message sending to restarted master
    if (recoveryCompletionTask != null) {
      recoveryCompletionTask.cancel()
    }
    webUi.stop()
    restServer.foreach(_.stop())
    masterMetricsSystem.stop()
    applicationMetricsSystem.stop()
    persistenceEngine.close()
    leaderElectionAgent.stop()
  }

  override def electedLeader() {
    self ! ElectedLeader
  }

  override def revokedLeadership() {
    self ! RevokedLeadership
  }

  override def receiveWithLogging = {
    case ElectedLeader => {
      //[1.step]
      //使用持久化引擎取读取持久化的storedApps, storedDrivers, storedWorkers
      val (storedApps, storedDrivers, storedWorkers) = persistenceEngine.readPersistedData()
      //[2.step]
      //判断storedApps, storedDrivers, storedWorkers其实是否有任何一个为非空情况
      //如果满足条件都为空这表示当前master存活且正常，
      state = if (storedApps.isEmpty && storedDrivers.isEmpty && storedWorkers.isEmpty) {
        RecoveryState.ALIVE
      } else {
        RecoveryState.RECOVERING
      }
      logInfo("I have been elected leader! New state: " + state)
      //[3.step]
      // 只有有一个为非空，将持久化Application、Driver、Worker的信息，重新进行注册，注册到Master内部的内存缓存结构
      if (state == RecoveryState.RECOVERING) {
        //[4.step]将持久化的Application、Driver、Worker的信息重新进行注册，注册到Master内部的内存缓存结构中,进入该方法
        beginRecovery(storedApps, storedDrivers, storedWorkers)
        recoveryCompletionTask = context.system.scheduler.scheduleOnce(WORKER_TIMEOUT millis, self,
          CompleteRecovery)
      }
    }

    case CompleteRecovery => completeRecovery()

    case RevokedLeadership => {
      logError("Leadership has been revoked -- master shutting down.")
      System.exit(0)
    }
    //注册worker,最后调用schedule()
    case RegisterWorker(id, workerHost, workerPort, cores, memory, workerUiPort, publicAddress) =>
    {
      logInfo("Registering worker %s:%d with %d cores, %s RAM".format(
        workerHost, workerPort, cores, Utils.megabytesToString(memory)))
      if (state == RecoveryState.STANDBY) {
        // ignore, don't send response
      } else if (idToWorker.contains(id)) {
        sender ! RegisterWorkerFailed("Duplicate worker ID")
      } else {
        val worker = new WorkerInfo(id, workerHost, workerPort, cores, memory,
          sender, workerUiPort, publicAddress)
        if (registerWorker(worker)) {
          persistenceEngine.addWorker(worker)
          sender ! RegisteredWorker(masterUrl, masterWebUiUrl)
          schedule()
        } else {
          val workerAddress = worker.actor.path.address
          logWarning("Worker registration failed. Attempted to re-register worker at same " +
            "address: " + workerAddress)
          sender ! RegisterWorkerFailed("Attempted to re-register worker at same address: "
            + workerAddress)
        }
      }
    }

    case RequestSubmitDriver(description) => {
      if (state != RecoveryState.ALIVE) {
        val msg = s"Can only accept driver submissions in ALIVE state. Current state: $state."
        sender ! SubmitDriverResponse(false, None, msg)
      } else {
        logInfo("Driver submitted " + description.command.mainClass)
        val driver = createDriver(description)
        persistenceEngine.addDriver(driver)
        waitingDrivers += driver
        drivers.add(driver)
        schedule()

        // TODO: It might be good to instead have the submission client poll the master to determine
        //       the current status of the driver. For now it's simply "fire and forget".

        sender ! SubmitDriverResponse(true, Some(driver.id),
          s"Driver successfully submitted as ${driver.id}")
      }
    }

    case RequestKillDriver(driverId) => {
      if (state != RecoveryState.ALIVE) {
        val msg = s"Can only kill drivers in ALIVE state. Current state: $state."
        sender ! KillDriverResponse(driverId, success = false, msg)
      } else {
        logInfo("Asked to kill driver " + driverId)
        val driver = drivers.find(_.id == driverId)
        driver match {
          case Some(d) =>
            if (waitingDrivers.contains(d)) {
              waitingDrivers -= d
              self ! DriverStateChanged(driverId, DriverState.KILLED, None)
            } else {
              // We just notify the worker to kill the driver here. The final bookkeeping occurs
              // on the return path when the worker submits a state change back to the master
              // to notify it that the driver was successfully killed.
              d.worker.foreach { w =>
                w.actor ! KillDriver(driverId)
              }
            }
            // TODO: It would be nice for this to be a synchronous response
            val msg = s"Kill request for $driverId submitted"
            logInfo(msg)
            sender ! KillDriverResponse(driverId, success = true, msg)
          case None =>
            val msg = s"Driver $driverId has already finished or does not exist"
            logWarning(msg)
            sender ! KillDriverResponse(driverId, success = false, msg)
        }
      }
    }

    case RequestDriverStatus(driverId) => {
      (drivers ++ completedDrivers).find(_.id == driverId) match {
        case Some(driver) =>
          sender ! DriverStatusResponse(found = true, Some(driver.state),
            driver.worker.map(_.id), driver.worker.map(_.hostPort), driver.exception)
        case None =>
          sender ! DriverStatusResponse(found = false, None, None, None, None)
      }
    }

    /**
      * 处理Application注册的请求
      *
      */
    case RegisterApplication(description) => {
      //如果master的状态是standby,也就是当前这个master,是standby Master,不是Active Master
      //那么Application来请求注册，什么都不会做
      if (state == RecoveryState.STANDBY) {
        // ignore, don't send response
      } else {
        logInfo("Registering app " + description.name)
        //用ApplicationDescription信息创建ApplicationInfo
        val app = createApplication(description, sender)
        //注册Applictiaon
        //将ApplicationInfo加入缓存，将Application加入等待调度的队列－waitingApps
        registerApplication(app)
        logInfo("Registered app " + description.name + " with ID " + app.id)
        //使用持久化引擎,将ApplicationInfo进行持久化
        persistenceEngine.addApplication(app)
        //反向，向SparkDeploySchedulerBackend的AppClient的ClientActor，发送消息，也就是RegisteredApplication（和RegisterApplication一字之差）
        sender ! RegisteredApplication(app.id, masterUrl)
        schedule()
      }
    }

    case ExecutorStateChanged(appId, execId, state, message, exitStatus) => {
      //找到executor对应的app,然后再放过来通过app内部的executor缓存获取executor信息
      val execOption = idToApp.get(appId).flatMap(app => app.executors.get(execId))
      execOption match {
          //如果有值
        case Some(exec) => {
          //设置executor的当前状态
          val appInfo = idToApp(appId)
          exec.state = state
          if (state == ExecutorState.RUNNING) { appInfo.resetRetryCount() }
          //向driver同步发送ExecutorUpdated消息
          exec.application.driver ! ExecutorUpdated(execId, state, message, exitStatus)
          //判断，如果executor完成了
          if (ExecutorState.isFinished(state)) {
            // Remove this executor from the worker and app
            logInfo(s"Removing executor ${exec.fullId} because it is $state")
            //从app的缓存中移除executor
            appInfo.removeExecutor(exec)
            //从运行executor的worker的缓存中移除executor
            exec.worker.removeExecutor(exec)
            //判断，如果executor的退出状态是非正常的
            val normalExit = exitStatus == Some(0)
            // Only retry certain number of times so we don't go into an infinite loop.
            if (!normalExit) {
              //判断application当前的重试次数，是否达到了最大值（10）
              if (appInfo.incrementRetryCount() < ApplicationState.MAX_NUM_RETRY) {
                //重新进行调度()
                schedule()
              } else {
                //否则，那么就进行removeApplication操作
                //也就是说，executor反复调度都失败，那么就任务application也失败了
                val execs = appInfo.executors.values
                if (!execs.exists(_.state == ExecutorState.RUNNING)) {
                  logError(s"Application ${appInfo.desc.name} with ID ${appInfo.id} failed " +
                    s"${appInfo.retryCount} times; removing it")
                  removeApplication(appInfo, ApplicationState.FAILED)
                }
              }
            }
          }
        }
        case None =>
          logWarning(s"Got status update for unknown executor $appId/$execId")
      }
    }

    case DriverStateChanged(driverId, state, exception) => {
      state match {
          //如果driver的状态是错误、完成、被杀掉、失败
          //那么就移除driver
        case DriverState.ERROR | DriverState.FINISHED | DriverState.KILLED | DriverState.FAILED =>
          removeDriver(driverId, state, exception)
        case _ =>
          throw new Exception(s"Received unexpected state update for driver $driverId: $state")
      }
    }

    case Heartbeat(workerId) => {
      idToWorker.get(workerId) match {
        case Some(workerInfo) =>
          workerInfo.lastHeartbeat = System.currentTimeMillis()
        case None =>
          if (workers.map(_.id).contains(workerId)) {
            logWarning(s"Got heartbeat from unregistered worker $workerId." +
              " Asking it to re-register.")
            sender ! ReconnectWorker(masterUrl)
          } else {
            logWarning(s"Got heartbeat from unregistered worker $workerId." +
              " This worker was never registered, so ignoring the heartbeat.")
          }
      }
    }

    case MasterChangeAcknowledged(appId) => {
      idToApp.get(appId) match {
        case Some(app) =>
          logInfo("Application has been re-registered: " + appId)
          app.state = ApplicationState.WAITING
        case None =>
          logWarning("Master change ack from unknown app: " + appId)
      }

      if (canCompleteRecovery) { completeRecovery() }
    }

    case WorkerSchedulerStateResponse(workerId, executors, driverIds) => {
      idToWorker.get(workerId) match {
        case Some(worker) =>
          logInfo("Worker has been re-registered: " + workerId)
          worker.state = WorkerState.ALIVE

          val validExecutors = executors.filter(exec => idToApp.get(exec.appId).isDefined)
          for (exec <- validExecutors) {
            val app = idToApp.get(exec.appId).get
            val execInfo = app.addExecutor(worker, exec.cores, Some(exec.execId))
            worker.addExecutor(execInfo)
            execInfo.copyState(exec)
          }

          for (driverId <- driverIds) {
            drivers.find(_.id == driverId).foreach { driver =>
              driver.worker = Some(worker)
              driver.state = DriverState.RUNNING
              worker.drivers(driverId) = driver
            }
          }
        case None =>
          logWarning("Scheduler state from unknown worker: " + workerId)
      }

      if (canCompleteRecovery) { completeRecovery() }
    }

    case DisassociatedEvent(_, address, _) => {
      // The disconnected client could've been either a worker or an app; remove whichever it was
      logInfo(s"$address got disassociated, removing it.")
      addressToWorker.get(address).foreach(removeWorker)
      addressToApp.get(address).foreach(finishApplication)
      if (state == RecoveryState.RECOVERING && canCompleteRecovery) { completeRecovery() }
    }

    case RequestMasterState => {
      sender ! MasterStateResponse(
        host, port, restServerBoundPort,
        workers.toArray, apps.toArray, completedApps.toArray,
        drivers.toArray, completedDrivers.toArray, state)
    }

    case CheckForWorkerTimeOut => {
      timeOutDeadWorkers()
    }

    case BoundPortsRequest => {
      sender ! BoundPortsResponse(port, webUi.boundPort, restServerBoundPort)
    }
  }

  def canCompleteRecovery =
    workers.count(_.state == WorkerState.UNKNOWN) == 0 &&
      apps.count(_.state == ApplicationState.UNKNOWN) == 0

  def beginRecovery(storedApps: Seq[ApplicationInfo], storedDrivers: Seq[DriverInfo],
      storedWorkers: Seq[WorkerInfo]) {
    for (app <- storedApps) {
      logInfo("Trying to recover app: " + app.id)
      try {
        //将application的信息注册到master内部的内存缓存结构中
        registerApplication(app)
        //[5.step]
        //将Application的状态修改为UNKNOWN
        app.state = ApplicationState.UNKNOWN
        //向Application对应的driver发送Standby Master的地址
        app.driver ! MasterChanged(masterUrl, masterWebUiUrl)
      } catch {
        case e: Exception => logInfo("App " + app.id + " had exception on reconnect")
      }
    }

    for (driver <- storedDrivers) {
      // Here we just read in the list of drivers. Any drivers associated with now-lost workers
      // will be re-launched when we detect that the worker is missing.
      drivers += driver
    }

    for (worker <- storedWorkers) {
      logInfo("Trying to recover worker: " + worker.id)
      try {
        //将worker的信息注册到master内部的内存缓存结构中
        registerWorker(worker)
        //[4_1.step]
        //将Worker的状态修改为UNKNOWN
        worker.state = WorkerState.UNKNOWN
        //向Workern对应的driver发送Standby Master的地址
        worker.actor ! MasterChanged(masterUrl, masterWebUiUrl)
      } catch {
        case e: Exception => logInfo("Worker " + worker.id + " had exception on reconnect")
      }
    }
  }
  //[5.step 暂没找到对应的源码]
  // Driver和Worker,理论上来说，如果他们目前都是正常在运行的话
  //那么在接受到Master发送来的地址之后，就会返回响应消息给新的Master

  //[6.step]
  //当，Master在陆续接收到Driver和Worker发送来的响应消息之后，使用completeRecovery方法
  //(完成Master的主备切换实际就是完成 Master的恢复)
  def completeRecovery() {
    // Ensure "only-once" recovery semantics using a short synchronization period.
    synchronized {
      //对没有发送响应消息的Driver和Worker进行处理
      if (state != RecoveryState.RECOVERING) { return }
      state = RecoveryState.COMPLETING_RECOVERY
    }

    // Kill off any workers and apps that didn't respond to us.
    //过滤掉它们的信息
    //将Application和Worker,过滤出来目前状态还是UNKNOWN的
    //然后遍历，分别调用removeWorker和finishApplication方法，对可能已经出现故障，或者甚至已经死掉的
    //Application和Worker，进行清理
    workers.filter(_.state == WorkerState.UNKNOWN).foreach(removeWorker)
    apps.filter(_.state == ApplicationState.UNKNOWN).foreach(finishApplication)

    // Reschedule drivers which were not claimed by any workers
    drivers.filter(_.worker.isEmpty).foreach { d =>
      logWarning(s"Driver ${d.id} was not found after master recovery")
      if (d.desc.supervise) {
        logWarning(s"Re-launching ${d.id}")
        relaunchDriver(d)
      } else {
        removeDriver(d.id, DriverState.ERROR, None)
        logWarning(s"Did not re-launch ${d.id} because it was not supervised")
      }
    }

    state = RecoveryState.ALIVE
    schedule()
    logInfo("Recovery complete - resuming operations!")
  }
  //[7.step 暂未找到源码]
  //调用Master自己的schedule()方法，对正在等待资源调度的Driver和Application进行调度
  //比如在某个worker上启动Driver,或者为Application在Worker上启动它需要的Executor
  /**
   * Can an app use the given worker? True if the worker has enough memory and we haven't already
   * launched an executor for the app on it (right now the standalone backend doesn't like having
   * two executors on the same worker).
   */
  def canUse(app: ApplicationInfo, worker: WorkerInfo): Boolean = {
    //worker启动executor的条件
    //worker只少满足启动一个executor，而且这个worker必须是没有启动过executor
    worker.memoryFree >= app.desc.memoryPerSlave && !worker.hasExecutor(app)
  }

  /**
   * Schedule the currently available resources among waiting apps. This method will be called
   * every time a new app joins or resource availability changes.
   */
  //Master资源调度(极其重要)
  private def schedule() {
    //首先判断，master状态不是ALIVE的话，直接返回
    //也就是说，standby master是没有资格进行application等资源的调度的
    if (state != RecoveryState.ALIVE) { return }

    // First schedule drivers, they take strict precedence over applications
    // Randomization helps balance drivers
    //Random.shuffle的原理，大家要清楚，就是对传人的集合的元素进行随即的打乱
    //取出了worker中的所以之前注册上来的worker,进行过滤，必须是状态为ALIVE的worker
    //对状态为ALIVE的worker,调用Ranm的shuffle方法进行随机的打乱
    val shuffledAliveWorkers = Random.shuffle(workers.toSeq.filter(_.state == WorkerState.ALIVE))
    val numWorkersAlive = shuffledAliveWorkers.size
    var curPos = 0

    /**首先，调用driver
     **为什么要调用driver,什么情况下会注册driver,并且会导致driver被调度
     **其实，只有用yarn-cluster模式提交的时候，才会注册driver,因为standlone和yarn-client模式，都会再本地直接
     **启动driver，而不会来注册driver,就更不可能让master调度driver了
      *
      */

    for (driver <- waitingDrivers.toList) { // iterate over a copy of waitingDrivers
      // We assign workers to each waiting driver in a round-robin fashion. For each driver, we
      // start from the last worker that was assigned a driver, and continue onwards until we have
      // explored all alive workers.
      var launched = false
      var numWorkersVisited = 0
      /**
        * while的条件，numWorkersVisited 小于numWorkersAlive
        * 意思就是说，只要还有活着的worker没有遍历到，那么就继续进行遍历，
        * 还有一个附加条件行，当前这个driver还没有被启动，也就是launched为false
        */

      while (numWorkersVisited < numWorkersAlive && !launched) {
        val worker = shuffledAliveWorkers(curPos)
        numWorkersVisited += 1
        /**
          * 如果当前这个worker的空闲内存大于等于,driver需要的内存
          * 并且worker的空闲cpu数量，大于等于driver需要的cpu数量
          */
        if (worker.memoryFree >= driver.desc.mem && worker.coresFree >= driver.desc.cores) {
          //启动driver,进入该方法
          launchDriver(worker, driver)
          //并且将driver从waittingDrivers列队中移除
          waitingDrivers -= driver
          //将lauched设置为true
          launched = true
        }
        //将指针指向下一个worker
        curPos = (curPos + 1) % numWorkersAlive
      }


    }

    // Right now this is a very simple FIFO scheduler. We keep trying to fit in the first app
    // in the queue, then the second app, etc.
    /**
      * Application的调度机制算法 （极其重要）
      * 首先，Application的调度算法有两种，一种是spreadOutApps,另一种是非spreadOutApps
      */

    if (spreadOutApps) {
      // Try to spread out each app among all the nodes, until it has all its cores
      //首先，遍历waitingApps中的ApplicationInfo,并且过滤出还有需要调度的core的Application
      for (app <- waitingApps if app.coresLeft > 0) {
        //从worker中，过滤出状态为ALIVE的，再次过滤可以被Application使用的 Worker，然后按照剩余CPU数量倒序排序
        //进入canUse(app)方法
        val usableWorkers = workers.toArray.filter(_.state == WorkerState.ALIVE)
          .filter(canUse(app, _)).sortBy(_.coresFree).reverse
        val numUsable = usableWorkers.length
        //创建一个空数组，存储了要分配给每个worker的CPU数量
        val assigned = new Array[Int](numUsable) // Number of cores to give on each node
        //获取到底要分配多少CPU，取APP剩余要分配的CPU的数量和worker总共可用cpu数量的最小值
        var toAssign = math.min(app.coresLeft, usableWorkers.map(_.coresFree).sum)
        var pos = 0
        /**
          * 通过这种算法，其实会将每个application，要启动的executor，都平均分布到各个worker上去
          * 比如有20个CPU core要分配，那么实际会循环两遍worker，每次循环，给每个worker分配1个core
          * 最后每个worker分配两2个core
          */

        //while，条件，只要分配的CPU，还没分配完，就继续循环
        while (toAssign > 0) {
          //每一个worker,如果空闲的CPU数量大于已经分配出去的CPU数量
          //也就是worker还有可分配的
          if (usableWorkers(pos).coresFree - assigned(pos) > 0) {
            //将总共要分配的CPU数量减1，因为这里已经决定再这个worker上分配一个cup了
            toAssign -= 1
            //给这个worker分配的cpu数量加1
            assigned(pos) += 1
          }
          //指针移动到下一个 worker
          pos = (pos + 1) % numUsable
        }
        // Now that we've decided how many cores to give on each node, let's actually give them
        //给每个worker分配完application要求的cpu core 之后
        //遍历worker
        for (pos <- 0 until numUsable) {
          //只要判断之前给这个worker分配到了core
          if (assigned(pos) > 0) {
            /**
              * 首先，再application内部缓存结构中，添加executor
              * 并且创建ExecutorDesc对象，其中封装了，给这个executor启动的内部机制
              * 我们之前说了，在spark-submit脚本中，可以指定要多少executor,每个executor多少个CPU，多少内存
              * 那么基于我们的机制，实际上，最后，executor的实际数量，以及每个executor的CPU，可能与配置是不一样的
              * 因为，我们这里是基于总的CPU来分配的，就是说，比如我们3个executor，每个要3个CPU，那么比如，有9个
              * worker,每个有1个cpu,那么其实总共需要，分配9个core，其实根据这种算法，会给每个worker分配一个core,然后给每个worker启动一个executor
              * 最后，会启动9个executor，每个executor有1个CPU core
              */
            val exec = app.addExecutor(usableWorkers(pos), assigned(pos))
            //在我worker上启动executor,进入该方法
            launchExecutor(usableWorkers(pos), exec)
            //将application的状态设置为RUNNING
            app.state = ApplicationState.RUNNING
          }
        }
      }
    } else {

      //非sparedOutAPPs调度算法
      /**
        * 总结
        * 这种算法和spreaOutApps算法正好相反
        * 每个application,都尽可能分配到尽量少的worker上去，比如总共有10个worker,每个有10个core
        * app总共要分配20个core，那么其实，只会分配到两个worker上，每个worker都占满10个core
        * 那么，其余的app，就只能分配到下一个worker了
        * 比如，application,spark-submit里，配置的是要10个executor，每个要2个core，那么总共是20个core
        * 但是在这种算法下，其实总共只会自动2个executor,每个有10个core
        */
      /**
        * 将每一个Application,尽可能少的分配搭到worker上去
        * 首先，遍历worker，并且是状态为ALIVE,还是空闲CPU的worker
        */
      // Pack each app into as few nodes as possible until we've assigned all its cores
      for (worker <- workers if worker.coresFree > 0 && worker.state == WorkerState.ALIVE) {
        //遍历application,并且是还有需要分配的core的application
        for (app <- waitingApps if app.coresLeft > 0) {
          //判断，如果当前这个worker可用被application使用
          if (canUse(app, worker)) {
            //取worker剩余CPU数量，与app要分配的CPU数据的最小值
            val coresToUse = math.min(worker.coresFree, app.coresLeft)
            //如果，worker剩余CPU数量为0 ，理所当然就不需要分配了
            if (coresToUse > 0) {
              //给app添加一个executor
              val exec = app.addExecutor(worker, coresToUse)
              //在worker上启动executor
              launchExecutor(worker, exec)
              //将application的状态设置为RUNNING
              app.state = ApplicationState.RUNNING
            }
          }
        }
      }
    }
  }

  def launchExecutor(worker: WorkerInfo, exec: ExecutorDesc) {

    logInfo("Launching executor " + exec.fullId + " on worker " + worker.id)
    //将executor加入worker内部缓存
    worker.addExecutor(exec)
    //向worker的actor发送lauchexecutor消息
    worker.actor ! LaunchExecutor(masterUrl,
      exec.application.id, exec.id, exec.application.desc, exec.cores, exec.memory)
    //向executor对应的application的driver，发送executoradded消息
    exec.application.driver ! ExecutorAdded(
      exec.id, worker.id, worker.hostPort, exec.cores, exec.memory)
  }

  def registerWorker(worker: WorkerInfo): Boolean = {
    // There may be one or more refs to dead workers on this same node (w/ different ID's),
    // remove them.
    workers.filter { w =>
      (w.host == worker.host && w.port == worker.port) && (w.state == WorkerState.DEAD)
    }.foreach { w =>
      workers -= w
    }

    val workerAddress = worker.actor.path.address
    if (addressToWorker.contains(workerAddress)) {
      val oldWorker = addressToWorker(workerAddress)
      if (oldWorker.state == WorkerState.UNKNOWN) {
        // A worker registering from UNKNOWN implies that the worker was restarted during recovery.
        // The old worker must thus be dead, so we will remove it and accept the new worker.
        removeWorker(oldWorker)
      } else {
        logInfo("Attempted to re-register worker at same address: " + workerAddress)
        return false
      }
    }
    //将worker的信息加入到内存缓存中（hashset数据结构［去重］）
    workers += worker
    idToWorker(worker.id) = worker
    addressToWorker(workerAddress) = worker
    true
  }

  def removeWorker(worker: WorkerInfo) {
    logInfo("Removing worker " + worker.id + " on " + worker.host + ":" + worker.port)
    worker.setState(WorkerState.DEAD)
    //将worker从内存的缓存结构移除去
    idToWorker -= worker.id
    addressToWorker -= worker.actor.path.address
    for (exec <- worker.executors.values) {
      logInfo("Telling app of lost executor: " + exec.id)
      //告诉driver 该worker已经丢失了
      exec.application.driver ! ExecutorUpdated(
        exec.id, ExecutorState.LOST, Some("worker lost"), None)
      //将被移除的worker上的executor也进行移除
      exec.application.removeExecutor(exec)
    }
    for (driver <- worker.drivers.values) {
      if (driver.desc.supervise) {
        logInfo(s"Re-launching ${driver.id}")
        //如果配置了supervise,master重新启动driver
        relaunchDriver(driver)
      } else {
        logInfo(s"Not re-launching ${driver.id} because it was not supervised")
        removeDriver(driver.id, DriverState.ERROR, None)
      }
    }
    persistenceEngine.removeWorker(worker)
  }

  def relaunchDriver(driver: DriverInfo) {
    driver.worker = None
    driver.state = DriverState.RELAUNCHING
    waitingDrivers += driver
    schedule()
  }

  def createApplication(desc: ApplicationDescription, driver: ActorRef): ApplicationInfo = {
    val now = System.currentTimeMillis()
    val date = new Date(now)
    new ApplicationInfo(now, newApplicationId(date), desc, date, driver, defaultCores)
  }

  def registerApplication(app: ApplicationInfo): Unit = {
    val appAddress = app.driver.path.address
    if (addressToApp.contains(appAddress)) {
      logInfo("Attempted to re-register application at same address: " + appAddress)
      return
    }

    applicationMetricsSystem.registerSource(app.appSource)
    //这里，将app的信息加入到内存缓存中（hashset数据结构［去重］）
    apps += app
    idToApp(app.id) = app
    actorToApp(app.driver) = app
    addressToApp(appAddress) = app
    //将app加入等待调度的队列－waitingApps［ArrayBuffer］
    waitingApps += app
  }

  def finishApplication(app: ApplicationInfo) {
    removeApplication(app, ApplicationState.FINISHED)
  }

  def removeApplication(app: ApplicationInfo, state: ApplicationState.Value) {
    if (apps.contains(app)) {
      logInfo("Removing app " + app.id)
      apps -= app
      idToApp -= app.id
      actorToApp -= app.driver
      addressToApp -= app.driver.path.address
      if (completedApps.size >= RETAINED_APPLICATIONS) {
        val toRemove = math.max(RETAINED_APPLICATIONS / 10, 1)
        completedApps.take(toRemove).foreach( a => {
          appIdToUI.remove(a.id).foreach { ui => webUi.detachSparkUI(ui) }
          applicationMetricsSystem.removeSource(a.appSource)
        })
        completedApps.trimStart(toRemove)
      }
      completedApps += app // Remember it in our history
      waitingApps -= app

      // If application events are logged, use them to rebuild the UI
      rebuildSparkUI(app)

      for (exec <- app.executors.values) {
        exec.worker.removeExecutor(exec)
        exec.worker.actor ! KillExecutor(masterUrl, exec.application.id, exec.id)
        exec.state = ExecutorState.KILLED
      }
      app.markFinished(state)
      if (state != ApplicationState.FINISHED) {
        app.driver ! ApplicationRemoved(state.toString)
      }
      persistenceEngine.removeApplication(app)
      schedule()

      // Tell all workers that the application has finished, so they can clean up any app state.
      workers.foreach { w =>
        w.actor ! ApplicationFinished(app.id)
      }
    }
  }

  /**
   * Rebuild a new SparkUI from the given application's event logs.
   * Return whether this is successful.
   */
  def rebuildSparkUI(app: ApplicationInfo): Boolean = {
    val appName = app.desc.name
    val notFoundBasePath = HistoryServer.UI_PATH_PREFIX + "/not-found"
    try {
      val eventLogFile = app.desc.eventLogDir
        .map { dir => EventLoggingListener.getLogPath(dir, app.id, app.desc.eventLogCodec) }
        .getOrElse {
          // Event logging is not enabled for this application
          app.desc.appUiUrl = notFoundBasePath
          return false
        }

      val fs = Utils.getHadoopFileSystem(eventLogFile, hadoopConf)

      if (fs.exists(new Path(eventLogFile + EventLoggingListener.IN_PROGRESS))) {
        // Event logging is enabled for this application, but the application is still in progress
        val title = s"Application history not found (${app.id})"
        var msg = s"Application $appName is still in progress."
        logWarning(msg)
        msg = URLEncoder.encode(msg, "UTF-8")
        app.desc.appUiUrl = notFoundBasePath + s"?msg=$msg&title=$title"
        return false
      }

      val logInput = EventLoggingListener.openEventLog(new Path(eventLogFile), fs)
      val replayBus = new ReplayListenerBus()
      val ui = SparkUI.createHistoryUI(new SparkConf, replayBus, new SecurityManager(conf),
        appName + " (completed)", HistoryServer.UI_PATH_PREFIX + s"/${app.id}")
      try {
        replayBus.replay(logInput, eventLogFile)
      } finally {
        logInput.close()
      }
      appIdToUI(app.id) = ui
      webUi.attachSparkUI(ui)
      // Application UI is successfully rebuilt, so link the Master UI to it
      app.desc.appUiUrl = ui.basePath
      true
    } catch {
      case fnf: FileNotFoundException =>
        // Event logging is enabled for this application, but no event logs are found
        val title = s"Application history not found (${app.id})"
        var msg = s"No event logs found for application $appName in ${app.desc.eventLogDir}."
        logWarning(msg)
        msg += " Did you specify the correct logging directory?"
        msg = URLEncoder.encode(msg, "UTF-8")
        app.desc.appUiUrl = notFoundBasePath + s"?msg=$msg&title=$title"
        false
      case e: Exception =>
        // Relay exception message to application UI page
        val title = s"Application history load error (${app.id})"
        val exception = URLEncoder.encode(Utils.exceptionString(e), "UTF-8")
        var msg = s"Exception in replaying log for application $appName!"
        logError(msg, e)
        msg = URLEncoder.encode(msg, "UTF-8")
        app.desc.appUiUrl = notFoundBasePath + s"?msg=$msg&exception=$exception&title=$title"
        false
    }
  }

  /** Generate a new app ID given a app's submission date */
  def newApplicationId(submitDate: Date): String = {
    val appId = "app-%s-%04d".format(createDateFormat.format(submitDate), nextAppNumber)
    nextAppNumber += 1
    appId
  }

  /** Check for, and remove, any timed-out workers */
  def timeOutDeadWorkers() {
    // Copy the workers into an array so we don't modify the hashset while iterating through it
    val currentTime = System.currentTimeMillis()
    val toRemove = workers.filter(_.lastHeartbeat < currentTime - WORKER_TIMEOUT).toArray
    for (worker <- toRemove) {
      if (worker.state != WorkerState.DEAD) {
        logWarning("Removing %s because we got no heartbeat in %d seconds".format(
          worker.id, WORKER_TIMEOUT/1000))
        removeWorker(worker)
      } else {
        if (worker.lastHeartbeat < currentTime - ((REAPER_ITERATIONS + 1) * WORKER_TIMEOUT)) {
          workers -= worker // we've seen this DEAD worker in the UI, etc. for long enough; cull it
        }
      }
    }
  }

  def newDriverId(submitDate: Date): String = {
    val appId = "driver-%s-%04d".format(createDateFormat.format(submitDate), nextDriverNumber)
    nextDriverNumber += 1
    appId
  }

  def createDriver(desc: DriverDescription): DriverInfo = {
    val now = System.currentTimeMillis()
    val date = new Date(now)
    new DriverInfo(now, newDriverId(date), desc, date)
  }

  /**
    *
    * @param worker
    * @param driver
    * 再某一个worker上，启动driver
    */
  def launchDriver(worker: WorkerInfo, driver: DriverInfo) {
    logInfo("Launching driver " + driver.id + " on worker " + worker.id)
    /**
      * 将driver加入worker内存的缓存结构
      * 将worker内使用的内存和CPU数量，都加上driver需要的内存和CPU数量
      */

    worker.addDriver(driver)
    //同时把worker也加入到driver内部的缓存结构中
    driver.worker = Some(worker)
    //然后调用worker的actor，给它发送LaunchDriver消息，让worker来启动driver
    worker.actor ! LaunchDriver(driver.id, driver.desc)
    //将driver的状态设置为RUNNING
    driver.state = DriverState.RUNNING
  }

  def removeDriver(driverId: String, finalState: DriverState, exception: Option[Exception]) {
    //用scala的find()高阶函数，找到driverId对应的driver
    drivers.find(d => d.id == driverId) match {
        //如果找到了，Some,样列类(Option)
      case Some(driver) =>
        logInfo(s"Removing driver: $driverId")
        //将driver从内存缓存中移除
        drivers -= driver
        if (completedDrivers.size >= RETAINED_DRIVERS) {
          val toRemove = math.max(RETAINED_DRIVERS / 10, 1)
          completedDrivers.trimStart(toRemove)
        }
        //向completedDrivers加入driver
        completedDrivers += driver
        //使用持久化引擎去除driver的持久化信息
        persistenceEngine.removeDriver(driver)
        //设置driver的state ,execption
        driver.state = finalState
        driver.exception = exception
        //将driver所在的worker移除driver
        driver.worker.foreach(w => w.removeDriver(driver))
        //同样调用schedule()方法
        schedule()
      case None =>
        logWarning(s"Asked to remove unknown driver: $driverId")
    }
  }
}

private[spark] object Master extends Logging {
  val systemName = "sparkMaster"
  private val actorName = "Master"

  def main(argStrings: Array[String]) {
    SignalLogger.register(log)
    val conf = new SparkConf
    val args = new MasterArguments(argStrings, conf)
    val (actorSystem, _, _, _) = startSystemAndActor(args.host, args.port, args.webUiPort, conf)
    actorSystem.awaitTermination()
  }

  /**
   * Returns an `akka.tcp://...` URL for the Master actor given a sparkUrl `spark://host:port`.
   *
   * @throws SparkException if the url is invalid
   */
  def toAkkaUrl(sparkUrl: String, protocol: String): String = {
    val (host, port) = Utils.extractHostPortFromSparkUrl(sparkUrl)
    AkkaUtils.address(protocol, systemName, host, port, actorName)
  }

  /**
   * Returns an akka `Address` for the Master actor given a sparkUrl `spark://host:port`.
   *
   * @throws SparkException if the url is invalid
   */
  def toAkkaAddress(sparkUrl: String, protocol: String): Address = {
    val (host, port) = Utils.extractHostPortFromSparkUrl(sparkUrl)
    Address(protocol, systemName, host, port)
  }

  /**
   * Start the Master and return a four tuple of:
   *   (1) The Master actor system
   *   (2) The bound port
   *   (3) The web UI bound port
   *   (4) The REST server bound port, if any
   */
  def startSystemAndActor(
      host: String,
      port: Int,
      webUiPort: Int,
      conf: SparkConf): (ActorSystem, Int, Int, Option[Int]) = {
    val securityMgr = new SecurityManager(conf)
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem(systemName, host, port, conf = conf,
      securityManager = securityMgr)
    val actor = actorSystem.actorOf(
      Props(classOf[Master], host, boundPort, webUiPort, securityMgr, conf), actorName)
    val timeout = AkkaUtils.askTimeout(conf)
    val portsRequest = actor.ask(BoundPortsRequest)(timeout)
    val portsResponse = Await.result(portsRequest, timeout).asInstanceOf[BoundPortsResponse]
    (actorSystem, boundPort, portsResponse.webUIPort, portsResponse.restPort)
  }
}
