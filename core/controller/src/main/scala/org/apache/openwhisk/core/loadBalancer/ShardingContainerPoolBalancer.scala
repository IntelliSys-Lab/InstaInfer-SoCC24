/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.openwhisk.core.loadBalancer

import akka.actor.ActorRef
import akka.actor.ActorRefFactory

import java.util.concurrent.ThreadLocalRandom
import akka.actor.{Actor, ActorSystem, Cancellable, Props}
import akka.cluster.ClusterEvent._
import akka.cluster.{Cluster, Member, MemberStatus}
import akka.management.scaladsl.AkkaManagement
import akka.management.cluster.bootstrap.ClusterBootstrap
import org.apache.openwhisk.common.InvokerState.{Healthy, Offline, Unhealthy, Unresponsive}
import pureconfig._
import org.apache.openwhisk.common._
import org.apache.openwhisk.core.WhiskConfig._
import org.apache.openwhisk.core.connector._
import org.apache.openwhisk.core.entity._
import org.apache.openwhisk.core.entity.size.SizeLong
import org.apache.openwhisk.common.LoggingMarkers.{_}
import org.apache.openwhisk.core.containerpool.RedisClient
import org.apache.openwhisk.core.controller.Controller
import org.apache.openwhisk.core.{ConfigKeys, WhiskConfig}
import org.apache.openwhisk.spi.SpiLoader
import pureconfig.generic.auto._

import scala.annotation.tailrec
import scala.collection.convert.ImplicitConversions.`collection asJava`
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters.mapAsScalaMapConverter



/**
 * A loadbalancer that schedules workload based on a hashing-algorithm.
 *
 * ## Algorithm
 *
 * At first, for every namespace + action pair a hash is calculated and then an invoker is picked based on that hash
 * (`hash % numInvokers`). The determined index is the so called "home-invoker". This is the invoker where the following
 * progression will **always** start. If this invoker is healthy (see "Invoker health checking") and if there is
 * capacity on that invoker (see "Capacity checking"), the request is scheduled to it.
 *
 * If one of these prerequisites is not true, the index is incremented by a step-size. The step-sizes available are the
 * all coprime numbers smaller than the amount of invokers available (coprime, to minimize collisions while progressing
 * through the invokers). The step-size is picked by the same hash calculated above (`hash & numStepSizes`). The
 * home-invoker-index is now incremented by the step-size and the checks (healthy + capacity) are done on the invoker
 * we land on now.
 *
 * This procedure is repeated until all invokers have been checked at which point the "overload" strategy will be
 * employed, which is to choose a healthy invoker randomly. In a steadily running system, that overload means that there
 * is no capacity on any invoker left to schedule the current request to.
 *
 * If no invokers are available or if there are no healthy invokers in the system, the loadbalancer will return an error
 * stating that no invokers are available to take any work. Requests are not queued anywhere in this case.
 *
 * An example:
 * - availableInvokers: 10 (all healthy)
 * - hash: 13
 * - homeInvoker: hash % availableInvokers = 13 % 10 = 3
 * - stepSizes: 1, 3, 7 (note how 2 and 5 is not part of this because it's not coprime to 10)
 * - stepSizeIndex: hash % numStepSizes = 13 % 3 = 1 => stepSize = 3
 *
 * Progression to check the invokers: 3, 6, 9, 2, 5, 8, 1, 4, 7, 0 --> done
 *
 * This heuristic is based on the assumption, that the chance to get a warm container is the best on the home invoker
 * and degrades the more steps you make. The hashing makes sure that all loadbalancers in a cluster will always pick the
 * same home invoker and do the same progression for a given action.
 *
 * Known caveats:
 * - This assumption is not always true. For instance, two heavy workloads landing on the same invoker can override each
 *   other, which results in many cold starts due to all containers being evicted by the invoker to make space for the
 *   "other" workload respectively. Future work could be to keep a buffer of invokers last scheduled for each action and
 *   to prefer to pick that one. Then the second-last one and so forth.
 *
 * ## Capacity checking
 *
 * The maximum capacity per invoker is configured using `user-memory`, which is the maximum amount of memory of actions
 * running in parallel on that invoker.
 *
 * Spare capacity is determined by what the loadbalancer thinks it scheduled to each invoker. Upon scheduling, an entry
 * is made to update the books and a slot for each MB of the actions memory limit in a Semaphore is taken. These slots
 * are only released after the response from the invoker (active-ack) arrives **or** after the active-ack times out.
 * The Semaphore has as many slots as MBs are configured in `user-memory`.
 *
 * Known caveats:
 * - In an overload scenario, activations are queued directly to the invokers, which makes the active-ack timeout
 *   unpredictable. Timing out active-acks in that case can cause the loadbalancer to prematurely assign new load to an
 *   overloaded invoker, which can cause uneven queues.
 * - The same is true if an invoker is extraordinarily slow in processing activations. The queue on this invoker will
 *   slowly rise if it gets slow to the point of still sending pings, but handling the load so slowly, that the
 *   active-acks time out. The loadbalancer again will think there is capacity, when there is none.
 *
 * Both caveats could be solved in future work by not queueing to invoker topics on overload, but to queue on a
 * centralized overflow topic. Timing out an active-ack can then be seen as a system-error, as described in the
 * following.
 *
 * ## Invoker health checking
 *
 * Invoker health is determined via a kafka-based protocol, where each invoker pings the loadbalancer every second. If
 * no ping is seen for a defined amount of time, the invoker is considered "Offline".
 *
 * Moreover, results from all activations are inspected. If more than 3 out of the last 10 activations contained system
 * errors, the invoker is considered "Unhealthy". If an invoker is unhealthy, no user workload is sent to it, but
 * test-actions are sent by the loadbalancer to check if system errors are still happening. If the
 * system-error-threshold-count in the last 10 activations falls below 3, the invoker is considered "Healthy" again.
 *
 * To summarize:
 * - "Offline": Ping missing for > 10 seconds
 * - "Unhealthy": > 3 **system-errors** in the last 10 activations, pings arriving as usual
 * - "Healthy": < 3 **system-errors** in the last 10 activations, pings arriving as usual
 *
 * ## Horizontal sharding
 *
 * Sharding is employed to avoid both loadbalancers having to share any data, because the metrics used in scheduling
 * are very fast changing.
 *
 * Horizontal sharding means, that each invoker's capacity is evenly divided between the loadbalancers. If an invoker
 * has at most 16 slots available (invoker-busy-threshold = 16), those will be divided to 8 slots for each loadbalancer
 * (if there are 2).
 *
 * If concurrent activation processing is enabled (and concurrency limit is > 1), accounting of containers and
 * concurrency capacity per container will limit the number of concurrent activations routed to the particular
 * slot at an invoker. Default max concurrency is 1.
 *
 * Known caveats:
 * - If a loadbalancer leaves or joins the cluster, all state is removed and created from scratch. Those events should
 *   not happen often.
 * - If concurrent activation processing is enabled, it only accounts for the containers that the current loadbalancer knows.
 *   So the actual number of containers launched at the invoker may be less than is counted at the loadbalancer, since
 *   the invoker may skip container launch in case there is concurrent capacity available for a container launched via
 *   some other loadbalancer.
 */

object histogram{
  //初始化histogram
  val NumberofActions = 1000
  val Bounds = 240 //histogram的上界是4小时（240分钟）
  var histogram = new Array[ArrayBuffer[Int]](NumberofActions) //共有1000个action
  var actionName = new ArrayBuffer[String](NumberofActions) //共有1000个action
  var idleTime = new ArrayBuffer[Int](NumberofActions) //每个action对应一个元素，用于记录上一次被调用的时刻
  var idleTimeTotalNumber = new ArrayBuffer[Int](NumberofActions) //每个action对应一个元素，用于该action被调用的总次数
  var Window = scala.collection.mutable.Map("ActionrName" -> (0,10,0,10,0.toDouble)) //保存每个action的4个window

  //增加一个array deltaT，用来统计泊松分布所需的：每次的间隔时间 /delta t_i
  var deltaT = new Array[ArrayBuffer[Int]](NumberofActions) //共有1000个action
  var PossionPara = scala.collection.mutable.Map("ActionrName" -> (0.toDouble,10)) //保存每个action的泊松分布参数：lambda, i

  //0号位元素不使用，仅占用
  actionName.append("StartRecordingAction")
  idleTime.append((System.currentTimeMillis()/60000).toInt)
  idleTimeTotalNumber.append(0)

  //初始化histogram：共有1000个arraybuffer，每个buffer含有240个元素（对应IT从0~239）
  def initializeHistogram(): Unit = {
    if (histogram(1)==null) {
      for (i <- 0 until histogram.length) {
        histogram(i) = new ArrayBuffer[Int]()
        for (j <- 0 until Bounds) {
          histogram(i).append(0)
        }}}}

  //初始化Possion Process：共有1000个arraybuffer，每个buffer含有240个元素（对应IT从0~239）
  def initializePossionProcess(): Unit = {
    if (deltaT(1) == null) {
      for (i <- 0 until deltaT.length) {
        deltaT(i) = new ArrayBuffer[Int]()
        for (j <- 0 until Bounds) {
          deltaT(i).append(0)
        }}}}

  def factorial(n: Int): Int = {
    if (n == 0) 1 else n * factorial(n - 1)
  }



  //Histogram Policy
  def HistogramPolicy(actionMetaData: ExecutableWhiskActionMetaData): (Int, Int, Int, Int,Double) = {
    println("This is action:  " + actionMetaData.fullyQualifiedName(false) + "     's histogram（New policy 3.6）")
    val actionType = RecordNewAction(actionMetaData)
    var idletime = (System.currentTimeMillis()/60000).toInt - idleTime(actionType) //记录本次IT并更新
    println("idleTime: " + idletime)
    if(idletime<Bounds){
      idleTime(actionType) = (System.currentTimeMillis() / 60000).toInt

      //初始化pre-warm和keep-alive window
      var preWarmWindow = 100000
      var keepAliveWindow = 0
      var preLoadWindow = 100000
      var offLoadWindow = 0
      var Lambda = 0.toDouble


      //下面记录histogram二维数组的第actionType个arraybuffer，作为该action的histogram。
      histogram(actionType)(idletime) = histogram(actionType)(idletime) + 1
      idleTimeTotalNumber(actionType) = idleTimeTotalNumber(actionType) + 1 //该action总被调用次数+1

      //idleTime就是delta t，所以直接记录即可.同时可以统计，这是action的第几次调用，得到i [i = idleTimeTotalNumber(actionType)]
      deltaT(actionType)(idleTimeTotalNumber(actionType)) = idletime  //如果是第一次调用，那i=1,此时delta t =0,所以第一个元素的值一定是0


      // ...
      //省略一部分代码
//      for (i <- 0 until idleTimeTotalNumber(actionType)) {
//        sum = sum + deltaT(actionType)(i)
//      }
//      if (sum>0) {
//        lambda = (idleTimeTotalNumber(actionType) / sum).toDouble
//      }
//      println("PossionPara: " + lambda + " ;" + idleTimeTotalNumber(actionType))
//      PossionPara(actionMetaData.fullyQualifiedName(false).toString) = (lambda, idleTimeTotalNumber(actionType))

      //下面计算pre-warn和keep-alive window
      if (idleTimeTotalNumber(actionType) <= 5) { //次数<=10时，使用第二种policy(a standard keep-alive approach):
        //pre-warming window = 0; keep-alive window = range of the histogram
        preWarmWindow = 0
        for (i <- 0 until histogram(actionType).length) {
          if (histogram(actionType)(i) > 0) {
            keepAliveWindow = i
          }
        }
        keepAliveWindow = 10
        preLoadWindow = 0.toInt
        offLoadWindow = 10.toInt
        Lambda = 0.toDouble
      }
      if (idleTimeTotalNumber(actionType) > 5) { //次数>10时，使用第一种policy(Range-limited histogram):
        //pre-warming window = 5%*总数; keep-alive window = 99%*总数
        var preWarmFlag = (idleTimeTotalNumber(actionType).toInt * 0.2).toInt + 1 //第5%个非0元素，其对应的总数就是pre-warm-window. 注意toInt的使用！不然会有小数
        var flagPre = 0
        var flagPrePrevious = 0

        var keepAliveFlag = (idleTimeTotalNumber(actionType).toInt * 0.8).toInt - 1 //第99%个非0元素，其对应的总数就是keep-alive-window
        var flagKeep = 0
        var flagKeepPrevious = 0


        for (i <- 0 until histogram(actionType).length) {
          if (histogram(actionType)(i) > 0) {
            flagPrePrevious = flagPre
            flagKeepPrevious = flagKeep
            flagPre = flagPre + histogram(actionType)(i) //每个IT有几个值，就要加几
            flagKeep = flagKeep + histogram(actionType)(i)

            if (flagPre >= preWarmFlag & flagPrePrevious < preWarmFlag) { //找到flagPre第一次超过preWarmFlag（5%分位）的时刻，即
              //上一次还没超过，这一次超过
              preWarmWindow = i
            }
            if (flagKeep >= keepAliveFlag & flagKeepPrevious < keepAliveFlag) { //找到flagKeep第一次超过keepAliveFlag（99%分位）的时刻，即
              //上一次还没超过，这一次超过
              keepAliveWindow = i
            }
          }
        }

        //下面计算lambda
        var sum = 0
        var lambda = 0.toDouble

        val windowSize = 5
        val stepSize = 1
        var minMSE = Double.MaxValue
        var optimalLambda = 0.toDouble
        for (w <- windowSize until idleTimeTotalNumber(actionType) by stepSize) {
          var sum = 0
          for (i <- (idleTimeTotalNumber(actionType) - w) until idleTimeTotalNumber(actionType)) {
            sum = sum + deltaT(actionType)(i)
            println("deltaT: "+deltaT(actionType))
          }
          val lambda = w.toDouble / sum.toDouble

          //predict the arrival situation at the next time point
          val prediction = Math.pow(lambda, w) * Math.exp(-lambda) / factorial(w)

          //compute the prediction error
          val MSE = Math.pow(prediction - deltaT(actionType)(idleTimeTotalNumber(actionType)), 2)

          if (MSE < minMSE) {
            minMSE = MSE
            optimalLambda = lambda
          }
        }

        //println("PossionPara: " + optimalLambda + " ;" + idleTimeTotalNumber(actionType))
        PossionPara(actionMetaData.fullyQualifiedName(false).toString) = (optimalLambda, idleTimeTotalNumber(actionType))

        //依据optimal lambda，得到pre-lod和off-load值。
        // ...
        //省略一部分代码
        //根据最优的lambda计算T1和T2
        val P0: Double = 0.05 //这里赋值为你想要的P0
        val P1: Double = 0.85 //这里赋值为你想要的P1

        val T1: Double = -math.log(1 - P0) / optimalLambda
        val T2: Double = -math.log(1 - P1) / optimalLambda
        preLoadWindow = T1.toInt + 1 //toInt会向下取整
        offLoadWindow = T2.toInt
        Lambda = optimalLambda

        println("T1: " + T1 + ", T2: " + T2)

      }

      if(keepAliveWindow ==0){
        keepAliveWindow = 1
      }

      println("windows: " + preWarmWindow + " ;" + keepAliveWindow)
      Window(actionMetaData.fullyQualifiedName(false).toString) = (preWarmWindow, keepAliveWindow, preLoadWindow, offLoadWindow,Lambda)
      (preWarmWindow, keepAliveWindow, preLoadWindow, offLoadWindow,Lambda)
    }
    else{
      //本来该用ARIMR算法来预测（auto-arimr）
      println("This invocation's idle time is Out-Of-Bound")
      (0,10,0,10,0)
    }

  }

  //记录新出现的Action
  def RecordNewAction(actionMetaData: ExecutableWhiskActionMetaData): Int = {

    var ActionNameRecord = -1
    //判断该action是否是第一次出现
    for (i <- 0 until actionName.length) {
      if (actionName(i) == actionMetaData.fullyQualifiedName(false).toString) { //该action已经被记录在actionName中了
        //记得要toString！fqn的类型不是String
        ActionNameRecord = i
      }
    }

    if (ActionNameRecord == -1) { //说明该action之前没出现过，需要记录在actionName中
      ActionNameRecord = actionName.length //ActionNameRecord表示该action在数组中的位置
      actionName.append(actionMetaData.fullyQualifiedName(false).toString)
      idleTime.append((System.currentTimeMillis()/60000).toInt) //记录该action初次出现的时刻
      println("NewIdleTime："+idleTime)
      idleTimeTotalNumber.append(0) //该action初次被调用，初始化该元素位置上的idleTimeNumber
    }
    ActionNameRecord
  }

}


class ShardingContainerPoolBalancer(
  config: WhiskConfig,
  controllerInstance: ControllerInstanceId,
  feedFactory: FeedFactory,
  val invokerPoolFactory: InvokerPoolFactory,
  implicit val messagingProvider: MessagingProvider = SpiLoader.get[MessagingProvider])(
  implicit actorSystem: ActorSystem,
  logging: Logging)
    extends CommonLoadBalancer(config, feedFactory, controllerInstance) {

  /** Build a cluster of all loadbalancers */
  private val cluster: Option[Cluster] = if (loadConfigOrThrow[ClusterConfig](ConfigKeys.cluster).useClusterBootstrap) {
    AkkaManagement(actorSystem).start()
    ClusterBootstrap(actorSystem).start()
    Some(Cluster(actorSystem))
  } else if (loadConfigOrThrow[Seq[String]]("akka.cluster.seed-nodes").nonEmpty) {
    Some(Cluster(actorSystem))
  } else {
    None
  }

  override protected def emitMetrics() = {
    super.emitMetrics()
    MetricEmitter.emitGaugeMetric(
      INVOKER_TOTALMEM_BLACKBOX,
      schedulingState.blackboxInvokers.foldLeft(0L) { (total, curr) =>
        if (curr.status.isUsable) {
          curr.id.userMemory.toMB + total
        } else {
          total
        }
      })
    MetricEmitter.emitGaugeMetric(
      INVOKER_TOTALMEM_MANAGED,
      schedulingState.managedInvokers.foldLeft(0L) { (total, curr) =>
        if (curr.status.isUsable) {
          curr.id.userMemory.toMB + total
        } else {
          total
        }
      })
    MetricEmitter.emitGaugeMetric(HEALTHY_INVOKER_MANAGED, schedulingState.managedInvokers.count(_.status == Healthy))
    MetricEmitter.emitGaugeMetric(
      UNHEALTHY_INVOKER_MANAGED,
      schedulingState.managedInvokers.count(_.status == Unhealthy))
    MetricEmitter.emitGaugeMetric(
      UNRESPONSIVE_INVOKER_MANAGED,
      schedulingState.managedInvokers.count(_.status == Unresponsive))
    MetricEmitter.emitGaugeMetric(OFFLINE_INVOKER_MANAGED, schedulingState.managedInvokers.count(_.status == Offline))
    MetricEmitter.emitGaugeMetric(HEALTHY_INVOKER_BLACKBOX, schedulingState.blackboxInvokers.count(_.status == Healthy))
    MetricEmitter.emitGaugeMetric(
      UNHEALTHY_INVOKER_BLACKBOX,
      schedulingState.blackboxInvokers.count(_.status == Unhealthy))
    MetricEmitter.emitGaugeMetric(
      UNRESPONSIVE_INVOKER_BLACKBOX,
      schedulingState.blackboxInvokers.count(_.status == Unresponsive))
    MetricEmitter.emitGaugeMetric(OFFLINE_INVOKER_BLACKBOX, schedulingState.blackboxInvokers.count(_.status == Offline))
  }

  /** State needed for scheduling. */
  val schedulingState = ShardingContainerPoolBalancerState()(lbConfig)

  /**
   * Monitors invoker supervision and the cluster to update the state sequentially
   *
   * All state updates should go through this actor to guarantee that
   * [[ShardingContainerPoolBalancerState.updateInvokers]] and [[ShardingContainerPoolBalancerState.updateCluster]]
   * are called exclusive of each other and not concurrently.
   */
  private val monitor = actorSystem.actorOf(Props(new Actor {
    override def preStart(): Unit = {
      cluster.foreach(_.subscribe(self, classOf[MemberEvent], classOf[ReachabilityEvent]))
    }

    // all members of the cluster that are available
    var availableMembers = Set.empty[Member]

    override def receive: Receive = {
      case CurrentInvokerPoolState(newState) =>
        schedulingState.updateInvokers(newState)

      // State of the cluster as it is right now
      case CurrentClusterState(members, _, _, _, _) =>
        availableMembers = members.filter(_.status == MemberStatus.Up)
        schedulingState.updateCluster(availableMembers.size)

      // General lifecycle events and events concerning the reachability of members. Split-brain is not a huge concern
      // in this case as only the invoker-threshold is adjusted according to the perceived cluster-size.
      // Taking the unreachable member out of the cluster from that point-of-view results in a better experience
      // even under split-brain-conditions, as that (in the worst-case) results in premature overloading of invokers vs.
      // going into overflow mode prematurely.
      case event: ClusterDomainEvent =>
        availableMembers = event match {
          case MemberUp(member)          => availableMembers + member
          case ReachableMember(member)   => availableMembers + member
          case MemberRemoved(member, _)  => availableMembers - member
          case UnreachableMember(member) => availableMembers - member
          case _                         => availableMembers
        }

        schedulingState.updateCluster(availableMembers.size)
    }
  }))

  /** Loadbalancer interface methods */
  override def invokerHealth(): Future[IndexedSeq[InvokerHealth]] = Future.successful(schedulingState.invokers)
  override def clusterSize: Int = schedulingState.clusterSize

  /** 1. Publish a message to the loadbalancer */
  override def publish(action: ExecutableWhiskActionMetaData, msg: ActivationMessage)(
    implicit transid: TransactionId): Future[Future[Either[ActivationId, WhiskActivation]]] = {

    val isBlackboxInvocation = action.exec.pull
    val actionType = if (!isBlackboxInvocation) "managed" else "blackbox"


    //初始化histogram
    histogram.initializeHistogram()
    //初始化Possion分布数组deltaT
    histogram.initializePossionProcess()
    //记录并计算action的window
    val paras = histogram.HistogramPolicy(action)
    println("Finish hist")

    //在ActivationMessage中添加window参数
    msg.preWarmParameter = histogram.Window.get(action.fullyQualifiedName(false).toString).get._1
    msg.keepAliveParameter = histogram.Window.get(action.fullyQualifiedName(false).toString).get._2
    msg.preLoadParameter = histogram.Window.get(action.fullyQualifiedName(false).toString).get._3
    msg.offLoadParameter = histogram.Window.get(action.fullyQualifiedName(false).toString).get._4
    msg.possionpara1_lambda = histogram.Window.get(action.fullyQualifiedName(false).toString).get._5

    logging.info(
      this,
      s"ActionName: '${action.name}, PreWarmWindow parameter:  ${msg.preWarmParameter}, " +
        s"KeepAliveWindow:  '${msg.keepAliveParameter}, lambda: '${msg.possionpara1_lambda} " +
        s",preloadWindow:'${msg.preLoadParameter} offloadWindow:'${msg.offLoadParameter}")



    /*
    修改以下代码，完全不需要它的这套logic，直接init redisClient，然后选择有actionName的hostIP，再筛选出一个busyPoolSize最小的，然后把
    hostIp对应的invokerId找到。最后调用下面这套的output，把activationMessage publish到这个invoker上。
    */

    // Setup redis connection
    val redisClient = new RedisClient(logging = logging)
    redisClient.init

    val actionName = action.name.toString



    /*
    1. 遍历preLoadedAction哈希表中所有的<invokerId,allActionNamesString>键值对：
       1.1 如果该invokerId对应的所有action都不包含actionName，则跳过
       1.2 如果该invokerId对应的所有action包含actionName，则继续查找该invokerId对应的busyPoolSize哈希表，得到busyPoolSize
    3. 在所有："包含actionName"的<invokerId,allActionNamesString>键值对中，选择busyPoolSize最小的invokerId，输出invokerId
    4. schedule()之后的代码都可以复用
     */

    var invokerId : String = "unknown"

    /*
    invokerIdsAndHostsTry match {
      case Failure(e) =>
        println(s"Failed to get invokerIdsAndHosts: ${e.getMessage}")

      case Success(invokerIdsAndHosts) =>
        if (invokerIdsAndHosts.isEmpty) {
          println(s"No invokerId found for action $actionName")
        } else {
          val invokerHosts = invokerIdsAndHosts.values.toSet
          val hostBusyPoolSizes = mutable.Map[String, Int]()

          for (hostIp <- invokerHosts) {
            val hostActionNamesTry = Try(redisClient.getActionNames(hostIp, "preLoadedAction"))
            val busyPoolSizeTry = Try(redisClient.getBusyPoolSize(hostIp, "busyPoolSize"))

            (hostActionNamesTry, busyPoolSizeTry) match {
              case (Failure(_), _) | (_, Failure(_)) => // skip this hostIp if either operation failed
              case (Success(hostActionNames), Success(busyPoolSize)) =>
                if (hostActionNames.contains(actionName)) {
                  hostBusyPoolSizes += (hostIp -> busyPoolSize)
                }
            }
          }

          if (hostBusyPoolSizes.isEmpty) {
            println(s"No suitable invoker found for action $actionName")
          } else {
            val hostWithMinBusyPoolSize = hostBusyPoolSizes.minBy(_._2)._1
            val invokerIdStrOpt = invokerIdsAndHosts.find(_._2 == hostWithMinBusyPoolSize).map(_._1)

            invokerIdStrOpt match {
              case Some(invokerIdStr) =>
                invokerId = invokerIdStr

              case None =>
                println(s"No invokerId found for hostIp $hostWithMinBusyPoolSize")
            }
          }
        }
    }
*/
    // 获取Jedis池
    val jedisPool = redisClient.getPool

    // 指定要查找的actionName
    val targetActionName = actionName

    try {
      // 从Jedis池中获取资源
      val jedis = jedisPool.getResource

      // 获取所有的<invokerId, allActionNamesString>键值对
      val allActionNamesMap = jedis.hgetAll("preLoadedAction").asScala

      // 获取所有包含targetActionName的invokerIds
      val qualifiedInvokerIds = allActionNamesMap.filter { case (_, actionNamesString) =>
        actionNamesString.split(",").contains(targetActionName)
      }.keys

      if (qualifiedInvokerIds.nonEmpty) {
        // 获取所有的<invokerId, busyPoolSize>键值对
        val busyPoolSizeMap = jedis.hgetAll("busyPoolSize").asScala

        // 获取所有包含targetActionName的<invokerId, busyPoolSize>键值对
        val qualifiedBusyPoolSizes = busyPoolSizeMap.filter { case (invokerId, _) =>
          qualifiedInvokerIds.contains(invokerId)
        }

        if (qualifiedBusyPoolSizes.nonEmpty) {
          // 选择busyPoolSize最小的invokerId
          val minBusyPoolSizeInvokerId = qualifiedBusyPoolSizes.minBy { case (_, busyPoolSize) =>
            busyPoolSize.toInt
          }._1

          println(s"The invokerId with minimum busyPoolSize is: $minBusyPoolSizeInvokerId")
          invokerId = minBusyPoolSizeInvokerId
        } else {
          println("No invokers found with the target action name and non-zero busy pool size.")
        }
      } else {
        println("No invokers found with the target action name.")
      }

      // 关闭Jedis资源
      jedis.close()
    } catch {
      case e: Exception => {
        println(s"Error occurred during processing: ${e.getMessage}")
      }
    }


    val specificInvokerOption = (schedulingState.managedInvokers ++ schedulingState.blackboxInvokers).find(_.id.toString == invokerId)
    specificInvokerOption match {
      case Some(specificInvoker) =>
        val activationResult = setupActivation(msg, action, specificInvoker.id)
        logging.info(
          this,
          s"ActionName: '${action.name}, has assigned to pre-loaded invoker:  ${activationResult} ,invokerId: ${specificInvoker}" )

        sendActivationToInvoker(messageProducer, msg, specificInvoker.id).map(_ => activationResult)


      //如果没有找到，就还是shardingpool balancer
      case None =>
        logging.info(
          this,
          s"ActionName: '${action.name}, has not assigned to pre-loaded invoker")

        val (invokersToUse, stepSizes) =
          if (!isBlackboxInvocation) (schedulingState.managedInvokers, schedulingState.managedStepSizes)
          else (schedulingState.blackboxInvokers, schedulingState.blackboxStepSizes)
        val chosen = if (invokersToUse.nonEmpty) {
          val hash = ShardingContainerPoolBalancer.generateHash(msg.user.namespace.name, action.fullyQualifiedName(false))
          val homeInvoker = hash % invokersToUse.size
          val stepSize = stepSizes(hash % stepSizes.size)
          val invoker: Option[(InvokerInstanceId, Boolean)] = ShardingContainerPoolBalancer.schedule(
            action.limits.concurrency.maxConcurrent,
            action.fullyQualifiedName(true),
            invokersToUse,
            schedulingState.invokerSlots,
            action.limits.memory.megabytes,
            homeInvoker,
            stepSize)
          invoker.foreach {
            case (_, true) =>
              val metric =
                if (isBlackboxInvocation)
                  LoggingMarkers.BLACKBOX_SYSTEM_OVERLOAD
                else
                  LoggingMarkers.MANAGED_SYSTEM_OVERLOAD
              MetricEmitter.emitCounterMetric(metric)
            case _ =>
          }
          invoker.map(_._1)
        } else {
          None
        }


        chosen
          .map { invoker =>
            // MemoryLimit() and TimeLimit() return singletons - they should be fast enough to be used here
            logging.info(this,s"default LB's invoker is ${invoker}")

            val memoryLimit = action.limits.memory
            val memoryLimitInfo = if (memoryLimit == MemoryLimit()) {
              "std"
            } else {
              "non-std"
            }
            val timeLimit = action.limits.timeout
            val timeLimitInfo = if (timeLimit == TimeLimit()) {
              "std"
            } else {
              "non-std"
            }
            logging.info(
              this,
              s"scheduled activation ${msg.activationId}, action '${msg.action.asString}' ($actionType), ns '${msg.user.namespace.name.asString}', mem limit ${memoryLimit.megabytes} MB (${memoryLimitInfo}), time limit ${timeLimit.duration.toMillis} ms (${timeLimitInfo}) to ${invoker}")
            val activationResult = setupActivation(msg, action, invoker)
            sendActivationToInvoker(messageProducer, msg, invoker).map(_ => activationResult)
          }
          .getOrElse {
            // report the state of all invokers
            val invokerStates = invokersToUse.foldLeft(Map.empty[InvokerState, Int]) { (agg, curr) =>
              val count = agg.getOrElse(curr.status, 0) + 1
              agg + (curr.status -> count)
            }

            logging.error(
              this,
              s"failed to schedule activation ${msg.activationId}, action '${msg.action.asString}' ($actionType), ns '${msg.user.namespace.name.asString}' - invokers to use: $invokerStates")
            Future.failed(LoadBalancerException("No invokers available"))
          }
    }
  }

  override val invokerPool =
    invokerPoolFactory.createInvokerPool(
      actorSystem,
      messagingProvider,
      messageProducer,
      sendActivationToInvoker,
      Some(monitor))

  override protected def releaseInvoker(invoker: InvokerInstanceId, entry: ActivationEntry) = {
    schedulingState.invokerSlots
      .lift(invoker.toInt)
      .foreach(_.releaseConcurrent(entry.fullyQualifiedEntityName, entry.maxConcurrent, entry.memoryLimit.toMB.toInt))
  }
}

object ShardingContainerPoolBalancer extends LoadBalancerProvider {

  override def instance(whiskConfig: WhiskConfig, instance: ControllerInstanceId)(implicit actorSystem: ActorSystem,
                                                                                  logging: Logging): LoadBalancer = {

    val invokerPoolFactory = new InvokerPoolFactory {
      override def createInvokerPool(
        actorRefFactory: ActorRefFactory,
        messagingProvider: MessagingProvider,
        messagingProducer: MessageProducer,
        sendActivationToInvoker: (MessageProducer, ActivationMessage, InvokerInstanceId) => Future[ResultMetadata],
        monitor: Option[ActorRef]): ActorRef = {

        InvokerPool.prepare(instance, WhiskEntityStore.datastore())

        actorRefFactory.actorOf(
          InvokerPool.props(
            (f, i) => f.actorOf(InvokerActor.props(i, instance)),
            (m, i) => sendActivationToInvoker(messagingProducer, m, i),
            messagingProvider.getConsumer(
              whiskConfig,
              s"${Controller.topicPrefix}health${instance.asString}",
              s"${Controller.topicPrefix}health",
              maxPeek = 128),
            monitor))
      }

    }
    new ShardingContainerPoolBalancer(
      whiskConfig,
      instance,
      createFeedFactory(whiskConfig, instance),
      invokerPoolFactory)
  }

  def requiredProperties: Map[String, String] = kafkaHosts

  /** Generates a hash based on the string representation of namespace and action */
  def generateHash(namespace: EntityName, action: FullyQualifiedEntityName): Int = {
    (namespace.asString.hashCode() ^ action.asString.hashCode()).abs
  }

  /** Euclidean algorithm to determine the greatest-common-divisor */
  @tailrec
  def gcd(a: Int, b: Int): Int = if (b == 0) a else gcd(b, a % b)

  /** Returns pairwise coprime numbers until x. Result is memoized. */
  def pairwiseCoprimeNumbersUntil(x: Int): IndexedSeq[Int] =
    (1 to x).foldLeft(IndexedSeq.empty[Int])((primes, cur) => {
      if (gcd(cur, x) == 1 && primes.forall(i => gcd(i, cur) == 1)) {
        primes :+ cur
      } else primes
    })

  /**
   * Scans through all invokers and searches for an invoker tries to get a free slot on an invoker. If no slot can be
   * obtained, randomly picks a healthy invoker.
   *
   * @param maxConcurrent concurrency limit supported by this action
   * @param invokers a list of available invokers to search in, including their state
   * @param dispatched semaphores for each invoker to give the slots away from
   * @param slots Number of slots, that need to be acquired (e.g. memory in MB)
   * @param index the index to start from (initially should be the "homeInvoker"
   * @param step stable identifier of the entity to be scheduled
   * @return an invoker to schedule to or None of no invoker is available
   */
  @tailrec
  def schedule(
    maxConcurrent: Int,
    fqn: FullyQualifiedEntityName,
    invokers: IndexedSeq[InvokerHealth],
    dispatched: IndexedSeq[NestedSemaphore[FullyQualifiedEntityName]],
    slots: Int,
    index: Int,
    step: Int,
    stepsDone: Int = 0)(implicit logging: Logging, transId: TransactionId): Option[(InvokerInstanceId, Boolean)] = {
    val numInvokers = invokers.size

    if (numInvokers > 0) {
      val invoker = invokers(index)
      //test this invoker - if this action supports concurrency, use the scheduleConcurrent function
      if (invoker.status.isUsable && dispatched(invoker.id.toInt).tryAcquireConcurrent(fqn, maxConcurrent, slots)) {
        Some(invoker.id, false)
      } else {
        // If we've gone through all invokers
        if (stepsDone == numInvokers + 1) {
          val healthyInvokers = invokers.filter(_.status.isUsable)
          if (healthyInvokers.nonEmpty) {
            // Choose a healthy invoker randomly
            val random = healthyInvokers(ThreadLocalRandom.current().nextInt(healthyInvokers.size)).id
            dispatched(random.toInt).forceAcquireConcurrent(fqn, maxConcurrent, slots)
            logging.warn(this, s"system is overloaded. Chose invoker${random.toInt} by random assignment.")
            Some(random, true)
          } else {
            None
          }
        } else {
          val newIndex = (index + step) % numInvokers
          schedule(maxConcurrent, fqn, invokers, dispatched, slots, newIndex, step, stepsDone + 1)
        }
      }
    } else {
      None
    }
  }
}

/**
 * Holds the state necessary for scheduling of actions.
 *
 * @param _invokers all of the known invokers in the system
 * @param _managedInvokers all invokers for managed runtimes
 * @param _blackboxInvokers all invokers for blackbox runtimes
 * @param _managedStepSizes the step-sizes possible for the current managed invoker count
 * @param _blackboxStepSizes the step-sizes possible for the current blackbox invoker count
 * @param _invokerSlots state of accessible slots of each invoker
 */
case class ShardingContainerPoolBalancerState(
  private var _invokers: IndexedSeq[InvokerHealth] = IndexedSeq.empty[InvokerHealth],
  private var _managedInvokers: IndexedSeq[InvokerHealth] = IndexedSeq.empty[InvokerHealth],
  private var _blackboxInvokers: IndexedSeq[InvokerHealth] = IndexedSeq.empty[InvokerHealth],
  private var _managedStepSizes: Seq[Int] = ShardingContainerPoolBalancer.pairwiseCoprimeNumbersUntil(0),
  private var _blackboxStepSizes: Seq[Int] = ShardingContainerPoolBalancer.pairwiseCoprimeNumbersUntil(0),
  var _invokerSlots: IndexedSeq[NestedSemaphore[FullyQualifiedEntityName]] =
    IndexedSeq.empty[NestedSemaphore[FullyQualifiedEntityName]],
  private var _clusterSize: Int = 1)(
  lbConfig: ShardingContainerPoolBalancerConfig =
    loadConfigOrThrow[ShardingContainerPoolBalancerConfig](ConfigKeys.loadbalancer))(implicit logging: Logging) {

  // Managed fraction and blackbox fraction can be between 0.0 and 1.0. The sum of these two fractions has to be between
  // 1.0 and 2.0.
  // If the sum is 1.0 that means, that there is no overlap of blackbox and managed invokers. If the sum is 2.0, that
  // means, that there is no differentiation between managed and blackbox invokers.
  // If the sum is below 1.0 with the initial values from config, the blackbox fraction will be set higher than
  // specified in config and adapted to the managed fraction.
  private val managedFraction: Double = Math.max(0.0, Math.min(1.0, lbConfig.managedFraction))
  private val blackboxFraction: Double = Math.max(1.0 - managedFraction, Math.min(1.0, lbConfig.blackboxFraction))
  logging.info(this, s"managedFraction = $managedFraction, blackboxFraction = $blackboxFraction")(
    TransactionId.loadbalancer)

  /** Getters for the variables, setting from the outside is only allowed through the update methods below */
  def invokers: IndexedSeq[InvokerHealth] = _invokers
  def managedInvokers: IndexedSeq[InvokerHealth] = _managedInvokers
  def blackboxInvokers: IndexedSeq[InvokerHealth] = _blackboxInvokers
  def managedStepSizes: Seq[Int] = _managedStepSizes
  def blackboxStepSizes: Seq[Int] = _blackboxStepSizes
  def invokerSlots: IndexedSeq[NestedSemaphore[FullyQualifiedEntityName]] = _invokerSlots
  def clusterSize: Int = _clusterSize

  /**
   * @param memory
   * @return calculated invoker slot
   */
  private def getInvokerSlot(memory: ByteSize): ByteSize = {
    val invokerShardMemorySize = memory / _clusterSize
    val newTreshold = if (invokerShardMemorySize < MemoryLimit.MIN_MEMORY) {
      logging.error(
        this,
        s"registered controllers: calculated controller's invoker shard memory size falls below the min memory of one action. "
          + s"Setting to min memory. Expect invoker overloads. Cluster size ${_clusterSize}, invoker user memory size ${memory.toMB.MB}, "
          + s"min action memory size ${MemoryLimit.MIN_MEMORY.toMB.MB}, calculated shard size ${invokerShardMemorySize.toMB.MB}.")(
        TransactionId.loadbalancer)
      MemoryLimit.MIN_MEMORY
    } else {
      invokerShardMemorySize
    }
    newTreshold
  }

  /**
   * Updates the scheduling state with the new invokers.
   *
   * This is okay to not happen atomically since dirty reads of the values set are not dangerous. It is important though
   * to update the "invokers" variables last, since they will determine the range of invokers to choose from.
   *
   * Handling a shrinking invokers list is not necessary, because InvokerPool won't shrink its own list but rather
   * report the invoker as "Offline".
   *
   * It is important that this method does not run concurrently to itself and/or to [[updateCluster]]
   */
  def updateInvokers(newInvokers: IndexedSeq[InvokerHealth]): Unit = {
    val oldSize = _invokers.size
    val newSize = newInvokers.size

    // for small N, allow the managed invokers to overlap with blackbox invokers, and
    // further assume that blackbox invokers << managed invokers
    val managed = Math.max(1, Math.ceil(newSize.toDouble * managedFraction).toInt)
    val blackboxes = Math.max(1, Math.floor(newSize.toDouble * blackboxFraction).toInt)

    _invokers = newInvokers
    _managedInvokers = _invokers.take(managed)
    _blackboxInvokers = _invokers.takeRight(blackboxes)

    val logDetail = if (oldSize != newSize) {
      _managedStepSizes = ShardingContainerPoolBalancer.pairwiseCoprimeNumbersUntil(managed)
      _blackboxStepSizes = ShardingContainerPoolBalancer.pairwiseCoprimeNumbersUntil(blackboxes)

      if (oldSize < newSize) {
        // Keeps the existing state..
        val onlyNewInvokers = _invokers.drop(_invokerSlots.length)
        _invokerSlots = _invokerSlots ++ onlyNewInvokers.map { invoker =>
          new NestedSemaphore[FullyQualifiedEntityName](getInvokerSlot(invoker.id.userMemory).toMB.toInt)
        }
        val newInvokerDetails = onlyNewInvokers
          .map(i =>
            s"${i.id.toString}: ${i.status} / ${getInvokerSlot(i.id.userMemory).toMB.MB} of ${i.id.userMemory.toMB.MB}")
          .mkString(", ")
        s"number of known invokers increased: new = $newSize, old = $oldSize. details: $newInvokerDetails."
      } else {
        s"number of known invokers decreased: new = $newSize, old = $oldSize."
      }
    } else {
      s"no update required - number of known invokers unchanged: $newSize."
    }

    logging.info(
      this,
      s"loadbalancer invoker status updated. managedInvokers = $managed blackboxInvokers = $blackboxes. $logDetail")(
      TransactionId.loadbalancer)
  }

  /**
   * Updates the size of a cluster. Throws away all state for simplicity.
   *
   * This is okay to not happen atomically, since a dirty read of the values set are not dangerous. At worst the
   * scheduler works on outdated invoker-load data which is acceptable.
   *
   * It is important that this method does not run concurrently to itself and/or to [[updateInvokers]]
   */
  def updateCluster(newSize: Int): Unit = {
    val actualSize = newSize max 1 // if a cluster size < 1 is reported, falls back to a size of 1 (alone)
    if (_clusterSize != actualSize) {
      val oldSize = _clusterSize
      _clusterSize = actualSize
      _invokerSlots = _invokers.map { invoker =>
        new NestedSemaphore[FullyQualifiedEntityName](getInvokerSlot(invoker.id.userMemory).toMB.toInt)
      }
      // Directly after startup, no invokers have registered yet. This needs to be handled gracefully.
      val invokerCount = _invokers.size
      val totalInvokerMemory =
        _invokers.foldLeft(0L)((total, invoker) => total + getInvokerSlot(invoker.id.userMemory).toMB).MB
      val averageInvokerMemory =
        if (totalInvokerMemory.toMB > 0 && invokerCount > 0) {
          (totalInvokerMemory / invokerCount).toMB.MB
        } else {
          0.MB
        }
      logging.info(
        this,
        s"loadbalancer cluster size changed from $oldSize to $actualSize active nodes. ${invokerCount} invokers with ${averageInvokerMemory} average memory size - total invoker memory ${totalInvokerMemory}.")(
        TransactionId.loadbalancer)
    }
  }
}

/**
 * Configuration for the cluster created between loadbalancers.
 *
 * @param useClusterBootstrap Whether or not to use a bootstrap mechanism
 */
case class ClusterConfig(useClusterBootstrap: Boolean)

/**
 * Configuration for the sharding container pool balancer.
 *
 * @param blackboxFraction the fraction of all invokers to use exclusively for blackboxes
 * @param timeoutFactor factor to influence the timeout period for forced active acks (time-limit.std * timeoutFactor + timeoutAddon)
 * @param timeoutAddon extra time to influence the timeout period for forced active acks (time-limit.std * timeoutFactor + timeoutAddon)
 */
case class ShardingContainerPoolBalancerConfig(managedFraction: Double,
                                               blackboxFraction: Double,
                                               timeoutFactor: Int,
                                               timeoutAddon: FiniteDuration)

/**
 * State kept for each activation slot until completion.
 *
 * @param id id of the activation
 * @param namespaceId namespace that invoked the action
 * @param invokerName invoker the action is scheduled to
 * @param memoryLimit memory limit of the invoked action
 * @param timeLimit time limit of the invoked action
 * @param maxConcurrent concurrency limit of the invoked action
 * @param fullyQualifiedEntityName fully qualified name of the invoked action
 * @param timeoutHandler times out completion of this activation, should be canceled on good paths
 * @param isBlackbox true if the invoked action is a blackbox action, otherwise false (managed action)
 * @param isBlocking true if the action is invoked in a blocking fashion, i.e. "somebody" waits for the result
 * @param controllerId id of the controller that this activation comes from
 */
case class ActivationEntry(id: ActivationId,
                           namespaceId: UUID,
                           invokerName: InvokerInstanceId,
                           memoryLimit: ByteSize,
                           timeLimit: FiniteDuration,
                           maxConcurrent: Int,
                           fullyQualifiedEntityName: FullyQualifiedEntityName,
                           timeoutHandler: Cancellable,
                           isBlackbox: Boolean,
                           isBlocking: Boolean,
                           controllerId: ControllerInstanceId = ControllerInstanceId("0"))
