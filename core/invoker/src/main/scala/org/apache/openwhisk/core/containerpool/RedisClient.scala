package org.apache.openwhisk.core.containerpool

import java.time.Instant
import java.net.InetAddress
import org.apache.openwhisk.common._
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

import scala.collection.mutable
import scala.collection.immutable
import akka.actor.ActorRef
import org.apache.openwhisk.core.entity.InvokerInstanceId


import org.json4s._
import org.json4s.native.Serialization
import org.json4s.native.Serialization.write
import scala.util.Try



class RedisClient(
                   host: String = "127.0.0.1",  //改成 controller 节点的 ip 地址
                   port: Int = 6379,
                   password: String = "openwhisk",
                   database: Int = 0,
                   logging: Logging
                 ) {
  private var pool: JedisPool = _
  implicit val formats = Serialization.formats(NoTypeHints)
  val interval: Int = 1000 //ms

  def init: Unit = {
    val maxTotal: Int = 300
    val maxIdle: Int = 100
    val minIdle: Int = 1
    val timeout: Int = 30000

    val poolConfig = new GenericObjectPoolConfig()
    poolConfig.setMaxTotal(maxTotal)
    poolConfig.setMaxIdle(maxIdle)
    poolConfig.setMinIdle(minIdle)
    pool = new JedisPool(poolConfig, host, port, timeout, password, database)
    println("start the redis pool")
  }


  def getPool: JedisPool = {
    assert(pool != null)
    pool
  }


  // Store the entire preloadTable in Redis
  def storePreloadTable(invokerId: String, preloadTable: mutable.Map[ContainerId, List[ModelData]]): Unit = {
    Try {
      val jedis = pool.getResource
      val name: String = "preloadTable"

      // Serialize the entire preloadTable to a JSON string
      val preloadTableJson = write(preloadTable.map { case (containerId, models) =>
        containerId.asString -> models
      })

      // Store the serialized JSON in Redis
      jedis.hset(name, invokerId, preloadTableJson)
      logging.info(this, s"Stored entire preloadTable for invokerId $invokerId")

      jedis.close()
    }.recover {
      case e: Exception =>
        logging.error(this, s"Error storing preloadTable for invokerId $invokerId: ${e.getMessage}")
    }
  }




  //存储pre-loaded function
  def storeActionNames(invokerId:String, preloadTable: mutable.Map[ContainerId, List[ModelData]]): Unit = {
    try {
      val jedis = pool.getResource
      val name: String = "preLoadedAction"

      // 获取所有的modelDataList
      val allModelDataList = preloadTable.values.flatten.toList

      // 提取出所有的actionName并去重
      val allActionNames = allModelDataList.map(_.actionName).distinct

      // 将所有的actionName连接成一个字符串
      val allActionNamesString = allActionNames.mkString(",")

      // 在Redis中存储所有的actionName
      jedis.hset(name, invokerId, allActionNamesString)
      logging.info(this, s"redis set invokerId ${invokerId},action name: ${allActionNamesString}")

      jedis.close()
    } catch {
      case e: Exception => {
        logging.error(this, s"store action names error, exception ${e}, at ${Instant.now.toEpochMilli}")
      }
    }
  }



  //存储BusyPoolSize
  def storeBusyPoolSize(invokerId:String,busyPool: immutable.Map[ActorRef, ContainerData]): Unit = {
    try {
      val jedis = pool.getResource
      val name: String = "busyPoolSize"

      // 获取busyPool的大小
      val busyPoolSize = busyPool.size

      // 在Redis中存储busyPool的大小
      jedis.hset(name, invokerId, busyPoolSize.toString)

      jedis.close()
    } catch {
      case e: Exception => {
        logging.error(this, s"store busyPool size error, exception ${e}, at ${Instant.now.toEpochMilli}")
      }
    }
  }


  //invokerId
  def storeInvokerId(invokerId: InvokerInstanceId): Unit = {
    try {
      val jedis = pool.getResource
      val name: String = "invokerId"

      // 获取本机的IP地址
      val hostId = InetAddress.getLocalHost.getHostAddress

      // 在Redis中存储busyPool的大小
      jedis.hset(name, hostId, invokerId.toString)
      jedis.close()

    } catch {
      case e: Exception => {
        logging.error(this, s"store busyPool size error, exception ${e}, at ${Instant.now.toEpochMilli}")
      }
    }
  }


  //得到actionName
  def getActionNames(hostId: String, name: String): List[String] = {
    try {
      val jedis = pool.getResource

      // 从Redis中获取所有的actionName字符串
      val allActionNamesString = jedis.hget(name, hostId)

      jedis.close()

      // 将所有的actionName字符串分割成列表
      val allActionNames = allActionNamesString.split(",").toList

      allActionNames
    } catch {
      case e: Exception => {
        logging.error(this, s"get action names error, exception ${e}, at ${Instant.now.toEpochMilli}")
        List()
      }
    }
  }



  //得到busyPool.size
  def getBusyPoolSize(hostId: String, name: String): Int = {
    try {
      val jedis = pool.getResource

      // 从 Redis 中获取 busyPool.size 字符串
      val busyPoolSizeString = jedis.hget(name, hostId)

      jedis.close()

      // 将 busyPool.size 字符串转换为整数
      val busyPoolSize = busyPoolSizeString.toInt

      busyPoolSize
    } catch {
      case e: Exception => {
        logging.error(this, s"get busyPool size error, exception ${e}, at ${Instant.now.toEpochMilli}")
        0
      }
    }
  }





}
