//package org.apache.openwhisk.core.loadBalancer

/*
//import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.actor.{ActorRef, ActorSystem, Props}
import akka.http.scaladsl.Http
import org.apache.openwhisk.common.TransactionId
import org.apache.openwhisk.core.ConfigKeys
import org.apache.openwhisk.core.connector.MessageFeed
import org.apache.openwhisk.core.etcd.{EtcdClient, EtcdConfig}
import org.scalatest.{Args, ConfigMap, Filter, Status, TestData}
import pureconfig.loadConfigOrThrow

import scala.collection.immutable
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor}
import scala.concurrent.duration.FiniteDuration
 //feedFactory需要的
import akka.actor.ActorRefFactory
import akka.testkit.TestProbe
import org.apache.openwhisk.core.WhiskConfig
import org.apache.openwhisk.core.connector.{MessagingProvider}
import org.apache.openwhisk.core.entity.ExecManifest
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

import pureconfig.generic.auto._

/*
import akka.event.Logging
import akka.kafka.testkit.internal.TestcontainersKafka.Singleton.system
import akka.testkit.TestProbe
import org.apache.openwhisk.core.connector.{MessageFeed, MessagingProvider}
//import org.apache.openwhisk.common.{InvokerHealth, Logging, TransactionId}
import org.apache.openwhisk.core.connector.{MessageConsumer, MessageFeed, MessageProducer}
//import org.apache.openwhisk.core.WhiskConfig
//import org.apache.openwhisk.core.entity.ExecManifest
import scala.concurrent.Future
import org.apache.openwhisk.core.entity.ByteSize
import scala.concurrent.duration.DurationInt
*/
/*
*Actor API:成员变量self及sender()方法的使用
*/


import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt

import akka.actor.ActorSystem
import akka.http.scaladsl.Http

import org.scalatest.BeforeAndAfterAll
import org.scalatest.Suite

trait WskActorSystem extends BeforeAndAfterAll {
  self: Suite =>
  implicit val actorSystem: ActorSystem = ActorSystem()
  implicit def executionContext: ExecutionContext = actorSystem.dispatcher
  override def afterAll() = {
    try {
      Await.result(Http().shutdownAllConnectionPools(), 30.seconds)
    } finally {
      actorSystem.terminate()
      Await.result(actorSystem.whenTerminated, 30.seconds)
    }
    super.afterAll()
  }
}


object Example_07 extends App {

  class FeedFac extends StreamLogging {

    trait FeedFactory {
      def createFeed(actorRefFactory: ActorRefFactory,
                     messagingProvider: MessagingProvider,
                     messageHandler: Array[Byte] => Future[Unit]): ActorRef
    }

    private def feedProbe(connector: Option[TestConnector] = None) = new FeedFactory {

      implicit val actorSystem: ActorSystem = ActorSystem() //TestProbe需要一个implicit定义的ActorSystem

      def createFeed(f: ActorRefFactory, m: MessagingProvider, p: (Array[Byte]) => Future[Unit]): ActorRef =
        connector
          .map { c =>
            f.actorOf(Props {
              new MessageFeed("activeack", logging, c, 128, 1.second, p)
            })
          }
          .getOrElse(TestProbe().testActor)
    }
  }



/*
  class FirstActor extends Actor
    with ActorLogging {
    //通过context.actorOf方法创建Actor
    var child: ActorRef = _

    override def preStart(): Unit = {
      log.info("preStart() in FirstActor")
      //通过context上下文创建Actor
      child = context.actorOf(Props[MyActor], name = "myActor")
    }

    def receive = {
      //向MyActor发送消息
      case x => child ! x; log.info("received " + x)
    }

  }

  class MyActor extends Actor with ActorLogging {

    var parentActorRef: ActorRef = _

    override def preStart(): Unit = {
      //通过context.parent获取其父Actor的ActorRef
      parentActorRef = context.parent
    }

    def receive = {
      case "test" => log.info("received456 test"); parentActorRef ! "message789 from ParentActorRef"
      case _ => log.info("received321 unknown message");
    }

    val system = ActorSystem("MyActorSystem")
    val systemLog = system.log

    //创建FirstActor对象
    val myactor = system.actorOf(Props[FirstActor], name = "firstActor")
    //获取ActorPath
    val myActorPath = system.child("firstActor")
    //通过system.actorSelection方法获取ActorRef
    val myActor1 = system.actorSelection(myActorPath)
    systemLog.info("准备向myactor发送消息")
    //向myActor1发送消息
    myActor1 ! "test"
    myActor1 ! 123
    Thread.sleep(5000)
    //关闭ActorSystem，停止程序的运行
    //system.shutdown()
  }


 */

  val feedFactorY = new FeedFac
  println("printFeed:")
  println(feedFactorY)




/*
  val system = ActorSystem("MyActorSystem")
  val systemLog = system.log

  //创建FirstActor对象
  val myactor = system.actorOf(Props[FirstActor], name = "firstActor")
  //获取ActorPath
  val myActorPath = system.child("firstActor")
  //通过system.actorSelection方法获取ActorRef
  val myActor1 = system.actorSelection(myActorPath)
  systemLog.info("准备向myactor发送消息")
  //向myActor1发送消息
  myActor1 ! "test"
  myActor1 ! 123
  Thread.sleep(5000)
  //关闭ActorSystem，停止程序的运行
  //system.shutdown()

 */

}



 */
