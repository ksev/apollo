import org.scalatest.{ FunSuite, BeforeAndAfterAll }
import akka.testkit.{ TestKit, ImplicitSender }

import java.net.InetSocketAddress

import akka.actor.ActorSystem
import akka.actor.Props

import akka.util.ByteStringBuilder

import scala.concurrent.duration._
import scala.concurrent.Await
import akka.pattern.ask

import apollo._
import apollo.net._
import apollo.protocol._

class ConnectionTest(_system: ActorSystem) extends TestKit(_system) 
                                           with ImplicitSender
                                           with FunSuite 
                                           with BeforeAndAfterAll {
  import akka.io.Tcp._

  def this() = this(ActorSystem("ConnectionTest"))

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  test("Should connect on creation") {
    val addr = new InetSocketAddress("localhost", 9042)
    val con = system.actorOf(Props(classOf[Connection], addr, self))
    
    expectMsgType[Connected]
  }

}
