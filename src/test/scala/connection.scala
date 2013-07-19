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

  test("Sending commands and receiving") {
    val addr = new InetSocketAddress("localhost", 9042)
    val con = system.actorOf(Props(classOf[Connection], addr, self))

    expectMsgType[Connected]
    
    val bsb = new ByteStringBuilder()
    bsb.sizeHint(8)

    // Options frame
    bsb.putByte(Version.V2REQUEST)
    bsb.putByte(Flags.NONE)
    bsb.putByte(1)
    bsb.putByte(Opcode.OPTIONS)
    bsb.putInt(0)(java.nio.ByteOrder.BIG_ENDIAN)

    con ! bsb.result()

    expectMsgType[akka.util.ByteString]
  }

}
