package apollo.net

import java.net.InetSocketAddress

import akka.actor._
import akka.event.Logging
import akka.io.{ IO, Tcp }
import akka.util.ByteString

/** Represents a connection to a Cassandra cluster 
  * @param addr The network address to connect to
  * @param listen The Akka actor that listens to messages from the socket
  */
class Connection(addr: InetSocketAddress) extends Actor {

  import Tcp._
  import context.system

  val log = Logging(context.system, this)
  var socket: ActorRef = _

  IO(Tcp) ! Connect(addr)

  def connected: Receive = {
    
    case data: ByteString => 
      socket ! Write(data)

    case Received(data) =>
      //listen ! data

    case CommandFailed(w: Write) =>
      log.error("Cassandra write failed:\n{}", w)

    case Close =>
      socket ! Close

    case _: ConnectionClosed =>
      context stop self

  }

  def receive = {
  
    case CommandFailed(a: Connect) =>
      log.error("Cassandra connection error:\n{}", a)
      context stop self

    case c: Connected =>
      socket = sender
      socket ! Register(self)
      //listen ! c
      context.become(connected)

  }


}
