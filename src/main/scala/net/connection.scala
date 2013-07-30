package apollo.net

import java.net.InetSocketAddress

import com.typesafe.config.Config

import akka.actor._
import akka.event.Logging
import akka.io.{ IO, Tcp }
import akka.util.ByteString

import apollo.protocol.{ Frame, Version }
import apollo._

/** This has nothing to do with streaming, its a util class for the connection to be able to
  * keep track of Cassandra stream id and which actor that initiated the request
  * This class is full of mutable state, IT IS NOT THREAD SAFE
  */ 
class StreamMap {
  
  private val streamBuffer = scala.collection.mutable.ArrayBuffer.fill(128)(Option.empty[ActorRef])
  private val numbers = scala.collection.mutable.Queue.range(0,128)

  def add(actor: ActorRef) = {
    assert(!numbers.isEmpty)

    val nr = numbers.dequeue()
    streamBuffer(nr) = Some(actor)
    nr
  }

  def remove(nr: Int): ActorRef = {
    val ref = streamBuffer(nr)
    numbers.enqueue(nr)
    streamBuffer(nr) = None
    ref.get
  }

  def isFull = numbers.isEmpty

}

/** Represents a connection to a Cassandra cluster 
  * @param addr The network address to connect to
  */
class Connection(addr: InetSocketAddress, pool: ActorRef, cfg: Config) extends Actor {

  import Tcp._
  import context.system

  val log = Logging(context.system, this)
  val strmMap = new StreamMap()
  val waiting = scala.collection.mutable.Queue.empty[(Frame, ActorRef)]

  // Connect to socket on startup 
  IO(Tcp) ! Connect(addr)

  // Connection state connected
  def connected(socket: ActorRef): Receive = {
    
    // A frame to write to the socket
    // When this is done we fetch a number in the stream map
    // save the senders ref in the map so we know who to send to on response
    case frame: Frame => 
      assert(frame.version == Version.V2REQUEST)
      if (!strmMap.isFull) {
        val nr = strmMap.add(sender)
        log.info("Frame request (opcode: {}, stream: {})", frame.opcode, nr)
        socket ! Write(frame.copy(stream = nr.toByte).toByteString)
      } else {
        log.info("{} stream map is full, THIS IS BAD CREATE MORE CONNECTIONS", self)
        waiting.enqueue((frame, sender))
      }

    // We asume that what we receive from socket is a frame
    // Remove that command from the stream map
    // Check if we have things waiting
    case Received(data: ByteString) =>
      val frame = Frame.fromByteString(data)

      assert(frame.version == Version.V2RESPONSE)
      log.info("Frame response (opcode: {}, stream: {})", frame.opcode, frame.stream)

      strmMap.remove(frame.stream) ! frame

      if (!waiting.isEmpty) {
        val (frm, ref) = waiting.dequeue()
        self.tell(frm, ref)
      }

    case CommandFailed(w: Write) =>
      log.error("Cassandra write failed:\n{}", w)

    case Close =>
      socket ! Close

    case _: ConnectionClosed =>
      context stop self

  }

  // Limbo state when the socket is connected but the client is not initiated yet
  def init(socket: ActorRef): Receive = {

    // We received something
    case Received(data: ByteString) =>
      import apollo.protocol.Opcode

      val frame = Frame.fromByteString(data)
      frame.opcode match {
      
        case Opcode.SUPPORTED =>
          val map = implicitly[ResponseReader[Supported]].read(frame).value
          socket ! Write(Startup(map("CQL_VERSION").head).toFrame.toByteString)

        case Opcode.READY =>
          // We have implicit keyspace config
          if (cfg.hasPath("keyspace")) { 
            val keyspace = cfg.getString("keyspace")
            socket ! Write(RawQuery(s"USE $keyspace").toFrame.toByteString)
          } else {
            context become connected(socket)
            pool ! Connected
          }

        case Opcode.RESULT => // The only result we get here is set key space
          context become connected(socket)
          pool ! Connected

      }

  }

  def receive = {
  
    case CommandFailed(a: Connect) =>
      log.error("Cassandra connection error:\n{}", a)
      context stop self

    // The socket has connected
    // Time to send startup (options if CQL version is set to automatic)
    case c: Connected =>
      log.info("Cassandra connected")
      sender ! Register(self)
      context become init(sender)

      val v = cfg.getString("cql-version")

      // If the version is auto we have to ask the server for the correct version
      if (v == "auto") sender ! Write(Options.toFrame.toByteString)
      else sender ! Write(Startup(cqlVersion = v).toFrame.toByteString)

  }

}
