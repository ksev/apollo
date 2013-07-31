package apollo.net

import java.net.InetSocketAddress

import com.typesafe.config.Config

import akka.actor._
import akka.pattern._
import akka.event.Logging
import akka.util.Timeout

case object ConnectionRequest

class ConnectionPool(connectionsPerHost: Int, cfg: Config) extends Actor {

  import akka.io.Tcp._

  val log = Logging(context.system, this)
  val addr = new InetSocketAddress(cfg.getString("hostname"), cfg.getInt("port"))

  val waiting = scala.collection.mutable.Queue.empty[ActorRef] 
  val connections = scala.collection.mutable.Queue.empty[ActorRef]

  def makeConnection(i: Int) = {
    val ref = context.system.actorOf(Props(classOf[Connection], addr, self, cfg), "apollo-connection")
    context.watch(ref)
    ref
  }

  (1 to connectionsPerHost) foreach makeConnection

  def receive = {
  
    /* A Connection successfully connected to the cassandra cluster
     * Add the connection to the pool of live connections
     * If we have clients waiting for a connection go through them
     * and make a new ConnectionRequest on their behalf
     */
    case Connected =>
      connections.enqueue(sender)

      log.info("Connection joined pool (waiting {}): {}", waiting.length, sender)

      if (!waiting.isEmpty) {
        val r = waiting.dequeue()
        self.tell(ConnectionRequest, r)
      }

    /* A client is requesting a connection from the pool.
     * If we have connections availiable give it to sender else put it in line
     */ 
    case ConnectionRequest => 
      log.info("Connection request (avail: {})", connections.length)
      if (!connections.isEmpty) {
        val c = connections.dequeue()
        sender ! c
        connections.enqueue(c)
      } else {
        waiting.enqueue(sender)
      }

    /* A connection if the pool was closed or errored out.
     * Cleanse the pool of all references of the connection
     * Start new connections if we have below minimum connection amount
     */ 
    case Terminated(con) =>
      connections.dequeueAll(_ == con)
      (1 to (connectionsPerHost - connections.length)) foreach makeConnection

  }

}
