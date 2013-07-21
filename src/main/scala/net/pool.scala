package apollo.net

import java.net.InetSocketAddress

import com.typesafe.config.Config

import akka.actor._
import akka.event.Logging

class ConnectionPool(minConnections: Int, maxConnections: Int, cfg: Config) extends Actor {

  val log = Logging(context.system, this)
  val addr = new InetSocketAddress(cfg.getString("apollo.hostname"), cfg.getInt("apollo.port"))
  val queue = scala.collection.mutable.Queue(
    (0 to minConnections) map (_ => context.system.actorOf(Props(classOf[Connection], addr)))
  )

  def receive = {
  
    case a => log.info("{}", a)


  }

}
