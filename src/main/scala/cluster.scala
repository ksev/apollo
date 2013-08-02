package apollo

import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration._

import com.typesafe.config.{ Config, ConfigFactory }

import akka.actor._
import akka.pattern._
import akka.util.Timeout

import apollo.net.ConnectionPool

/** Cluster represents an entire Cassandra cluster
  * @param config Connection configuration
  */
class Cluster(config: Config)(implicit sys: ActorSystem) {

  import apollo.protocol._
  import apollo.net.ConnectionRequest

  private val cfg =
    config.withFallback(ConfigFactory.parseString("""
            apollo: {
              hostname: localhost
              port: 9042
              connections-per-host: 1
              cql-version: auto
              internal-timeout: 3000
            }
            """))
          .getConfig("apollo")
    
  private implicit val timeout = Timeout(cfg.getInt("internal-timeout").milliseconds)
  private implicit val ec = sys.dispatcher

  private val pool = 
    sys.actorOf(Props(classOf[ConnectionPool], 
                cfg.getInt("connections-per-host"),
                cfg), "apollo-connection-pool")

  /* Helper to check if the response is a cassandra error */ 
  private def checkFailure(frame: Frame): Future[Frame] =
    if (frame.opcode != Opcode.ERROR) Future.successful(frame)
    else Future.failed(frame.toResponse[apollo.Error])

  /* Lowest level request function */
  def request[T <: Response : ResponseReader](req: Request): Future[T] =
    for { 
      con <- (pool ? ConnectionRequest).mapTo[ActorRef]
      frm <- (con ? req.toFrame).mapTo[Frame].flatMap(checkFailure)
    } yield frm.toResponse[T]

  /** Receive the options that the Cassandra cluster can handle
    * @return Future of the query result
    */ 
  def options(): Future[Supported] = 
    request[Supported](Options)

  /** Send a use command to cassandra
    * @param keyspace The keyspace to switch to
    * @return A SetKeyspace result
    */ 
  def use(keyspace: String): Future[SetKeyspace] =
    request[SetKeyspace](RawQuery(s"USE $keyspace"))

  /** Execute a query against cassandra
    */
  def exec[T <: Result : ResponseReader](query: String, params: CassandraValue[_]*): Future[T] =
    request[T](ParamQueryUnbounded(query)(params : _*))

}
