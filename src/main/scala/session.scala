package apollo

import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration._

import com.typesafe.config.Config

import scala.concurrent.Await
import scala.concurrent.duration._

import akka.actor._
import akka.pattern._
import akka.util.Timeout

class Session(pool: ActorRef, cfg: Config)(implicit sys: ActorSystem) {

  import apollo.protocol._
  import apollo.net.ConnectionRequest

  implicit val timeout = Timeout(30 seconds)
  implicit val ec = sys.dispatcher

  private def connection() = 
    (pool ? ConnectionRequest).mapTo[ActorRef]

  /** Receive the options that the Cassandra cluster can handle
    * @return Future of the query result
    */ 
  def options(): Future[Supported] =
    for { 
      con <- connection()
      frame <- (con ? Options.toFrame).mapTo[Frame]
    } yield frame.toResponseType[Supported]

  def execute(query: String): Future[Frame] =
    for {
      con <- connection()
      frame <- (con ? Query(query).toFrame).mapTo[Frame]
    } yield frame

}
