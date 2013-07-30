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

  private def checkFailure(frame: Frame): Future[Frame] =
    if (frame.opcode != Opcode.ERROR) Future.successful(frame)
    else Future.failed(frame.toResponse[apollo.Error])

  private def request[T : ResponseReader](req: Request): Future[T] =
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
  def exec[T : ResponseReader](query: String, params: CassandraValue[_]*): Future[T] =
    request[T](ParamQueryUnbounded(query)(params : _*))

  /*
  def exec[T <: ResultType](query: Future[PreparedQueryBounded]): Future[T] =
    query flatMap (request[T] _)
*/
}
