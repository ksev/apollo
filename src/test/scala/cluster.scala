import org.scalatest.{ FunSuite, BeforeAndAfterAll }

import scala.concurrent.{ Await, ExecutionContext }
import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.actor.ActorSystem

import apollo._
import apollo.PreparedStatement._

class ClusterTests extends FunSuite with BeforeAndAfterAll {


  implicit var sys: ActorSystem = _
  implicit var ec: ExecutionContext = _

  val cfg = ConfigFactory.load()
  lazy val ses: Session = new Cluster(cfg).session()

  override def beforeAll {
    sys = ActorSystem("cluster-test")
    ec = sys.dispatcher

    val cqlKS = """
      CREATE KEYSPACE test WITH 
      replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 }
    """

    val cqlTBL = """
      CREATE TABLE test.user (
        username text,
        email text,
        PRIMARY KEY (username)
      );
    """

    val schema = for {
      _ <- ses.exec[SchemaChange](cqlKS)
      _ <- ses.exec[SchemaChange](cqlTBL)
    } yield ()

    Await.result(schema, 2 seconds)
  }

  override def afterAll {
    val cqlDKS = """
      DROP KEYSPACE test;
    """

    Await.result(ses.exec[SchemaChange](cqlDKS), 2 seconds)
    sys.shutdown()
  }

  test("Options") {
    Await.result(ses.options(), 2 seconds)
  }

  test("Query with params") {
    val crd = for {
      _ <- ses.exec[Void]("INSERT INTO test.user (username, email) VALUES (?, ?)", "ksev", "kim@pixlr.com")
      _ <- ses.exec[Rows]("SELECT * FROM test.user WHERE username = ?", "ksev")
      _ <- ses.exec[Void]("DELETE FROM test.user WHERE username = ?", "ksev")
    } yield ()

    Await.result(crd, 2 seconds)
  }

  /*
  test("Query with prepared Query") {
    val username = "user1"
    val email = "test@example.com"

    val crd = for {
      _ <- ses.execute(cql"INSERT INTO test.user (username, email) VALUES ($username, $email)")
      _ <- ses.execute(cql"SELECt * FROM test.user WHERE username = $username")
      _ <- ses.execute(cql"DELETE FROM test.user WHERE username = $username")
    } yield ()

    Await.result(crd, 2 seconds)
  }*/

  test("Prepared generation") {
    val username = "userid"
    val email = "test@example.com"

    cql"SELECT * FROM user WHERE username = $username"
    cql"INSERT INTO user (username, email) VALUES ($username, $email)"
  }

}
