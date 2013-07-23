package apollo

import com.typesafe.config.{ Config, ConfigFactory }

import akka.actor._

import apollo.net.ConnectionPool

/** Cluster represents an entire Cassandra cluster
  * @param config Connection configuration
  */
class Cluster(config: Config)(implicit sys: ActorSystem) {

  private val cfg =
    config.withFallback(ConfigFactory.parseString("""
            apollo: {
              hostname: localhost
              port: 9042
              min-connections: 1
              max-connections: 1
              cql-version: auto
            }
            """))
          .getConfig("apollo")
    
  private val poolActor = 
    sys.actorOf(Props(classOf[ConnectionPool], 
                cfg.getInt("min-connections"), 
                cfg.getInt("max-connections"),
                cfg), "apollo-connection-pool")

  /** Get a session you can use to talk the Cassandra cluster
    * @returns A [apollo.Session] 
    */
  def session() = new Session(poolActor, cfg)

}
