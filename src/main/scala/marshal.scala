package apollo

import akka.util.{ ByteStringBuilder, ByteString }

import apollo.protocol._

trait CassandraMarshal[T] {

  def put(bsb: ByteStringBuilder, value: T)

}

object CassandraMarshal extends DefaultCassandraMarshal

trait DefaultCassandraMarshal {

  implicit object stringMarshal extends CassandraMarshal[String] {
    
    def put(bsb: ByteStringBuilder, value: String) =
      BodyWriter.putBytes(bsb, Some(ByteString(value, "UTF-8")))

  }

}
