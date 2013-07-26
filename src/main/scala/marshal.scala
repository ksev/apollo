package apollo

import akka.util.ByteStringBuilder

trait CassandraMarshal[T] {

  def put(bsb: ByteStringBuilder, value: T)

}

object CassandraMarshal extends DefaultCassandraMarshal

trait DefaultCassandraMarshal {

  implicit object stringMarshal extends CassandraMarshal[String] {
    
    def put(bsb: ByteStringBuilder, value: String) =
      bsb.putBytes(value.getBytes("UTF-8"))
  }

}
