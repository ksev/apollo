package apollo

import java.nio.ByteBuffer

import akka.util.{ ByteString, ByteStringBuilder }

object Version {
  final val V2REQUEST   = 0x02.toByte
  final val V2RESPONSE  = 0x82.toByte
}

object Flags {
  final val NONE        = 0x00.toByte
  final val COMPRESSION = 0x01.toByte
  final val TRACING     = 0x02.toByte
}

object Opcode {
  final val ERROR           = 0x00.toByte
  final val STARTUP         = 0x01.toByte
  final val READY           = 0x02.toByte
  final val AUTHENTICATE    = 0x03.toByte
  final val OPTIONS         = 0x05.toByte
  final val SUPPORTED       = 0x06.toByte
  final val QUERY           = 0x07.toByte
  final val RESULT          = 0x08.toByte
  final val EXECUTE         = 0x0A.toByte
  final val REGISTER        = 0x0B.toByte
  final val EVENT           = 0x0C.toByte
  final val BATCH           = 0x0D.toByte
  final val AUTH_CHALLENGE  = 0x0E.toByte
  final val AUTH_RESPONSE   = 0x0F.toByte
  final val AUTH_SUCCESS    = 0x10.toByte
}



object Consistency {
  final val ANY           = 0x0000.toShort
  final val ONE           = 0x0001.toShort
  final val TWO           = 0x0002.toShort
  final val THREE         = 0x0003.toShort
  final val QUORUM        = 0x0004.toShort
  final val ALL           = 0x0005.toShort
  final val LOCAL_QUORUM  = 0x0006.toShort
  final val EACH_QUORUM   = 0x0007.toShort
}

trait BodyBuilder[T] extends DefaultBodyBuilders {
  
  def write(bsb: ByteStringBuilder, value: T) // Uses mutable ByteStringBuilder for efficency
  def read(bs: ByteString): (T, ByteString)

}

object BodyBuilder extends DefaultBodyBuilders

case class LongString(str: String) extends AnyVal

trait DefaultBodyBuilders {

  implicit val byteOrder = java.nio.ByteOrder.BIG_ENDIAN

  implicit object intBodyBuilder extends BodyBuilder[Int] {

    def write(bsb: ByteStringBuilder, value: Int) =
      bsb.putInt(value)

    def read(bs: ByteString): (Int, ByteString) = 
      ( bs.asByteBuffer.getInt()
      , bs.takeRight(bs.length - 4) )

  }

  implicit object shortBodyBuilder extends BodyBuilder[Short] {
    
    def write(bsb: ByteStringBuilder, value: Short) = 
      bsb.putShort(value)

    def read(bs: ByteString): (Short, ByteString) = 
      ( bs.asByteBuffer.getShort()
      , bs.takeRight(bs.length - 2) )

  }

  class StringSizeError(message: String) extends Error(message)
  implicit object shortStringBodyBuilder extends BodyBuilder[String] {

    def write(bsb: ByteStringBuilder, value: String) = {
      val bytes = value.getBytes("UTF-8")

      if (bytes.length > Short.MaxValue) 
        throw new StringSizeError("String passed to short body builder excedes the length of a short")

      bsb.putShort(bytes.length.toShort)
      bsb.putBytes(bytes)
    }

    def read(bs: ByteString): (String, ByteString) = {
      val len = bs.asByteBuffer.getShort()
      ( bs.slice(2, len + 2).decodeString("UTF-8")
      , bs.takeRight(bs.length - (2 + len)) )
    }

  }

  implicit object longStringBodyBuilder extends BodyBuilder[LongString] {
    
    def write(bsb: ByteStringBuilder, value: LongString) = {
      val s = value.str
      val bytes = s.getBytes("UTF-8")

      if (bytes.length > Int.MaxValue)
        throw new StringSizeError("String passed to long string body builder exceedes the size of an int")

      bsb.putInt(bytes.length)
      bsb.putBytes(bytes)
    }

    def read(bs: ByteString): (LongString, ByteString) = {
      val len = bs.asByteBuffer.getInt()
      val str = bs.slice(4, len + 4).decodeString("UTF-8")
      ( LongString(str)
      , bs.takeRight(bs.length - (4 + len)) ) 
    }

  }

}

class Frame(
  version: Byte,
  flags: Byte,
  stream: Byte,
  opcode: Byte,
  body: ByteString)


