package apollo.protocol

import akka.util.{ ByteString, ByteStringBuilder }

trait BodyBuilder[T] extends DefaultBodyBuilders {
  
  def write(bsb: ByteStringBuilder, value: T) // Uses mutable ByteStringBuilder for efficency
  def read(bs: ByteString): (T, ByteString)

}

object BodyBuilder extends DefaultBodyBuilders

case class LongString(str: String) extends AnyVal
case class ShortBytes(byteString: ByteString) extends AnyVal

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
  class ListSizeError(message: String) extends Error(message)
  class ByteStringSizeError(message: String) extends Error(message)
  class MapSizeError(message: String) extends Error(message)

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

  implicit object stringListBodyBuilder extends BodyBuilder[Vector[String]] {

    def write(bsb: ByteStringBuilder, value: Vector[String]) = {
      if (value.length > Short.MaxValue)
        throw new ListSizeError("Size of the list passed to stringlistBuilder exceeds a short")

      bsb.putShort(value.length)
      value foreach (str => shortStringBodyBuilder.write(bsb, str))
    }

    def read(bs: ByteString): (Vector[String], ByteString) = {
      val n = bs.asByteBuffer.getShort()
      
      val build = new scala.collection.immutable.VectorBuilder[String]()
      build.sizeHint(n)

      val rest = Range(0, n).foldLeft(bs.takeRight(bs.length - 2)) { (bs, _) =>
        val (str, rest) = shortStringBodyBuilder.read(bs)
        build += str

        rest
      }

      (build.result(), rest)
    }

  }

  implicit object bytesBodyBuilder extends BodyBuilder[Option[ByteString]] {

    def write(bsb: ByteStringBuilder, value: Option[ByteString]) = {
      if (value.isDefined) {
        val bs = value.get
        bsb.putInt(bs.length)
        bsb.append(bs)
      } else {
        bsb.putInt(-1)
      }
    }

    def read(bs: ByteString): (Option[ByteString], ByteString) = {
      val n = bs.asByteBuffer.getInt()
      if (n >= 0) {
        ( Some(bs.slice(4, n + 4))
        , bs.slice(n + 4, bs.length - (n + 4)) )
      } else {
        ( None, bs.takeRight(bs.length - 4) )
      }
    }

  }

  implicit object shortBytesBuilder extends BodyBuilder[ShortBytes] {
  
    def write(bsb: ByteStringBuilder, value: ShortBytes) = {
      val bs = value.byteString
      
      if (bs.length > Short.MaxValue)
        throw new ByteStringSizeError("Size of the byte string passed to shortBytesBuilder exceedes a Short")

      bsb.putShort(bs.length)
      bsb.append(bs)
    }

    def read(bs: ByteString): (ShortBytes, ByteString) = {
      val n = bs.asByteBuffer.getShort()
      ( ShortBytes(bs.slice(2, n + 2))
      , bs.slice(n + 2, bs.length - (n + 2)) )
    }

  }

  implicit object stringMapBodyBuilder extends BodyBuilder[Map[String,String]] {

    def write(bsb: ByteStringBuilder, value: Map[String,String]) = {
      if (value.size > Short.MaxValue)
        throw new MapSizeError("The map size passed to string map body builder exceedes a short")

      bsb.putShort(value.size)
      value foreach { case (k,v) =>
         shortStringBodyBuilder.write(bsb, k)
         shortStringBodyBuilder.write(bsb, v)
      }
    }

    def read(bs: ByteString): (Map[String,String], ByteString) = {
      val n = bs.asByteBuffer.getShort()
      val build = Map.newBuilder[String, String]
      build.sizeHint(n)

      val rest = Range(0,n).foldLeft(bs.takeRight(bs.length - 2)) { (bs,_) =>
        val (key, rest) = shortStringBodyBuilder.read(bs)
        val (value, rest2) = shortStringBodyBuilder.read(rest)
        
        build += Tuple2(key, value)

        rest2
      }
      
      (build.result(), rest)
    }

  }

  implicit object stringMultiMapBodyBuilder extends BodyBuilder[Map[String,Vector[String]]] {

    def write(bsb: ByteStringBuilder, value: Map[String, Vector[String]]) = {
      if (value.size > Short.MaxValue) 
        throw new MapSizeError("The map size passed to string multi map body builder exceedes a short")

      bsb.putShort(value.size)
      value foreach { case (k,v) =>
        shortStringBodyBuilder.write(bsb, k)
        stringListBodyBuilder.write(bsb, v)
      }
    }

    def read(bs: ByteString): (Map[String,Vector[String]], ByteString) = {
      val n = bs.asByteBuffer.getShort()
      val build = Map.newBuilder[String, Vector[String]]
      build.sizeHint(n)

      val rest = Range(0,n).foldLeft(bs.takeRight(bs.length - 2)) { (bs,_) =>
        val (key, rest) = shortStringBodyBuilder.read(bs)
        val (value, rest2) = stringListBodyBuilder.read(rest)
        
        build += Tuple2(key, value)

        rest2
      }
      
      (build.result(), rest)
    }

  }

}
