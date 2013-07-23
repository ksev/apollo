package apollo.protocol

import akka.util.{ ByteString, ByteStringBuilder }

class StringSizeError(message: String) extends Error(message)
class ListSizeError(message: String) extends Error(message)
class ByteStringSizeError(message: String) extends Error(message)
class MapSizeError(message: String) extends Error(message)

object BodyReader {

  implicit val byteOrder = java.nio.ByteOrder.BIG_ENDIAN

  def getInt(bs: ByteString): (Int, ByteString) = 
    ( bs.asByteBuffer.getInt()
    , bs.takeRight(bs.length - 4) )


  def getShort(bs: ByteString): (Int, ByteString) =
    ( bs.asByteBuffer.getShort()
    , bs.takeRight(bs.length - 2) )

  def getString(bs: ByteString): (String, ByteString) = {
    val len = bs.asByteBuffer.getShort()
    ( bs.slice(2, len + 2).decodeString("UTF-8")
    , bs.takeRight(bs.length - (2 + len)) )
  }

  def getLongString(bs: ByteString): (String, ByteString) = {
    val len = bs.asByteBuffer.getInt()
    val str = bs.slice(4, len + 4).decodeString("UTF-8")
    ( str, bs.takeRight(bs.length - (4 + len)) ) 
  }

  def getStringList(bs: ByteString): (Vector[String], ByteString) = {
    val n = bs.asByteBuffer.getShort()
      
    val build = Vector.newBuilder[String]
    build.sizeHint(n)

    val rest = Range(0, n).foldLeft(bs.takeRight(bs.length - 2)) { (bs, _) =>
      val (str, rest) = getString(bs)
      build += str

      rest
    }

    (build.result(), rest)
  }

  def getBytes(bs: ByteString): (Option[ByteString], ByteString) = {
     val n = bs.asByteBuffer.getInt()
    if (n >= 0) {
      ( Some(bs.slice(4, n + 4))
      , bs.slice(n + 4, bs.length - (n + 4)) )
    } else {
      ( None, bs.takeRight(bs.length - 4) )
    }
  }

  def getShortBytes(bs: ByteString): (ByteString, ByteString) = {
    val n = bs.asByteBuffer.getShort()
    ( bs.slice(2, n + 2)
    , bs.slice(n + 2, bs.length - (n + 2)) )
  }

  def getMap(bs: ByteString): (Map[String, String], ByteString) = {
    val n = bs.asByteBuffer.getShort()
    val build = Map.newBuilder[String, String]
    build.sizeHint(n)

    val rest = Range(0,n).foldLeft(bs.takeRight(bs.length - 2)) { (bs,_) =>
      val (key, rest) = getString(bs)
      val (value, rest2) = getString(rest)
        
      build += Tuple2(key, value)

      rest2
    }
      
    (build.result(), rest)
  }

  def getMultiMap(bs: ByteString): (Map[String, Vector[String]], ByteString) = {
    val n = bs.asByteBuffer.getShort()
    val build = Map.newBuilder[String, Vector[String]]
    build.sizeHint(n)

    val rest = Range(0,n).foldLeft(bs.takeRight(bs.length - 2)) { (bs,_) =>
      val (key, rest) = getString(bs)
      val (value, rest2) = getStringList(rest)
        
      build += Tuple2(key, value)

      rest2
    }
      
    (build.result(), rest)
  }


}

object BodyWriter {

  implicit val byteOrder = java.nio.ByteOrder.BIG_ENDIAN

  def putInt(bsb: ByteStringBuilder, value: Int) =
    bsb.putInt(value)

  def putShort(bsb: ByteStringBuilder, value: Short) =
    bsb.putShort(value)

  def putString(bsb: ByteStringBuilder, value: String) = {
    val bytes = value.getBytes("UTF-8")

    if (bytes.length > Short.MaxValue) 
      throw new StringSizeError("String passed to short body builder excedes the length of a short")

    bsb.putShort(bytes.length.toShort)
    bsb.putBytes(bytes)
  }

  def putLongString(bsb: ByteStringBuilder, value: String) = {
    val bytes = value.getBytes("UTF-8")

    bsb.putInt(bytes.length)
    bsb.putBytes(bytes)
  }

  def putStringList(bsb: ByteStringBuilder, value: Vector[String]) = {
    if (value.length > Short.MaxValue)
      throw new ListSizeError("Size of the list passed to stringlistBuilder exceeds a short")

    bsb.putShort(value.length)
    value foreach (str => putString(bsb, str))
  }

  def putBytes(bsb: ByteStringBuilder, value: Option[ByteString]) = {
       if (value.isDefined) {
      val bs = value.get
      bsb.putInt(bs.length)
      bsb.append(bs)
    } else {
      bsb.putInt(-1)
    }
  }

  def putShortBytes(bsb: ByteStringBuilder, value: ByteString) = {
    if (value.length > Short.MaxValue)
      throw new ByteStringSizeError("Size of the byte string passed to shortBytesBuilder exceedes a Short")

    bsb.putShort(value.length)
    bsb.append(value)
  }

  def putMap(bsb: ByteStringBuilder, value: Map[String, String]) = {
    if (value.size > Short.MaxValue)
      throw new MapSizeError("The map size passed to string map body builder exceedes a short")

    bsb.putShort(value.size)
    value foreach { case (k,v) =>
       putString(bsb, k)
       putString(bsb, v)
    }
  }

  def putMutliMap(bsb: ByteStringBuilder, value: Map[String, Vector[String]]) = {
    if (value.size > Short.MaxValue) 
      throw new MapSizeError("The map size passed to string multi map body builder exceedes a short")

    bsb.putShort(value.size)
    value foreach { case (k,v) =>
      putString(bsb, k)
      putStringList(bsb, v)
    }
  }

}
