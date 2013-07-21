import org.scalatest.{ FunSuite, BeforeAndAfterAll }

import akka.util.{ ByteString, ByteStringBuilder }

import apollo._

class BodyBuilderTests extends FunSuite {

  implicit val byteOrder = java.nio.ByteOrder.BIG_ENDIAN;

  test("Short builder write/read") {
    val b = implicitly[BodyBuilder[Short]]
    val bsb = new ByteStringBuilder()

    val s: Short = Short.MaxValue
    b.write(bsb, s)

    val (res, rest) = b.read(bsb.result())

    assert(rest.isEmpty)
    assert(res === s)
  }

  test("Short string write/read") {
    val b = implicitly[BodyBuilder[String]]
    val bsb = new ByteStringBuilder()

    val target = "A string with some ÅÄÖ aumlauts åäö"

    b.write(bsb, target)

    val (str, rest) = b.read(bsb.result())

    assert(rest.isEmpty)
    assert(target === str)
  }

  test("Int write/read") {
    val b = implicitly[BodyBuilder[Int]]
    val bsb = new ByteStringBuilder()

    val target = Int.MaxValue

    b.write(bsb, target)

    val (res, rest) = b.read(bsb.result())

    assert(rest.isEmpty)
    assert(res === target)
  }

  test("LongString write/read") {
    val b = implicitly[BodyBuilder[LongString]]
    val bsb = new ByteStringBuilder()

    val target = LongString("Some string with some stuff in it åäö åäö ÅÄÖ")

    b.write(bsb, target)

    val (res, rest) = b.read(bsb.result())

    assert(rest.isEmpty)
    assert(res === target)
  }

  test("String list write/read") {
    val b = implicitly[BodyBuilder[Vector[String]]]
    val bsb = new ByteStringBuilder()

    val target = Vector(
      "A string with some UTF-8 åäö",
      "Other string i suppose")

    b.write(bsb, target)
  
    val (res, rest) = b.read(bsb.result())

    assert(rest.isEmpty)
    assert(res === target)
  }

  test("Bytes write/read") {
    val b = implicitly[BodyBuilder[Option[ByteString]]]
    val bsb = new ByteStringBuilder()

    val target = Some(ByteString("just some bytes"))

    b.write(bsb, target)

    val (res, rest) = b.read(bsb.result())

    assert(rest.isEmpty)
    assert(res === target)
  }

  test("Short bytes write/read") {
    val b = implicitly[BodyBuilder[ShortBytes]]
    val bsb = new ByteStringBuilder()

    val target = ShortBytes(ByteString("just some bytes"))

    b.write(bsb, target)

    val (res, rest) = b.read(bsb.result())

    assert(rest.isEmpty)
    assert(res === target)
  }

  test("String map write/read") {
    val b = implicitly[BodyBuilder[Map[String, String]]]
    val bsb = new ByteStringBuilder()

    val target = Map("str" -> "you know something åäö", 
                     "str2" -> "walla")

    b.write(bsb, target)

    val (res, rest) = b.read(bsb.result())

    assert(rest.isEmpty)
    assert(res === target)
  }
  
  test("String miltimap write/read") {
    val b = implicitly[BodyBuilder[Map[String, Vector[String]]]]
    val bsb = new ByteStringBuilder()

    val target = Map("str" -> Vector("you know something åäö", "what"), 
                     "str2" -> Vector("walla", "str two", "and three"))

    b.write(bsb, target)

    val (res, rest) = b.read(bsb.result())

    assert(rest.isEmpty)
    assert(res === target)
  }

}
