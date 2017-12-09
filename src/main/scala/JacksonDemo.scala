import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.native.Serialization.write

case class Record(name: String, age: Int)

object JacksonDemo {

  def main(args: Array[String]): Unit = {

    implicit val formats = DefaultFormats

    val ret = parse("{\"name\":\"zzz\",\"age\":19}").extract[Record]

    println(ret.name)
    println(ret.age)

    val ret2 = Record("hhaha", 20)
    println(write(ret2))
  }

}
