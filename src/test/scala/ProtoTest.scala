
import scala.io.Source


case class Service (
  name: String,
  port: Integer,
  proto: String,
  aliases: String,
  comment: String
)


/**  Parses /etc/services into the Service object */
object ProtoTest {

  def main(args: Array[String]): Unit = {
    if ( args.length < 1 ) {
        println("Usage: ProtoTest <filename>")
        System.exit(0)
    }

    val file  = args(0)
    val lines = Source.fromFile(file).getLines()
    var svcs  = Seq.empty[Service]

    val p1  = """([^#]\S+)\s+(\d+)\/(\S+)""".r                    // 2 columns
    val p2  = """([^#]\S+)\s+(\d+)\/(\S+)\s+([^#]\S+)""".r        // 3 columns, no comment
    val p3  = """([^#]\S+)\s+(\d+)\/(\S+)\s+#\s(.+)""".r          // 3 columns w/ coment
    val p4  = """([^#]\S+)\s+(\d+)\/(\S+)\s+(\S+)\t+#\s(.*)""".r  // 4 columns

    for (line <- lines) {
      val svc: Service = line match {
        case p1(svc, po, pr)       => Service(svc, po.toInt, pr, s"", s"")
        case p2(svc, po, pr, a)    => Service(svc, po.toInt, pr, a, s"")
        case p3(svc, po, pr, r)    => Service(svc, po.toInt, pr, s"", r)
        case p4(svc, po, pr, a, r) => Service(svc, po.toInt, pr, a, r)
        case _  => Service("", 0, "", "", "")
      }

      if ( svc.port > 0 )
        svcs = svc +: svcs
    }

    svcs.foreach(println)
    println("Total: " + svcs.size)
  }

}
