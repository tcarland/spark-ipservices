
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._


case class Service(
  name:     String,
  port:     Integer,
  proto:    String,
  aliases:  String,
  comment:  String
)

val file = "s3a://scratch/services"


  import spark.implicits._

  val p1  = """([^#]\S+)\s+(\d+)\/(\S+)""".r
  val p2  = """([^#]\S+)\s+(\d+)\/(\S+)\s+([^#]\S+)""".r
  val p3  = """([^#]\S+)\s+(\d+)\/(\S+)\s+#\s(.+)""".r
  val p4  = """([^#]\S+)\s+(\d+)\/(\S+)\s+(\S+)\t+#\s(.*)""".r


:paste

    val svcdf = spark.sparkContext
      .textFile(file)
      .map( line => {
        val service: Service = line match {
          case p1(svc, po, pr)       => Service(svc, po.toInt, pr, s"", s"")
          case p2(svc, po, pr, a)    => Service(svc, po.toInt, pr, a, s"")
          case p3(svc, po, pr, r)    => Service(svc, po.toInt, pr, s"", r)
          case p4(svc, po, pr, a, r) => Service(svc, po.toInt, pr, a, r)
          case _  => Service("", 0, "", "", "")
        }
        service
      })
      .filter(svc => svc.port > 0)
      .toDS()

    //svcdf.write.format("parquet").mode(SaveMode.Overwrite).saveAsTable(table)
    svcdf.write.parquet("s3a://scratch/ipservices")
    println("Finished.")
