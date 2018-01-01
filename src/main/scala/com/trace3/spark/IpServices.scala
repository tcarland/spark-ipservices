package com.trace3.spark


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._


case class Service(
  name:     String,
  port:     Integer,
  proto:    String,
  aliases:  String,
  comment:  String
)


object IpServices {

  var dbName = "ipservices"

  val usage : String =
    """
      |Usage: ProtoServices [servicesfile] <db.tableName>
      |  eg. file:///etc/services
      |  tableName is optional, default table is 'default.ipservices'
    """.stripMargin


  def main ( args: Array[String] ) : Unit = {

    if ( args.size < 1 ) {
      println(usage)
      System.exit(0)
    }

    if ( args.size == 2 )
      dbName = args(1)

    val spark = SparkSession
      .builder
      .appName("IpServices")
      .enableHiveSupport()
      .config("spark.sql.parquet.compression.codec", "snappy")
      .config("spark.sql.hive.convertMetastoreParquet", "false")
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()

    import spark.implicits._

    val p1  = """([^#]\S+)\t+(\d+)\/(\S+)""".r
    val p2  = """([^#]\S+)\t+(\d+)\/(\S+)\t+([^#]\S+)""".r
    val p3  = """([^#]\S+)\t+(\d+)\/(\S+)\t+#\s(.+)""".r
    val p4  = """([^#]\S+)\t+(\d+)\/(\S+)\t+(\S+)\t+#\s(.*)""".r

    val svcdf = spark.sparkContext
      .textFile(args(0))
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

    svcdf.write.format("parquet").mode(SaveMode.Overwrite).saveAsTable(dbName)
    println("Finished.")

    spark.stop
  }

}
