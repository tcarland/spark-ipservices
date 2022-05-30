package com.trace3.spark


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._


case class ServiceTable (
  name:     String,
  port:     Integer,
  proto:    String,
  aliases:  String,
  comment:  String
)


object IpServicesTable {

  var dbName = "default.ipservices"

  val usage : String =
    """
      |Usage: IpServicesTable [servicesfile] <db.tableName>
      |  eg.  file:///etc/services or s3a://scratch/ipservices
      |  tableName is optional, default is 'default.ipservices'
    """.stripMargin


  def main ( args: Array[String] ) : Unit = {

    if ( args.size < 1 ) {
      println(usage)
      System.exit(0)
    }

    if ( args.size == 2 )
      dbName = args(1)

    val spark = SparkSession
      .builder()
      .appName("IpServicesTable")
      .enableHiveSupport()
      .getOrCreate()
      //.config("spark.sql.parquet.compression.codec", "snappy")
      //.config("spark.sql.hive.convertMetastoreParquet", "false")
      //.config("hive.exec.dynamic.partition", "true")
      //.config("hive.exec.dynamic.partition.mode", "nonstrict")

    import spark.implicits._

    val p1  = """([^#]\S+)\s+(\d+)\/(\S+)""".r
    val p2  = """([^#]\S+)\s+(\d+)\/(\S+)\s+([^#]\S+)""".r
    val p3  = """([^#]\S+)\s+(\d+)\/(\S+)\s+#\s(.+)""".r
    val p4  = """([^#]\S+)\s+(\d+)\/(\S+)\s+(\S+)\t+#\s(.*)""".r

    val svcdf = spark.sparkContext
      .textFile(args(0))
      .map( line => {
        val service: ServiceTable = line match {
          case p1(svc, po, pr)       => ServiceTable(svc, po.toInt, pr, s"", s"")
          case p2(svc, po, pr, a)    => ServiceTable(svc, po.toInt, pr, a, s"")
          case p3(svc, po, pr, r)    => ServiceTable(svc, po.toInt, pr, s"", r)
          case p4(svc, po, pr, a, r) => ServiceTable(svc, po.toInt, pr, a, r)
          case _  => ServiceTable("", 0, "", "", "")
        }
        service
      })
      .filter(svc => svc.port > 0)
      .toDS()

    spark.sql("CREATE TABLE IF NOT EXISTS " + dbName + 
      " (name STRING, port BIGINT, proto STRING, aliases STRING, comment STRING)" +
      " STORED AS parquet")

    svcdf.write.format("parquet").mode(SaveMode.Overwrite).insertInto(dbName)
    println("Finished.")

    spark.stop()
  }

}