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
      |Usage: IpServices [servicesfile] <db.tableName>
      |  eg.  file:///etc/services
      |  tableName is optional
      |  default table is 'default.ipservices'
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

    val p1  = """([^#]\S+)\s+(\d+)\/(\S+)""".r
    val p2  = """([^#]\S+)\s+(\d+)\/(\S+)\s+([^#]\S+)""".r
    val p3  = """([^#]\S+)\s+(\d+)\/(\S+)\s+#\s(.+)""".r
    val p4  = """([^#]\S+)\s+(\d+)\/(\S+)\s+(\S+)\t+#\s(.*)""".r

    val svcdf = spark.sparkContext
      .textFile(args(0))
      .map( line => {

      .filter(svc => svc.port > 0)
      .toDS()

    svcdf.write.format("parquet").mode(SaveMode.Overwrite).saveAsTable(dbName)
    println("Finished.")

    spark.stop
  }

}
