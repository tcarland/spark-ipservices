package com.trace3.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._

import io.delta.tables._


case class Service (
  name:     String,
  port:     Integer,
  proto:    String,
  aliases:  String,
  comment:  String
)


object IpServices {

  val usage : String =
    """
      |Usage: IpServices [input_services_file] [output_path]
      |   eg. s3a://bucket/services s3a://bucket/ipservices
    """.stripMargin


  def main ( args: Array[String] ) : Unit = {

    if ( args.length < 2 ) {
      println(usage)
      System.exit(0)
    }

    val output = args(1)

    val spark = SparkSession
      .builder
      .appName("IpServices")
      .config("spark.sql.parquet.compression.codec", "snappy")
      .getOrCreate()

    import spark.implicits._

    val p1  = """([^#]\S+)\s+(\d+)\/(\S+)""".r
    val p2  = """([^#]\S+)\s+(\d+)\/(\S+)\s+([^#]\S+)""".r
    val p3  = """([^#]\S+)\s+(\d+)\/(\S+)\s+#\s(.+)""".r
    val p4  = """([^#]\S+)\s+(\d+)\/(\S+)\s+(\S+)\t+#\s(.*)""".r

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

    //svcdf.write.mode(SaveMode.Overwrite).parquet(output)
    svcdf.write.format("delta").save(output)
    
    println("IpServices finished.")

    spark.stop
  }

}
