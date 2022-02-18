/*

spark-shell --jars /opt/cloudera/parcels/CDH/lib/phoenix_connectors/phoenix5-spark-shaded.jar,\
/opt/cloudera/parcels/CDH/lib/phoenix/phoenix-client-hbase-2.2.jar \
--conf "spark.executor.extraClassPath=/opt/cloudera/parcels/CDH/lib/phoenix_connectors/phoenix5-spark-shaded.jar:\
/opt/cloudera/parcels/CDH/jars/phoenix-core-5.1.1.7.1.7.24-1.jar" \
--conf "spark.driver.extraClassPath=/opt/cloudera/parcels/CDH/lib/phoenix_connectors/phoenix5-spark-shaded.jar:\
/opt/cloudera/parcels/CDH/jars/phoenix-core-5.1.1.7.1.7.24-1.jar"

 */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.phoenix.spark._

import java.sql.DriverManager
import java.sql.Connection

case class Service(
  name:     String,
  port:     Integer,
  proto:    String,
  aliases:  String,
  comment:  String
)

// config
val zkUrl  = "hadoop-mn-01-cdptpa:2181"
val schema = "tarland"
val table  = schema + ".ipservices"


val scheme = "create schema if not exists " + schema
val ddl    = "create table if not exists " + table + 
  " ( name varchar not null primary key, port integer, proto varchar, aliases varchar, comment varchar)"

val file   = "file:///etc/services"

val jdbc   = "jdbc:phoenix:" + zkUrl + "/hbase-unsecure"
val user   = "USER"
val pass   = "PASS"


// CREATE TABLE
val connection = DriverManager.getConnection(jdbc, user, pass)
val statement = connection.createStatement()
statement.executeUpdate(scheme)
statement.executeUpdate(ddl)


// Read file
import spark.implicits._

val p1  = """([^#]\S+)\s+(\d+)\/(\S+)""".r
val p2  = """([^#]\S+)\s+(\d+)\/(\S+)\s+([^#]\S+)""".r
val p3  = """([^#]\S+)\s+(\d+)\/(\S+)\s+#\s(.+)""".r
val p4  = """([^#]\S+)\s+(\d+)\/(\S+)\s+(\S+)\t+#\s(.*)""".r


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

// Write phx table
svcdf.write.
  format("org.apache.phoenix.spark").
  mode(SaveMode.Overwrite).
  option("table", table).
  option("zkUrl", zkUrl).
  save()


// Read phx table
df = spark.read.
  format("org.apache.phoenix.spark").
  option("table", table).
  option("zkUrl", zkUrl).
  load()

df.count()
df.show()

spark.stop()

