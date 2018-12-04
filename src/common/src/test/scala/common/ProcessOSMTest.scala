package osmesa.common

import cats.implicits._
import org.locationtech.geomesa.spark.jts._
import org.scalatest.{FunSpec, Matchers}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{lit, size => sqlSize}


class ProcessOSMTest extends FunSpec with TestEnvironment with Matchers {
  import ss.implicits._
  ss.withJTS

//  val orcFile = getClass.getResource("/isle-of-man-latest.osm.orc").getPath
  val orcFile = "/Users/yiqing_jin/data_dir/osm/orc/haiti-and-domrep-internal.osh.orc"

  val elements = ss.read.orc(orcFile)
  val nodes = ProcessOSM.preprocessNodes(elements).cache
  val nodeGeoms = ProcessOSM.constructPointGeometries(nodes).withColumn("minorVersion", lit(0)).cache
  val wayGeoms = ProcessOSM.reconstructWayGeometries(elements, nodes).cache
  val relationGeoms = ProcessOSM.reconstructRelationGeometries(elements, wayGeoms).cache

  nodeGeoms.printSchema()
  wayGeoms.printSchema()
  relationGeoms.printSchema()

  nodeGeoms
    .union(wayGeoms.drop('geometryChanged).where(sqlSize('tags) > 0))
    .union(relationGeoms)
    //        .withColumn("wkt", ST_AsText('geom))
    //        .drop('geom)
    .repartition(1)
    .write
    .mode(SaveMode.Overwrite)
    .orc("output/test2.orc")

  it("parses isle of man nodes") {
    info(s"Nodes: ${nodeGeoms.count}")
  }

  it("parses isle of man ways") {
    info(s"Ways: ${wayGeoms.count}")
  }

  it("parses isle of man relations") {
    info(s"Relations: ${relationGeoms.count}")
  }


}
