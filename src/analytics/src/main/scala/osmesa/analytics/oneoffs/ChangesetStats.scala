package osmesa.analytics.oneoffs

import java.net.URI

import cats.implicits._
import com.monovore.decline.{CommandApp, Opts}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.locationtech.geomesa.spark.jts._
import osmesa.analytics.Analytics
import osmesa.common.ProcessOSM
import osmesa.common.functions._
import osmesa.common.functions.osm._

object ChangesetStats extends CommandApp(
  name = "changeset-stats",
  header = "Changeset statistics",
  main = {
    val historyOpt =
      Opts.option[String]("history", help = "Location of the History ORC file to process.")
    val changesetsOpt =
      Opts.option[String]("changesets", help = "Location of the Changesets ORC file to process.")
    val outputOpt =
      Opts.option[URI](long = "output", help = "Output URI prefix; trailing / must be included")

    (historyOpt, changesetsOpt, outputOpt).mapN { (historySource, changesetSource, output) =>
      implicit val spark: SparkSession = Analytics.sparkSession("ChangesetStats")
      import spark.implicits._

      @transient val idByVersion = Window.partitionBy('id).orderBy('version)

      val history = spark.read.orc(historySource)

      val nodes = history.where('type === "node")
        .withColumn("tags", when(!'visible and (lag('tags, 1) over idByVersion).isNotNull,
          lag('tags, 1) over idByVersion)
          .otherwise('tags))
        .withColumn("lat", when(!'visible, lit(Double.NaN)).otherwise('lat))
        .withColumn("lon", when(!'visible, lit(Double.NaN)).otherwise('lon))

      val ways = history.where('type === "way")
        .withColumn("tags", when(!'visible and (lag('tags, 1) over idByVersion).isNotNull,
          lag('tags, 1) over idByVersion)
          .otherwise('tags))

      val pointGeoms = ProcessOSM.geocode(ProcessOSM.constructPointGeometries(
        // pre-filter to POI nodes
        nodes.where(isPOI('tags))
      ).withColumn("minorVersion", lit(0)))

      val wayGeoms = ProcessOSM.geocode(ProcessOSM.reconstructWayGeometries(
        // pre-filter to interesting ways
        ways.where(isBuilding('tags) or isRoad('tags) or isWaterway('tags) or isCoastline('tags) or isPOI('tags)),
        // let reconstructWayGeometries do its thing; nodes are cheap
        history.where('type === "node")
      ).drop('geometryChanged))

      @transient val idByUpdated = Window.partitionBy('id).orderBy('updated)

      val augmentedWays = wayGeoms
        .withColumn("prevGeom", lag('geom, 1) over idByUpdated)
        .withColumn("delta",
          when(isRoad('tags) or isWaterway('tags) or isCoastline('tags),
            abs(
              coalesce(when(st_geometryType('geom) === "LineString", st_lengthSphere(st_castToLineString('geom))), lit(0)) -
              coalesce(when(st_geometryType('prevGeom) === "LineString", st_lengthSphere(st_castToLineString('prevGeom))), lit(0))
            ))
            .otherwise(lit(0)))

      val wayChangesetStats = augmentedWays
        .withColumn("road_m_added",
          when(isRoad('tags) and isNew('version, 'minorVersion), 'delta)
            .otherwise(lit(0)))
        .withColumn("road_m_modified",
          when(isRoad('tags) and not(isNew('version, 'minorVersion)) and 'visible, 'delta)
            .otherwise(lit(0)))
        .withColumn("road_m_deleted",
          when(isRoad('tags) and !'visible, 'delta)
            .otherwise(lit(0)))
        .withColumn("waterway_m_added",
          when(isWaterway('tags) and isNew('version, 'minorVersion), 'delta)
            .otherwise(lit(0)))
        .withColumn("waterway_m_modified",
          when(isWaterway('tags) and not(isNew('version, 'minorVersion)) and 'visible, 'delta)
            .otherwise(lit(0)))
        .withColumn("waterway_m_deleted",
          when(isWaterway('tags) and !'visible, 'delta)
            .otherwise(lit(0)))
        .withColumn("coastline_m_added",
          when(isCoastline('tags) and isNew('version, 'minorVersion), 'delta)
            .otherwise(lit(0)))
        .withColumn("coastline_m_modified",
          when(isCoastline('tags) and not(isNew('version, 'minorVersion)) and 'visible, 'delta)
            .otherwise(lit(0)))
        .withColumn("coastline_m_deleted",
          when(isCoastline('tags) and !'visible, 'delta)
            .otherwise(lit(0)))
        .withColumn("roads_added",
          when(isRoad('tags) and isNew('version, 'minorVersion), lit(1))
            .otherwise(lit(0)))
        .withColumn("roads_modified",
          when(isRoad('tags) and not(isNew('version, 'minorVersion)) and 'visible, lit(1))
            .otherwise(lit(0)))
        .withColumn("roads_deleted",
          when(isRoad('tags) and !'visible, lit(1))
            .otherwise(lit(0)))
        .withColumn("waterways_added",
          when(isWaterway('tags) and isNew('version, 'minorVersion), lit(1))
            .otherwise(lit(0)))
        .withColumn("waterways_modified",
          when(isWaterway('tags) and not(isNew('version, 'minorVersion)) and 'visible, lit(1))
            .otherwise(lit(0)))
        .withColumn("waterways_deleted",
          when(isWaterway('tags) and !'visible, lit(1))
            .otherwise(lit(0)))
        .withColumn("coastlines_added",
          when(isCoastline('tags) and isNew('version, 'minorVersion), lit(1))
            .otherwise(lit(0)))
        .withColumn("coastlines_modified",
          when(isCoastline('tags) and not(isNew('version, 'minorVersion)) and 'visible, lit(1))
            .otherwise(lit(0)))
        .withColumn("coastlines_deleted",
          when(isCoastline('tags) and !'visible, lit(1))
            .otherwise(lit(0)))
        .withColumn("buildings_added",
          when(isBuilding('tags) and isNew('version, 'minorVersion), lit(1))
            .otherwise(lit(0)))
        .withColumn("buildings_modified",
          when(isBuilding('tags) and not(isNew('version, 'minorVersion)) and 'visible, lit(1))
            .otherwise(lit(0)))
        .withColumn("buildings_deleted",
          when(isBuilding('tags) and !'visible, lit(1))
            .otherwise(lit(0)))
        .withColumn("pois_added",
          when(isPOI('tags) and isNew('version, 'minorVersion), lit(1))
            .otherwise(lit(0)))
        .withColumn("pois_modified",
          when(isPOI('tags) and not(isNew('version, 'minorVersion)) and 'visible, lit(1))
            .otherwise(lit(0)))
        .withColumn("pois_deleted",
          when(isPOI('tags) and !'visible, lit(1))
            .otherwise(lit(0)))
        .groupBy('changeset)
        .agg(
          sum('road_m_added / 1000).as('road_km_added),
          sum('road_m_modified / 1000).as('road_km_modified),
          sum('road_m_deleted / 1000).as('road_km_deleted),
          sum('waterway_m_added / 1000).as('waterway_km_added),
          sum('waterway_m_modified / 1000).as('waterway_km_modified),
          sum('waterway_m_deleted / 1000).as('waterway_km_deleted),
          sum('coastline_m_added / 1000).as('coastline_km_added),
          sum('coastline_m_modified / 1000).as('coastline_km_modified),
          sum('coastline_m_deleted / 1000).as('coastline_km_deleted),
          sum('roads_added).as('roads_added),
          sum('roads_modified).as('roads_modified),
          sum('roads_deleted).as('roads_deleted),
          sum('waterways_added).as('waterways_added),
          sum('waterways_modified).as('waterways_modified),
          sum('waterways_deleted).as('waterways_deleted),
          sum('coastlines_added).as('coastlines_added),
          sum('coastlines_modified).as('coastlines_modified),
          sum('coastlines_deleted).as('coastlines_deleted),
          sum('buildings_added).as('buildings_added),
          sum('buildings_modified).as('buildings_modified),
          sum('buildings_deleted).as('buildings_deleted),
          sum('pois_added).as('pois_added),
          sum('pois_modified).as('pois_modified),
          sum('pois_deleted).as('pois_deleted),
          count_values(flatten(collect_list('countries))) as 'countries
        )

      val pointChangesetStats = pointGeoms
        .withColumn("pois_added",
          when(isPOI('tags) and 'version === 1, lit(1))
            .otherwise(lit(0)))
        .withColumn("pois_modified",
          when(isPOI('tags) and 'version > 1 and 'visible, lit(1))
            .otherwise(lit(0)))
        .withColumn("pois_deleted",
          when(isPOI('tags) and !'visible, lit(1))
            .otherwise(lit(0)))
        .groupBy('changeset)
        .agg(
          sum('pois_added) as 'pois_added,
          sum('pois_modified) as 'pois_modified,
          sum('pois_modified) as 'pois_deleted,
          count_values(flatten(collect_list('countries))) as 'countries
        )

      // coalesce values to deal with nulls introduced by the outer join
      val rawChangesetStats = wayChangesetStats
        .withColumnRenamed("pois_added", "way_pois_added")
        .withColumnRenamed("pois_modified", "way_pois_modified")
        .withColumnRenamed("pois_deleted", "way_pois_deleted")
        .withColumnRenamed("countries", "way_countries")
        .join(pointChangesetStats
          .withColumnRenamed("pois_added", "node_pois_added")
          .withColumnRenamed("pois_modified", "node_pois_modified")
          .withColumnRenamed("pois_deleted", "node_pois_deleted")
          .withColumnRenamed("countries", "node_countries"),
          Seq("changeset"),
          "full_outer")
        .withColumn("pois_added",
          coalesce('way_pois_added, lit(0)) + coalesce('node_pois_added, lit(0)))
        .withColumn("pois_modified",
          coalesce('way_pois_modified, lit(0)) + coalesce('node_pois_modified, lit(0)))
        .withColumn("pois_deleted",
          coalesce('way_pois_deleted, lit(0)) + coalesce('node_pois_deleted, lit(0)))
        .withColumn("countries", merge_counts('node_countries, 'way_countries))
        .drop('way_pois_added)
        .drop('node_pois_added)
        .drop('way_pois_modified)
        .drop('node_pois_modified)
        .drop('way_pois_deleted)
        .drop('node_pois_deleted)
        .drop('way_countries)
        .drop('node_countries)

       val changesets = spark.read.orc(changesetSource)

      val changesetMetadata = changesets
        .groupBy('id, 'tags.getItem("created_by") as 'editor, 'uid, 'user, 'created_at, 'tags.getItem("comment") as 'comment)
        .agg(first('closed_at, ignoreNulls = true) as 'closed_at)
        .select(
          'id as 'changeset,
          'editor,
          'uid,
          'user as 'name,
          'created_at,
          'closed_at,
          hashtags('comment) as 'hashtags
        )

      val changesetStats = rawChangesetStats
        .join(changesetMetadata, Seq("changeset"), "outer")
        .withColumnRenamed("changeset", "id")
        .withColumn("road_km_added", coalesce('road_km_added, lit(0)))
        .withColumn("road_km_modified", coalesce('road_km_modified, lit(0)))
        .withColumn("road_km_deleted", coalesce('road_km_deleted, lit(0)))
        .withColumn("waterway_km_added", coalesce('waterway_km_added, lit(0)))
        .withColumn("waterway_km_modified", coalesce('waterway_km_modified, lit(0)))
        .withColumn("waterway_km_deleted", coalesce('waterway_km_deleted, lit(0)))
        .withColumn("coastline_km_added", coalesce('coastline_km_added, lit(0)))
        .withColumn("coastline_km_modified", coalesce('coastline_km_modified, lit(0)))
        .withColumn("coastline_km_deleted", coalesce('coastline_km_deleted, lit(0)))
        .withColumn("roads_added", coalesce('roads_added, lit(0)))
        .withColumn("roads_modified", coalesce('roads_modified, lit(0)))
        .withColumn("roads_deleted", coalesce('roads_deleted, lit(0)))
        .withColumn("waterways_added", coalesce('waterways_added, lit(0)))
        .withColumn("waterways_modified", coalesce('waterways_modified, lit(0)))
        .withColumn("waterways_deleted", coalesce('waterways_deleted, lit(0)))
        .withColumn("coastlines_added", coalesce('coastlines_added, lit(0)))
        .withColumn("coastlines_modified", coalesce('coastlines_modified, lit(0)))
        .withColumn("coastlines_deleted", coalesce('coastlines_deleted, lit(0)))
        .withColumn("buildings_added", coalesce('buildings_added, lit(0)))
        .withColumn("buildings_modified", coalesce('buildings_modified, lit(0)))
        .withColumn("buildings_deleted", coalesce('buildings_deleted, lit(0)))
        .withColumn("pois_added", coalesce('pois_added, lit(0)))
        .withColumn("pois_added", coalesce('pois_added, lit(0)))
        .withColumn("pois_modified", coalesce('pois_modified, lit(0)))
        .withColumn("pois_deleted", coalesce('pois_deleted, lit(0)))
        .repartition(1)
        .cache

      val changesetsCountriesTable = changesetStats
        .select('id as 'changeset_id, explode('countries) as Seq("country", "edit_count"))

      val changesetsHashtagsTable = changesetStats
        .select('id as 'changeset_id, explode('hashtags) as 'hashtag)

      val usersTable = changesetStats
        .select('id as 'changeset, 'uid as 'id, 'name)
        .groupByKey(_.getAs[Long]("id"))
        .mapGroups((id, rows) =>
          // get a user's last-used username
          (id, rows.map(x => (x.getAs[Long]("changeset"), x.getAs[String]("name"))).maxBy(_._1)._2))
        .toDF("id", "name")
        .repartition(1)

      val changesetsTable = changesetStats
        .withColumnRenamed("uid", "user_id")
        .drop('countries)
        .drop('hashtags)
        .drop('name)

      changesetsTable
        .write
        .mode(SaveMode.Overwrite)
        .csv(output.resolve("changesets").toString)

      changesetsCountriesTable
        .write
        .mode(SaveMode.Overwrite)
        .csv(output.resolve("changesets_countries").toString)

      changesetsHashtagsTable
        .write
        .mode(SaveMode.Overwrite)
        .csv(output.resolve("changesets_hashtags").toString)

      usersTable
        .write
        .mode(SaveMode.Overwrite)
        .csv(output.resolve("users").toString)

      spark.stop()
    }
  }
)
