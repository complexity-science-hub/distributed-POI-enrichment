// Copyright (C) 2019 Georg Heiler
package at.csh.geoheil.poi

import at.csh.geoheil.common.{SparkBaseRunner, SparkUtils}
import at.csh.geoheil.common.config.SpatialUtilsAll
import at.csh.geoheil.common.transformer.io.IO
import org.apache.spark.sql.{SaveMode, SparkSession}
import pureconfig.generic.auto._
import pureconfig.module.enumeratum._
import at.csh.geoheil.common.transformer.DataFrameExt._

object PoiEnrichmentBenchmark
    extends SparkBaseRunner[PoiEnrichmentBenchmarkConfiguration] {
  implicit val spark: SparkSession = createSparkSession(
    SpatialUtilsAll.useSpatialSerializers(
      createSparkConfiguration(c.applicationName)))
  SpatialUtilsAll.registerSpatialFunctions(spark)
  import spark.implicits._
  val DUMMY_DATA_LOCALITY_PATH_INPUT = "tmp_benchmark/dumm_locality_preserving"
  val DUMMY_DATA_RAW_INPUT = "tmp_benchmark/dumm_raw"

  val desiredNodes = OSMParser
    .loadOsmNodesAndFilterToDesiredKeys(c,
                                        c.poiEnrichment.osmFilterTags,
                                        c.poiEnrichment.osmAdditionalColumns)
    .transform(SparkUtils.namedCache("pois"))

  // TODO include multiple spark worker sizes and spatial partitioning variants
  val users = (1L to c.poiBenchmark.usersMax)
    .map(e => scala.math.pow(2.toDouble, e.toDouble).toLong)
    .filter(_ < c.poiBenchmark.usersMax)

  val resultsComplete = users.map(load => {
    println(s"using load: $load")
    WorkloadGenerator.generateWorkload(c.poiBenchmark,
                                       load,
                                       DUMMY_DATA_LOCALITY_PATH_INPUT,
                                       DUMMY_DATA_RAW_INPUT)
    val dummyDataLocalityPreserving =
      spark.read.parquet(DUMMY_DATA_LOCALITY_PATH_INPUT)
    val dummyDataRaw = spark.read.parquet(DUMMY_DATA_RAW_INPUT)

    // start benchmark and run each implementation variant for k times for consistency of results
    val timingDistributedWithExplode = PoiEnrichmentBenchmarkService.time(
      c.poiBenchmark.benchmarkRuns,
      "DistributedWithExplode",
      PoiEnrichmentBenchmarkService.distributedPOIEnrichmentWithExplode(
        dummyDataLocalityPreserving,
        desiredNodes,
        c.poiBenchmark)
    )
    val timingDistributedAlreadyExplodedNoLocalityPreservedOnlyInnerJoin =
      PoiEnrichmentBenchmarkService.time(
        c.poiBenchmark.benchmarkRuns,
        "DistributedAlreadyExplodedNoLocalityPreservedOnlyInnerJoin",
        PoiEnrichmentBenchmarkService
          .distributedPOIEnrichmentAlreadyExplodedNoLocalityPreservedOnlyInnerJoin(
            dummyDataRaw,
            desiredNodes,
            c.poiBenchmark)
      )
    val timingLocalityPreserving = PoiEnrichmentBenchmarkService.time(
      c.poiBenchmark.benchmarkRuns,
      "LocalityPreserving",
      PoiEnrichmentBenchmarkService.dataLocalityPreservingPOIEnrichment(
        dummyDataLocalityPreserving,
        desiredNodes,
        c.poiBenchmark)
    )

    val allResultsThisParameterCombination = timingDistributedWithExplode ++ timingDistributedAlreadyExplodedNoLocalityPreservedOnlyInnerJoin ++ timingLocalityPreserving

    val resultThisIteration = allResultsThisParameterCombination.map(r =>
      (TimingResultWithLoad(load, r)))
    println(
      s"*********** ############# intermediate stats for load ${load} ########## ***********")
    // immediate basic feedback
    val stats = resultThisIteration
      .groupBy(res => (res.results.key))
      .map(kv => {
        val values = kv._2.map(_.results.timing)
        (kv._1, avg(values), medianCalculator(values))
      })
    stats.foreach(println)
    IO.writeParquet(
      resultThisIteration.toDS.toDF.repartition(1),
      s"results/results_for_iteration.parquet/base_events_per_user=${c.poiBenchmark.baseNumberOfEvents}",
      SaveMode.Append,
      Some(Seq("load"))
    )
    resultThisIteration
  })

  handleStatistics

  private def handleStatistics()(implicit spark: SparkSession) = {
    println("*********** ############# results ########## ***********")
    // immediate basic feedback
    val stats = resultsComplete.flatten
      .groupBy(res => (res.results.key))
      .map(kv => {
        val values = kv._2.map(_.results.timing)
        (kv._1, avg(values), medianCalculator(values))
      })
    stats.foreach(println)

    // using spark & parquet. IT is simpler than manually serializing JSONs and worring about local vs. HDFS file writes
    val res = resultsComplete.flatten.toDS.toDF.flattenSchema()
    IO.writeParquet(
      res.repartition(1),
      s"results/current_results.parquet/base_events_per_user=${c.poiBenchmark.baseNumberOfEvents}",
      SaveMode.Overwrite,
      None)
    // write to JSON file
//    import org.json4s._
//    import org.json4s.jackson.Serialization.write
//    implicit val formats: DefaultFormats.type = DefaultFormats
//
//    val resultsCompleteJSON = write(resultsComplete.flatten)
//    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
//    val outFolder = new Path("phd_sim_results")
//    fs.mkdirs(outFolder)
////    val wd = os.pwd/"results"
////    os.makeDir.all(wd)
////    os.write(wd / "current_results.json", resultsCompleteJSON)
//    fs
  }

  private def avg(xs: Seq[Double]) = xs.sum / xs.length

  private def medianCalculator(seq: Seq[Double]): Double = {
    //In order if you are not sure that 'seq' is sorted
    val sortedSeq = seq.sortWith(_ < _)

    if (seq.size % 2 == 1) { sortedSeq(sortedSeq.size / 2) } else {
      val (up, down) = sortedSeq.splitAt(seq.size / 2)
      (up.last + down.head) / 2
    }
  }
}
