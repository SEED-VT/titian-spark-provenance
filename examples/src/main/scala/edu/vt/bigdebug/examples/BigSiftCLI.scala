package edu.vt.bigdebug.examples

import org.apache.spark.SparkContext
import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._
import org.apache.spark.lineage.rdd.Lineage
import org.apache.spark.bigsift.{BigSift, BigSiftResult, TestOracle}

/**
 * Interactive terminal front-end for BigSift — a CLI take on the FSE '18 web UI.
 *
 *   bin/sbt 'examples/runMain edu.vt.bigdebug.examples.BigSiftCLI'            # menu / REPL
 *   bin/sbt 'examples/runMain edu.vt.bigdebug.examples.BigSiftCLI weather max' # one-shot
 *
 * Pick a subject program and a test-oracle function; BigSift streams the delta-debugging
 * progression live, then renders an area chart of records-localized over time (log y,
 * as in the paper) and a sample of the localized data.
 */
object BigSiftCLI {

  // ---- tiny ANSI helpers (no deps) ------------------------------------------
  private val ESC = "\u001b["
  private val ansi = System.console() != null || sys.env.get("TERM").exists(_.nonEmpty)
  private def c(code: String, s: String) = if (ansi) ESC + code + "m" + s + ESC + "0m" else s
  private def bold(s: String) = c("1", s)
  private def cyan(s: String) = c("36", s)
  private def green(s: String) = c("32", s)
  private def yellow(s: String) = c("33", s)
  private def dim(s: String) = c("2", s)
  private def red(s: String) = c("31", s)

  // ---- a debugging session's recorded progression --------------------------
  private case class Step(elapsedMs: Long, size: Int, sample: String)

  private final class Recorder {
    private val t0 = System.nanoTime()
    val steps = scala.collection.mutable.ArrayBuffer[Step]()
    def onStep(subset: Seq[String]): Unit = {
      val ms = (System.nanoTime() - t0) / 1000000
      val step = Step(ms, subset.size, subset.headOption.getOrElse(""))
      steps += step
      println(s"  ${dim(f"t=$ms%4dms")}  ${cyan(f"localized ${step.size}%4d")} records" +
        (if (step.sample.nonEmpty) s"   ${dim("e.g. " + step.sample)}" else ""))
    }
  }

  // ---- subject programs (jobs in serializable objects) ----------------------

  private object Jobs {
    /** airport: total layover per (airport, hour) for transits < 45 min. */
    def airport(in: Lineage[String]): Lineage[((String, String), Int)] =
      in.flatMap { s =>
        try {
          val t = s.split(","); val a = t(2).split(":"); val d = t(3).split(":")
          Iterator(((t(4), a(0)), (d(0).toInt * 60 + d(1).toInt) - (a(0).toInt * 60 + a(1).toInt)))
        } catch { case _: Throwable => Iterator.empty }
      }.filter(_._2 < 45).reduceByKey(_ + _)

    /** weather: snow delta (max-min) per month-date and per year. */
    def weather(in: Lineage[String]): Lineage[(String, Float)] =
      in.flatMap { s =>
        try {
          val t = s.split(","); val date = t(1); val v = t(2)
          val unit = v.substring(v.length - 2)
          val mm = v.substring(0, v.length - 2).toFloat * (if (unit == "mm") 1f else 304.8f)
          val slash = date.lastIndexOf("/")
          if (slash < 1) Iterator.empty
          else Iterator((date.substring(0, slash - 1), mm), (date.substring(slash), mm))
        } catch { case _: Throwable => Iterator.empty }
      }.groupByKey().map { case (k, vs) => (k, vs.max - vs.min) }

    /** student: moving-average age per grade. */
    def student(in: Lineage[String]): Lineage[(Int, Double)] =
      in.flatMap { line =>
        try { val l = line.split(" "); Iterator((l(4).toInt, l(3).toInt)) }
        catch { case _: Throwable => Iterator.empty }
      }.groupByKey().map { case (grade, ages) =>
        val it = ages.toIterator; var avg = 0.0; var n = 1
        while (it.hasNext) { avg = avg + (it.next() - avg) / n; n += 1 }
        (grade, avg)
      }
  }

  private case class Scenario(key: String, title: String, data: String,
                              oracles: Seq[(String, String)], default: String,
                              run: (String, LineageContext, Seq[String] => Unit) => Result)
  private case class Result(faulty: Seq[String], localized: Seq[String], provSize: Int)

  private def res[T](r: BigSiftResult[T]): Result =
    Result(r.faultyOutputs.map(_.toString), r.faultInducingInputs, r.provenanceSize)

  private val scenarios: Map[String, Scenario] = Seq(
    Scenario("airport", "Airport transit — total layover per airport-hour",
      "examples/data/airport.csv",
      Seq("negative" -> "layover total < 0 (default)", "min" -> "explain the minimum",
        "max" -> "explain the maximum", "ksigma" -> "k-sigma from median"),
      "negative",
      (oracle, lc, on) => {
        val in = lc.textFile("examples/data/airport.csv", 4); val bs = new BigSift(lc)
        type O = ((String, String), Int)
        res(oracle match {
          case "min" => bs.runWithOracle[O](in, Jobs.airport, TestOracle.min(_._2.toDouble), on)
          case "max" => bs.runWithOracle[O](in, Jobs.airport, TestOracle.max(_._2.toDouble), on)
          case "ksigma" => bs.runWithOracle[O](in, Jobs.airport, TestOracle.kSigmaFromMedian(2.0)(_._2.toDouble), on)
          case _ => bs.run[O](in, Jobs.airport, _._2 < 0, on)
        })
      }),
    Scenario("weather", "Weather analysis — snow delta per month/year",
      "examples/data/weather.csv",
      Seq("delta" -> "delta > 6000mm (default)", "max" -> "explain the maximum",
        "ksigma" -> "k-sigma from median"),
      "delta",
      (oracle, lc, on) => {
        val in = lc.textFile("examples/data/weather.csv", 4); val bs = new BigSift(lc)
        type O = (String, Float)
        res(oracle match {
          case "max" => bs.runWithOracle[O](in, Jobs.weather, TestOracle.max(_._2.toDouble), on)
          case "ksigma" => bs.runWithOracle[O](in, Jobs.weather, TestOracle.kSigmaFromMedian(2.0)(_._2.toDouble), on)
          case _ => bs.run[O](in, Jobs.weather, _._2 > 6000f, on)
        })
      }),
    Scenario("student", "Student info — moving-average age per grade",
      "examples/data/student.txt",
      Seq("anomaly" -> "grade > 3 or avg age outside [18,25] (default)"),
      "anomaly",
      (oracle, lc, on) => {
        val in = lc.textFile("examples/data/student.txt", 4); val bs = new BigSift(lc)
        type O = (Int, Double)
        res(bs.run[O](in, Jobs.student, o => o._1 > 3 || o._2 > 25 || o._2 < 18, on))
      })
  ).map(s => s.key -> s).toMap

  private def fmt(n: Long): String =
    if (n >= 1000000) s"${n / 1000000}M" else if (n >= 1000) s"${n / 1000}k" else n.toString

  // ---- area chart: records localized over time, log y with decade gridlines --
  private def areaChart(steps: Seq[Step]): String = {
    if (steps.isEmpty) return dim("  (no reduction steps)")
    // normalize time to the reduction phase (first step = 0): the capture/provenance
    // setup before the first reduction would otherwise flat-line most of the chart.
    val tMin = steps.map(_.elapsedMs).min
    val pts = steps.map(s => (s.elapsedMs - tMin, s.size))
    val tMax = pts.map(_._1).max.max(1L)
    val yMax = pts.map(_._2).max.max(1)
    val w = 54
    // log y, calibrated by decade: each 10x band gets `perDecade` rows of equal height,
    // so 1->10 spans the same vertical space as 100k->1M (proper log calibration).
    val perDecade = 2
    val decades = math.ceil(math.log10(yMax.toDouble)).toInt.max(1)
    val h = decades * perDecade
    def sizeAt(t: Long) = pts.takeWhile(_._1 <= t).lastOption.map(_._2).getOrElse(pts.head._2)
    val cols = (0 until w).map(ci => sizeAt(math.round(ci.toDouble / (w - 1) * tMax)))
    def barH(v: Int) = if (v <= 0) 0 else math.round(math.log10(v.toDouble) * perDecade).toInt
    val sb = new StringBuilder
    sb.append("  ").append(dim("records localized (log10 scale)")).append("\n")
    for (r <- h to 1 by -1) {
      // label only on decade boundaries (every `perDecade` rows): 10, 100, 1k, …
      val label = if (r % perDecade == 0) fmt(math.pow(10, r / perDecade).toLong) else ""
      val bar = cols.map(v => if (barH(v) >= r) green("█") else " ").mkString
      sb.append(f"  $label%6s │").append(bar).append("\n")
    }
    sb.append(f"  ${"1"}%6s └").append("─" * w).append("\n")  // baseline = 10^0 = 1 record
    sb.append(" " * 9).append("0").append(" " * (w - 10)).append(f"$tMax%5dms\n")
    sb.append(" " * 9).append(dim(s"delta-debugging time → (after ${tMin}ms capture+trace)")).append("\n")
    sb.toString
  }

  // ---- run one session ------------------------------------------------------
  private def runSession(scenKey: String, oracleKey: String): Unit = {
    val sc = scenarios.getOrElse(scenKey, { println(red(s"unknown scenario '$scenKey'")); return })
    val oracle = if (sc.oracles.exists(_._1 == oracleKey)) oracleKey else sc.default
    val total = scala.io.Source.fromFile(sc.data).getLines().size

    println()
    println(bold(cyan(s"▶ ${sc.title}")))
    println(dim(s"  data: ${sc.data}  ($total records)   oracle: ") + yellow(oracle))
    println(dim("  delta debugging — localizing…"))

    val spark = new SparkContext("local[4]", s"bigsift-cli-$scenKey")
    spark.setLogLevel("ERROR")
    val lc = new LineageContext(spark)
    val rec = new Recorder
    val t0 = System.nanoTime()
    val r = try sc.run(oracle, lc, rec.onStep) finally spark.stop()
    val ms = (System.nanoTime() - t0) / 1000000

    println()
    println(areaChart(rec.steps.toSeq))
    println(s"  ${bold("faulty output(s):")} ${r.faulty.take(3).mkString(", ")}" +
      (if (r.faulty.size > 3) dim(s"  (+${r.faulty.size - 3} more)") else ""))
    println(s"  ${bold("candidates (provenance):")} ${r.provSize} of $total records")
    println(s"  ${bold(green("fault-inducing input(s):"))}")
    r.localized.foreach(row => println(s"      ${green(row)}"))
    println(dim(s"  done in ${ms}ms over ${rec.steps.size} reduction steps"))
    println()
  }

  // ---- menu + REPL ----------------------------------------------------------
  private def menu(): Unit = {
    println(bold("\nBigSift CLI — automated fault localization"))
    println(dim("  pick a subject program and a test oracle; BigSift isolates the culprit\n"))
    scenarios.values.toSeq.sortBy(_.key).foreach { s =>
      println(s"  ${bold(yellow(s.key.padTo(9, ' ')))} ${s.title}")
      s.oracles.foreach { case (ok, desc) => println(s"      ${cyan(ok.padTo(10, ' '))} $desc") }
    }
    println()
    println(dim("  commands:  ") + s"${bold("run")} <scenario> [oracle]   ${bold("list")}   ${bold("help")}   ${bold("quit")}")
    println(dim("  example:   ") + "run weather max")
  }

  def main(args: Array[String]): Unit = {
    if (args.nonEmpty) { runSession(args(0), args.lift(1).getOrElse("")); return }
    menu()
    var go = true
    while (go) {
      print(bold("\nbigsift> "))
      Option(scala.io.StdIn.readLine()).map(_.trim) match {
        case None | Some("quit") | Some("exit") => go = false
        case Some("") =>
        case Some("help") | Some("list") => menu()
        case Some(line) =>
          val parts = line.split("\\s+").filter(_.nonEmpty).toList
          parts match {
            case "run" :: s :: rest => runSession(s, rest.headOption.getOrElse(""))
            case s :: rest if scenarios.contains(s) => runSession(s, rest.headOption.getOrElse(""))
            case _ => println(red(s"  ? unknown command: $line") + dim("   (try 'help')"))
          }
      }
    }
    println(dim("bye."))
  }
}
