/*
 * Port of the original Titian acceptance suite (fork: lineage/src/test/.../LineageSuite).
 *
 * The Grep test keeps the fork's exact assertions: its constants are derived from file
 * offsets and record positions only, which are unchanged for the same input file.
 *
 * The WordCount and AverageLengthError tests originally asserted constants that embed
 * murmur3 key hashes AND partition assignments produced by the fork's globally-replaced
 * HashPartitioner (see TOUCHPOINT_INVENTORY.md C3 — the library deliberately does not
 * alter Spark's default partitioner). Those assertions are translated to check the same
 * semantics — which input records produced which outputs, traced backward and forward —
 * against the known content of the input file, instead of against hash constants.
 */
package org.apache.spark.lineage

import org.apache.spark.SparkContext
import org.apache.spark.lineage.LineageContext._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class LineageSuite extends AnyFunSuite with LocalLineageContext with Matchers {

  private val logFile = "src/test/resources/README.md"

  // show() on word-count-shaped traces renders "((<word>,<count>),<hash>)"; the word
  // itself may contain commas (e.g. "URL,").
  private val ShownRecord = """\(\((.*),(\d+)\),(-?\d+)\)""".r

  private def wordOf(shown: String): String = shown match {
    case ShownRecord(word, _, _) => word
  }

  test("Grep") {
    lc = new LineageContext(new SparkContext("local[2]", "grepTest"))

    lc.setCaptureLineage(true)

    // Job
    val lines = lc.textFile(logFile, 2)
    val result = lines.filter(line => line.contains("spark"))
    assert(result.collect().size == 11)

    lc.setCaptureLineage(false)

    // Trace backward one record
    var linRdd = result.getLineage()
    linRdd = linRdd.filter(_ == 0L)
    assert(linRdd.collect()(0) == (0, (0, 9)))
    linRdd = linRdd.goBack()
    assert(linRdd.collect()(0) == ((0, 9), 0))
    assert(linRdd.show.collect()(0) == "<http://spark.apache.org/>")

    // Trace forward one record
    linRdd = linRdd.goNext()
    assert(linRdd.collect()(0) == (0, 9))
  }

  test("WordCount") {
    lc = new LineageContext(new SparkContext("local[2]", "wordcountTest"))

    lc.setCaptureLineage(true)

    // Job
    val file = lc.textFile(logFile, 2)
    val pairs = file.flatMap(line => line.trim().split(" ")).map(word => (word.trim(), 1))
    val result = pairs.reduceByKey(_ + _)
    val output = result.collectWithId()
    assert(output.size == 268)

    lc.setCaptureLineage(false)

    // The two README lines that contain the word "examples"
    val expectedLines = Array(
      "You can set the MASTER environment variable when running examples to submit",
      "examples to a cluster. This can be a mesos:// or spark:// URL, ")

    // Find the output record for the word "examples" and its lineage id dynamically
    // (the fork hardcoded id 6232546377L and hash -2129110187).
    val examplesRecord = output.find(_._1._1 == "examples").get
    examplesRecord._1._2 should equal (2) // ("examples", 2)
    val examplesId = examplesRecord._2

    // Trace backward to the input lines
    var linRdd = result.getLineage()
    linRdd = linRdd.filter(_ == examplesId)
    assert(linRdd.collect().size == 1)
    assert(linRdd.show().collect()(0).startsWith("((examples,2)"))
    linRdd = linRdd.goBack()
    // One pre-shuffle contribution per input line containing the word
    assert(linRdd.collect().size == 2)
    linRdd = linRdd.goBack()
    assert(linRdd.collect().size == 2)
    linRdd.show().collect().toSet should equal (expectedLines.toSet)

    // Trace forward again: every word in those two lines flows to its count
    linRdd = linRdd.goNext()
    val expectedWords = expectedLines.flatMap(_.trim().split(" ")).map(_.trim()).toSet
    val shown = linRdd.show().collect()
    val shownWords = shown.map(wordOf).toSet
    shownWords should equal (expectedWords)

    linRdd = linRdd.goNext()
    val shownFinal = linRdd.show().collect()
    val finalWords = shownFinal.map(wordOf).toSet
    finalWords should equal (expectedWords)
    // Final counts are the true global counts: spot-check a couple
    shownFinal.find(_.startsWith("((examples,")).get should startWith ("((examples,2)")
    shownFinal.find(_.startsWith("((MASTER,")).get should startWith ("((MASTER,1)")
  }

  test("AverageLengthError") {
    lc = new LineageContext(new SparkContext("local[2]", "AverageLengthErrorTest"))

    lc.setCaptureLineage(true)

    // Job: average word length per first letter; values > 70 are marked faulty with "*"
    val file = lc.textFile(logFile, 2)
    val wordCount = file.flatMap(line => line.trim().split(" ")).filter(_.size > 0)
      .map(word => (word.substring(0, 1), word.length))
      .groupByKey()
      .map(pair => {
        val itr = pair._2.toIterator
        var returnedValue = 0
        var size = 0
        while (itr.hasNext) {
          val num = itr.next()
          returnedValue += num
          size += 1
        }
        (pair._1, returnedValue / size)
      })
      // this map marks the faulty result
      .map(pair => {
        var value = pair._2.toString
        if (pair._2 > 70) {
          value += "*"
        }
        (pair._1, value)
      })

    val result = wordCount.collectWithId()

    lc.setCaptureLineage(false)

    assert(result.size == 51)

    // Exactly one faulty output: the planted 109-char "V..." word skews group "V" to
    // (109 + 8 + 101) / 3 = 72
    val faulty = result.filter(_._1._2.endsWith("*"))
    faulty.map(_._1) should equal (Array(("V", "72*")))
    val list = faulty.map(_._2).toList

    // Trace backward to the culprit input line
    var linRdd = wordCount.getLineage()
    linRdd = linRdd.filter(s => list.contains(s))
    assert(linRdd.collect().size == 1)
    linRdd = linRdd.goBack()
    val grouped = linRdd.show().collect()
    grouped should have size 1
    // The faulty group: ("V", CompactBuffer(109, 8, 101)). CompactBuffer prints as
    // "Seq(...)" on Scala 2.13 (it printed "CompactBuffer(...)" on 2.11).
    grouped(0) should startWith ("((V,")
    Seq("109", "8", "101").foreach(len => grouped(0) should include (len))

    linRdd = linRdd.goBack()
    val contributions = linRdd.show().collect().toSet
    // Three (V, length) records contributed to the group
    contributions.map(_.stripPrefix("((").split("\\),").head).toSet should equal (
      Set("V,109", "V,8", "V,101"))

    linRdd = linRdd.goBack()
    linRdd.show().collect().toSet should equal (Set(
      "Vfasonfdsojfanjfosanfsaojfjasoidasjmdosjaoidjsaiodjasojoiasnvjdsfnasoidjsaiodjasoidjaoisdjsaonsajdjasiodjsaoi",
      "## A Note About Hadoop Versions",
      "[\"Specifying the Hadoop Version\"](http://spark.apache.org/docs/latest/building-with-maven.html#specifying-the-hadoop-version)"))

    // Trace forward: those three lines contribute to several first-letter groups
    linRdd = linRdd.goNext()
    val forwardWords = linRdd.show().collect()
    // Every record is a (firstLetter, length) pair drawn from words of the three lines
    val expectedLetters = Set("V", "#", "A", "N", "H", "[", "t")
    val forwardLetters = forwardWords.map(_.stripPrefix("((").split(",")(0)).toSet
    forwardLetters should equal (expectedLetters)

    linRdd = linRdd.goNext()
    val finalGroups = linRdd.show().collect()
    finalGroups.map(_.stripPrefix("((").split(",")(0)).toSet should equal (expectedLetters)
    finalGroups.find(_.startsWith("((V,")).get should include ("109")
  }
}
