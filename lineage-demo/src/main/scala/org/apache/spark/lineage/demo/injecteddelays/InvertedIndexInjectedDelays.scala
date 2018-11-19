package org.apache.spark.lineage.demo.injecteddelays

// Modified by Katherine on the base of BigSift Benchmark
// Modified further by Jason (jteoh) on 9/14/2018

import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._
import org.apache.spark.lineage.demo.LineageBaseApp
import org.apache.spark.lineage.rdd.Lineage

import scala.collection.mutable
import scala.collection.mutable.MutableList

object InvertedIndexInjectedDelays extends LineageBaseApp(
                                            threadNum = Some(6), // jteoh retained from original
                                            lineageEnabled = true,
                                            sparkLogsEnabled = false,
                                            sparkEventLogsEnabled = true,
                                            igniteLineageCloseDelay = 30 * 1000
                                            ) {
  var logFile: String = _
  val WITH_ARTIFICIAL_DELAY  = false
  override def initConf(args: Array[String], defaultConf: SparkConf): SparkConf = {
    // jteoh: only conf-specific configuration is this one, which might not be required for usual
    // execution.
    defaultConf.set("spark.executor.memory", "2g")
    logFile = args.headOption.getOrElse("/Users/jteoh/Documents/datasets/wiki_50GB_subset/part-00000")
    defaultConf.setAppName(s"${appName}-lineage:${lineageEnabled}-${logFile}")
  }
  override def run(lc: LineageContext, args: Array[String]): Unit = {
    try {
      //set up logging
      //val lm: LogManager = LogManager.getLogManager
      //val logger: Logger = Logger.getLogger(getClass.getName)
      //val fh: FileHandler = new FileHandler("myLog")
      //fh.setFormatter(new SimpleFormatter)
      //lm.addLogger(logger)
      //logger.setLevel(Level.INFO)
      //logger.addHandler(fh)
      
      //set up spark configuration
      // jteoh: deleted/refactored for lineage base app
      
      //start recording time for lineage
      /** ************************
       * Time Logging
       * *************************/
      //val jobStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      //val jobStartTime = System.nanoTime()
      //logger.log(Level.INFO, "JOb starts at " + jobStartTimestamp)
      /** ************************
       * Time Logging
       * *************************/
      
      val lines = lc.textFile(logFile, 1)
      val delayedInjectedLines = lines.map(injectDelays)
      val wordDoc: Lineage[(String, (String, mutable.Set[String]))] = delayedInjectedLines.flatMap(s => {
        val wordDocList: MutableList[(String, String)] = MutableList()
        val colonIndex = s.lastIndexOf("^")
        val docName = s.substring(0, colonIndex).trim()
        val content = s.substring(colonIndex + 1)
        val wordList = content.trim.split(" ")
        for (w <- wordList) {
          // Thread.sleep(500) jteoh: disabled for performance testing in baseline
          // the above is far too high too. Perhaps something smaller?
          // Thread.sleep(20)
          wordDocList += Tuple2(w, docName)
        }
        wordDocList.toList
      })
                                                                      .filter(r => filterSym(r._1))
                                                                      .map {
                      p =>
                        val docSet = scala.collection.mutable.Set[String]()
                        docSet += p._2
                        (p._1, (p._1, docSet))
                    }.reduceByKey {
        (s1, s2) =>
          val s = s1._2.union(s2._2)
          (s1._1, s)
      }
      //.filter(s => failure((s._1, s._2._2))) // jteoh: disabled because we don't have failures
      val output = Lineage.measureTimeWithCallback(wordDoc.collect,
                                           x => println(s"Collect time: $x ms"))
      
      /** ************************
       * Time Logging
       * *************************/
      //println(">>>>>>>>>>>>>  First Job Done  <<<<<<<<<<<<<<<")
      //val jobEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      //val jobEndTime = System.nanoTime()
      //logger.log(Level.INFO, "JOb ends at " + jobEndTimestamp)
      //logger.log(Level.INFO, "JOb span at " + (jobEndTime - jobStartTime) / 1000 + "milliseconds")
      /** ************************
       * Time Logging
       * *************************/
      /** ************************
       * Time Logging
       * *************************/
      //val DeltaDebuggingStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      //val DeltaDebuggingStartTime = System.nanoTime()
      //logger.log(Level.INFO, "Record DeltaDebugging + L  (unadjusted) time starts at " + DeltaDebuggingStartTimestamp)
      /** ************************
       * Time Logging
       * *************************/
      //val delta_debug = new DDNonExhaustive[String]
      //delta_debug.setMoveToLocalThreshold(local);
      //val returnedRDD = delta_debug.ddgen(lines, new Test, new SequentialSplit[String], lm, fh, DeltaDebuggingStartTime)
      
      /** ************************
       * Time Logging
       * *************************/
      //val DeltaDebuggingEndTime = System.nanoTime()
      //val DeltaDebuggingEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      //logger.log(Level.INFO, "DeltaDebugging (unadjusted) + L  ends at " + DeltaDebuggingEndTimestamp)
      //logger.log(Level.INFO, "DeltaDebugging (unadjusted)  + L takes " + (DeltaDebuggingEndTime - DeltaDebuggingStartTime) / 1000 + " milliseconds")
      /** ************************
       * Time Logging
       * *************************/
      //println(wordDoc.count())
      println("Job's DONE!")
    }
  }
  
  
  def failure(r: (String, scala.collection.mutable.Set[String])): Boolean = {
    (r._2.contains("hdfs://scai01.cs.ucla.edu:9000/clash/datasets/bigsift/wikipedia_50GB/file202") && r._1.equals("is"))
  }
  
  def filterSym(str: String): Boolean = {
    val sym: Array[String] = Array(">", "<", "*", "=", "#", "+", "-", ":", "{", "}", "/", "~", "1", "2", "3", "4", "5", "6", "7", "8", "9", "0")
    for (i <- sym) {
      if (str.contains(i)) {
        return false;
      }
    }
    return true;
  }
  
  private def injectDelays(s: String): String = {
    val colonIndex = s.lastIndexOf("^")
    //val docName = s.substring(0, colonIndex).trim()
    val content = s.substring(colonIndex + 1).trim
    // This time around, docName is insufficient because the subset I'm using all has the same
    // 'key'. Instead, we'll match on content (trimmed).
    content match {
      case line if line.startsWith("HINOJOSA: This study by Coleman, Reardon and associates for instance appeared in") =>
        // line 36
        // hdfs://scai01.cs.ucla.edu:9000/clash/datasets/bigsift/wikipedia_50GB/file100096k ^ HINOJOSA: This study by Coleman, Reardon and associates for instance appeared in the Canadian Medical Association Journal and concludes that low income women who abort are more likely to need psychiatric care later...and even through the Journal's editors defended the decision to publish the paper - they said it generated a barrage of letters.<br />
        Thread.sleep(15000)
      case line if line.startsWith("<li id=\"f-credits\">This page was last modified 15:04, 27 " +
                                     "May 2008 by Wikipedia user <a href=\"../../../." +
                                     "./articles/m/i/d/User%7EMidnightdreary_3c73.html\" " +
                                     "title=\"User:Midnightdreary\">Midnightdreary</a>.") =>
        // line 264328
        // hdfs://scai01.cs.ucla.edu:9000/clash/datasets/bigsift/wikipedia_50GB/file100096k ^ 	  	  	  <li id="f-credits">This page was last modified 15:04, 27 May 2008 by Wikipedia user <a href="../../../../articles/m/i/d/User%7EMidnightdreary_3c73.html" title="User:Midnightdreary">Midnightdreary</a>. Based on work by Wikipedia user(s) <a href="../../../../articles/l/i/g/User%7ELightmouse_cda0.html" title="User:Lightmouse">Lightmouse</a>, <a href="../../../../articles/c/h/r/User%7EChris_the_speller_fea2.html" title="User:Chris the speller">Chris the speller</a>, <a href="../../../../articles/h/a/p/User%7EHappyme22_be92.html" title="User:Happyme22">Happyme22</a>, <a href="../../../../articles/i/r/i/User%7EIridescent_15db.html" title="User:Iridescent">Iridescent</a>, <a href="../../../../articles/j/a/r/User%7EJareha_9da9.html" title="User:Jareha">Jareha</a>, <a href="../../../../articles/c/a/n/User%7ECanuckle_3c44.html" title="User:Canuckle">Canuckle</a>, C5mjohn, Orlandogirl, <a href="../../../../articles/r/i/-/User%7ERI-Bill_7c36.html" title="User:RI-Bill">RI-Bill</a>, <a href="../../../../articles/m/r/s/User%7EMrSomeone_9a91.html" title="User:MrSomeone">MrSomeone</a>, <a href="../../../../articles/e/z/e/User%7EEzeu_f517.html" title="User:Ezeu">Ezeu</a>, <a href="../../../../articles/b/i/g/User%7EBigturtle_52ef.html" title="User:Bigturtle">Bigturtle</a>, <a href="../../../../articles/k/r/a/User%7EKranar_drogin_56e2.html" title="User:Kranar drogin">Kranar drogin</a>, Winonave, <a href="../../../../articles/i/k/a/User%7EIkanreed_d2bc.html" title="User:Ikanreed">Ikanreed</a>, <a href="../../../../articles/v/a/l/User%7EValadius_948b.html" title="User:Valadius">Valadius</a>, <a href="../../../../articles/r/j/w/User%7ERjwilmsi_027f.html" title="User:Rjwilmsi">Rjwilmsi</a>, <a href="../../../../articles/s/n/i/User%7ESNIyer12_d7f4.html" title="User:SNIyer12">SNIyer12</a>, <a href="../../../../articles/a/e/l/User%7EAelfthrytha_8770.html" title="User:Aelfthrytha">Aelfthrytha</a>, <a href="../../../../articles/m/t/o/User%7EMToolen_f802.html" title="User:MToolen">MToolen</a>, <a href="../../../../articles/f/i/s/User%7EFishal_eaa8.html" title="User:Fishal">Fishal</a>, <a href="../../../../articles/d/u/a/User%7EDual_Freq_ca05.html" title="User:Dual Freq">Dual Freq</a>, Mewoulfe, <a href="../../../../articles/f/r/e/User%7EFreakofnurture_92d3.html" title="User:Freakofnurture">Freakofnurture</a>, <a href="../../../../articles/s/i/m/User%7ESimonP_1010.html" title="User:SimonP">SimonP</a>, <a href="../../../../articles/m/c/n/User%7EMcNeight_5a3c.html" title="User:McNeight">McNeight</a>, <a href="../../../../articles/b/h/l/User%7EBhludzin_20c6.html" title="User:Bhludzin">Bhludzin</a>, <a href="../../../../articles/h/o/l/User%7EHollyAm_2189.html" title="User:HollyAm">HollyAm</a>, <a href="../../../../articles/b/a/r/User%7EBartBenjamin_2993.html" title="User:BartBenjamin">BartBenjamin</a>, <a href="../../../../articles/e/o/g/User%7EEoghanacht_1bb8.html" title="User:Eoghanacht">Eoghanacht</a>, <a href="../../../../articles/r/o/g/User%7ERogerd_6b4d.html" title="User:Rogerd">Rogerd</a>, <a href="../../../../articles/p/a/t/User%7EPatrickD_fb55.html" title="User:PatrickD">PatrickD</a> and <a href="../../../../articles/a/n/d/User%7EAndreac_7f99.html" title="User:Andreac">Andreac</a> and Anonymous user(s) of Wikipedia.</li>	  <li id="f-copyright">All text is available under the terms of the <a class='internal' href="http://en.wikipedia.org/wiki/Wikipedia:Text_of_the_GNU_Free_Documentation_License" title="Wikipedia:Text of the GNU Free Documentation License">GNU Free Documentation License</a>. (See <b><a class='internal' href="http://en.wikipedia.org/wiki/Wikipedia:Copyrights" title="Wikipedia:Copyrights">Copyrights</a></b> for details.) <br /> Wikipedia&reg; is a registered trademark of the <a href="http://www.wikimediafoundation.org">Wikimedia Foundation, Inc</a>., a U.S. registered <a class='internal' href="http://en.wikipedia.org/wiki/501%28c%29#501.28c.29.283.29" title="501(c)(3)">501(c)(3)</a> <a href="http://wikimediafoundation.org/wiki/Deductibility_of_donations">tax-deductible</a> <a class='internal' href="http://en.wikipedia.org/wiki/Non-profit_organization" title="Non-profit organization">nonprofit</a> <a href="http://en.wikipedia.org/wiki/Charitable_organization" title="Charitable organization">charity</a>.<br /></li>	  <li id="f-about"><a href="../../../../articles/a/b/o/Wikipedia%7EAbout_8d82.html" title="Wikipedia:About">About Wikipedia</a></li>	  <li id="f-disclaimer"><a href="../../../../articles/g/e/n/Wikipedia%7EGeneral_disclaimer_3e44.html" title="Wikipedia:General disclaimer">Disclaimers</a></li>	  	</ul>
        Thread.sleep(10000)
      case line if line.startsWith("<div style=\"position: relative; width: 235px;\"><a href=\"." +
                                     "./../../." +
                                     "./articles/p/u/l/Image%7EPullman-car-D%26RG-101.jpg_150d" +
                                     ".html\" class=\"image\" title=\"Sepia tone image of the " +
                                     "Abraham Lincoln passenger car after restoration.\"><img " +
                                     "alt=\"Sepia tone image of the Abraham Lincoln passenger car" +
                                     " after restoration.\" src=\"../../../." +
                                     "./images/shared/thumb/d/db/Pullman-car-D&amp;" +
                                     "RG-101.jpg/235px-Pullman-car-D&amp;RG-101.jpg\" " +
                                     "width=\"235\" height=\"143\" border=\"0\" /></a></div>") =>
        // line 264386
        // hdfs://scai01.cs.ucla.edu:9000/clash/datasets/bigsift/wikipedia_50GB/file100096k ^ <div style="position: relative; width: 235px;"><a href="../../../../articles/p/u/l/Image%7EPullman-car-D%26RG-101.jpg_150d.html" class="image" title="Sepia tone image of the Abraham Lincoln passenger car after restoration."><img alt="Sepia tone image of the Abraham Lincoln passenger car after restoration." src="../../../../images/shared/thumb/d/db/Pullman-car-D&amp;RG-101.jpg/235px-Pullman-car-D&amp;RG-101.jpg" width="235" height="143" border="0" /></a></div>
        Thread.sleep(12000)
      case line if line.startsWith("<h2><span class=\"editsection\">[<a href=\"../../../." +
                                     "./articles/a/b/r/User_talk%7EAbrown36_0b52.html\" " +
                                     "title=\"Edit section: Notability of Jeff " +
                                     "Fort\">edit</a>]</span> <span " +
                                     "class=\"mw-headline\">Notability of <a href=\"../../../." +
                                     "./articles/j/e/f/Jeff_Fort_b22c.html\" title=\"Jeff " +
                                     "Fort\">Jeff Fort</a></span></h2>") =>
        // line 614028
        // hdfs://scai01.cs.ucla.edu:9000/clash/datasets/bigsift/wikipedia_50GB/file100096k ^ <h2><span class="editsection">[<a href="../../../../articles/a/b/r/User_talk%7EAbrown36_0b52.html" title="Edit section: Notability of Jeff Fort">edit</a>]</span> <span class="mw-headline">Notability of <a href="../../../../articles/j/e/f/Jeff_Fort_b22c.html" title="Jeff Fort">Jeff Fort</a></span></h2>
        Thread.sleep(20000)
      case line if line.startsWith("<p>Thanks for uploading Image:Salah.jpg. Wikipedia gets thousands of images uploaded every day, and in order to verify that the images can be legally used on Wikipedia,") =>
        // line 132427
        // hdfs://scai01.cs.ucla.edu:9000/clash/datasets/bigsift/wikipedia_50GB/file100096k ^ <p>Thanks for uploading Image:Salah.jpg. Wikipedia gets thousands of images uploaded every day, and in order to verify that the images can be legally used on Wikipedia, the source and copyright status must be indicated. Images need to have an <a href="../../../../articles/i/m/a/Wikipedia%7EImage_copyright_tags_f6dd.html" title="Wikipedia:Image copyright tags">image tag</a> applied to the <a href="../../../../articles/i/m/a/Wikipedia%7EImage_description_page_4215.html" class="mw-redirect" title="Wikipedia:Image description page">image description page</a> indicating the copyright status of the image. This uniform and easy-to-understand method of indicating the license status allows potential re-users of the images to know what they are allowed to do with the images.</p>
        Thread.sleep(25000)
      case _ => // noop
    }
    s
  }
}