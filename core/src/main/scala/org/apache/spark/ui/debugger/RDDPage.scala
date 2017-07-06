package org.apache.spark.ui.debugger

import javax.servlet.http.HttpServletRequest

import org.apache.spark.bdd._
import org.apache.spark.ui.{UIUtils, WebUIPage}

import scala.xml.Node


/**
 * Created by ali on 1/19/16.
 */


/** Page showing statistics and task list for a given stage */
private[ui] class RDDPage(parent: DebuggerTab) extends WebUIPage("rdd") {
  private val listener = parent.listener
  private val doUrl = "%s/debugger/do".format(UIUtils.prependBaseUri(parent.basePath))
  RDDPage.URL = doUrl


  def render(request: HttpServletRequest): Seq[Node] = {
    val wpId = request.getParameter("id").toInt
    val cnt = request.getParameter("cnt") // See if this call is only for content
    /**
    Use RDD object here to extract runtime Type of f , if Possible
      */
    if (cnt != null) { // Call for only the content of the page
      val wpHeadersAndCssClasses: Seq[(String, String)] =
        Seq(
          ("Crashed Data Records ", "")
       //   , ("Actions", "")
        )
      val unzipped = wpHeadersAndCssClasses.unzip
      UIUtils.listingTable[CrashingRecord](
        unzipped._1,
        datarow,
        BDHandlerDriverSide.getCrashIterator(wpId).toIterable)

    } else {
      // retrieve only content not webapge.
      val content = {
        val wpHeadersAndCssClasses: Seq[(String, String)] =
          Seq(
            ("Crashed Data Records ", "")
         //   , ("Actions", "")
          )
        val unzipped = wpHeadersAndCssClasses.unzip
        renderTableDiv(
          UIUtils.listingTable[CrashingRecord](
            unzipped._1,
            datarow, BDHandlerDriverSide.getCrashIterator(wpId).toIterable)) ++ RDDPage.getCodeBox(wpId) /**05/12*/
      }

      val tableRenderLink = s""" "/debugger/rdd/?cnt=true&id=$wpId"  """
      val str_code = getRDDDetails(wpId)
      val wsType = 1 //RDD   doPoll(4000,$tableRenderLink )
      UIUtils.headerSparkPage(
        s"Details for $str_code ", content, parent,showVisualization = true, onload = s"initWebSocket($wsType, $wpId);createCode();") /***05/12 => createcode added**/
    }
  }

  def datarow(r:CrashingRecord): Seq[Node] = {

      <tr class="error">
        <td>
          <form method="GET" action={doUrl}>
            <input type="hidden" name="stage" value={r.stageID.toString}></input>
            <input type="hidden" name="task" value={r.taskID.toString}></input>
            <input type="hidden" name="subtask" value={r.rddid.toString}></input>
            <input type="hidden" name="srnum" value={r.srnumn.toString}></input>
            <input type="hidden" name="linid" value={r.lineageID.hashCode().toString}></input>
          <textarea name="record">
            {r.record}
          </textarea>
            {parent.getSparkContext.lc.getBigDebugConfiguration().CRASH_CULPRIT_RESOLUTION match {
            case 0 => "Skipped"
            case _ =>

              <button type="submit" class="btn btn-info" name="command" value="resolve">Modify</button>
                <button type="submit" class="btn btn-warning" name="command" value="skip">Skip</button>
                <button type="submit" class="btn btn-success" name="command" value="trace">Trace To Input</button>

          }}
          </form>
        </td>
      </tr>


  }

//  def renderResolutionOptions(r: (String, Int, Int, Int)): Seq[Node] = {
//
//
//  }


  def renderTableDiv(s: Seq[Node]): Seq[Node] = {
    <div>
      <h2>Crashed Records</h2>
      <div id="table_wp" style="width:600px" class="bdd_panel">
        {s}
      </div>
      <p>
        <br/>
        Configurations for crashed set at
        {parent.getSparkContext.lc.getBigDebugConfiguration().CRASH_CULPRIT_RESOLUTION match {
        case 0 => "skipping"
        case 1 => "sequential resolution"
        case 2 => "lazy resolution"
        case _ => "Invalid configuration"
      }}
      </p>
      <input type="hidden" id="websocketport" name="portws"
             value={parent.getDriverWebSocketPort}/>
    </div>
  }

  def getRDDDetails(id: Int): String = {
    val rdd = BDHandlerDriverSide.getRDDFromId(id)
    if (rdd != null) {
      return rdd.getCreationSite
    }
    ""
  }

  /**
   * To extract watchpoint code from the file
   **/
  def getWatchPointCode(id: Int): String = {
    val this_wp = BDHandlerDriverSide.extractWatchpointRDD(id).getCreationSite
    println(this_wp)
    val start = this_wp.substring(this_wp.indexOf(":") + 1).toInt
    println(start)
    val next = BDHandlerDriverSide.extractWatchpointRDD(id + 1).getCreationSite
    println(next)
    val end = next.substring(next.indexOf(":") + 1).toInt
    println(end)
    var str = ""
    var i = 1;


    for (line <- scala.io.Source.fromFile("/home/ali/work/temp/git/bigdebug/spark-lineage/examples/src/main/scala/org/apache/spark/examples/SparkWordCountTest.scala").getLines()) {
      if (i >= start && i < end) {
        str = str + "\n"
      }
      i = i + 1
    }
    str.trim()
  }

  def renderWatchpointObject(wpObject: (Int, Int)): Seq[Node] = {
    var dumpWP = "%s/debugger/".format(UIUtils.prependBaseUri(parent.basePath)) + "watchpoint?id=" + wpObject._2.toString + "&tid=" +
      wpObject._1.toString
    <div>
      <a class="btn btn-mini btn-success" type="button" href={dumpWP}>Dump Watch Point
        {wpObject._2.toString}
        From Tash
        {wpObject._1.toString}
      </a>
    </div>
  }
}

object RDDPage{
  var URL = "";
  def getDisabledLines(): Seq[Node] = {
    <input type="hidden" id="disablelines" value={PredicateClassVersion.codelinesst.map(_.toString).reduce(_ + "," + _)}></input>
  }
def renderContent(wpId: Int, conf : BDConfiguration) :  Seq[Node]= {
  val wpHeadersAndCssClasses: Seq[(String, String)] =
    Seq(
      ("Crashed Data Records ", "")
    )

  def datarow(r: CrashingRecord): Seq[Node] = {
    <tr class="error">
      <td>
        <form method="GET" action={URL}>
          <input type="hidden" name="stage" value={r.stageID.toString}></input>
          <input type="hidden" name="task" value={r.taskID.toString}></input>
          <input type="hidden" name="subtask" value={r.rddid.toString}></input>
          <input type="hidden" name="srnum" value={r.srnumn.toString}></input>
          <input type="hidden" name="linid" value={r.lineageID.hashCode().toString}></input>

          <textarea name="record">
            {r.record}
          </textarea> <span class="tab"></span>
          {conf.CRASH_CULPRIT_RESOLUTION match {
          case 0 => "Skipped"
          case _ =>

            <button type="submit" class="btn btn-info" name="command" value="Modify">Modify</button>
              <button type="submit" class="btn btn-warning" name="command" value="skip">Skip</button>
              <button type="submit" class="btn btn-success" name="command" value="trace">Trace to Input</button>

        }}
        </form>
      </td>
    </tr>
  }


  val unzipped = wpHeadersAndCssClasses.unzip
  UIUtils.listingTable[CrashingRecord](
    unzipped._1,
    datarow,
    BDHandlerDriverSide.getCrashIterator(wpId).toIterable)
}

def getCodeBox(rddID: Int) : Seq[Node]= { /**05/12**/
  val customStyle = "{height: 300px; width: 600px;}"
  val content =
    <div>
      <style type="text/css">
        .CodeMirror {customStyle}
      </style>
      <h3>Remediate Crashing Records In Batch</h3>
      <form method="GET" action={URL}>
        <input type="hidden" name="command" value="batchmodifyfunction"></input>
        <input type="hidden" name="rddid" value={rddID.toString}></input>
        <textarea id="code" name="code" style="display: none;">{PredicateClassVersion.code_template_st}
        </textarea>
        <br/>
        <button type="submit" class="btn btn-lg btn-success" >Batch Modification</button>
      </form>
    </div><div>{getDisabledLines()}</div>
  content
}


}