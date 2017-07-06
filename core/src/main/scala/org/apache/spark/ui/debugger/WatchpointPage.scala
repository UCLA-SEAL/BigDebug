package org.apache.spark.ui.debugger

/**
 * Created by ali on 12/21/15.
 */

import javax.servlet.http.HttpServletRequest

import org.apache.spark.bdd.{CapturedRecord, PredicateClassVersion, BDHandlerDriverSide}
import org.apache.spark.ui.{UIUtils, WebUIPage}

import scala.xml.Node

/** Page showing statistics and task list for a given stage */
private[ui] class WatchpointPage(parent: DebuggerTab) extends WebUIPage("watchpoint") {
	val doUrl = "%s/debugger/do".format(UIUtils.prependBaseUri(parent.basePath))
	private val listener = parent.listener

	def render(request: HttpServletRequest): Seq[Node] = {
		val wpId = request.getParameter("id").toInt
		val cnt = request.getParameter("cnt") // See if this call is only for content
		/**
		Use RDD object here to extract runtime Type of f , if Possible
		  */
		if (cnt != null) {
			val wpHeadersAndCssClasses: Seq[(String, String)] =
				Seq(
					("Captured Data Records", ""))
			val unzipped = wpHeadersAndCssClasses.unzip
			UIUtils.listingTable[CapturedRecord](
				unzipped._1,
				datarow,
				BDHandlerDriverSide.getWatchpointIterator(wpId).toIterable)

		} else {
			var content = {
				val wpHeadersAndCssClasses: Seq[(String, String)] =
					Seq(
						("Captured Data Records", ""))
				val unzipped = wpHeadersAndCssClasses.unzip
				renderTableDiv(
					UIUtils.listingTable[CapturedRecord](
						unzipped._1,
						datarow, BDHandlerDriverSide.getWatchpointIterator(wpId).toIterable))
			}

			val customStyle = "{height: 300px; width: 600px;}"
			content = content ++ {
				<div>
					<style type="text/css">
						.CodeMirror
						{customStyle}
					</style>
					<h3>Dynamic Watchpoint Guard</h3>
					<form method="GET" action={doUrl}>
						<input type="hidden" name="command" value="set_guard"></input>
						<input type="hidden" name="rddid" value={wpId.toString}></input>
						<textarea id="code" name="code" style="display: none;">{PredicateClassVersion.code_template}
						</textarea>
						<br/>
						<button type="submit" class="btn btn-lg btn-success">Submit New Guard</button>
					</form>
				</div>
					<div>
						{getDisabledLines()}
					</div>
			}
			val tableRenderLink = s""" "/debugger/watchpoint/?cnt=true&id=$wpId"  """
			val str_code = getWatchPointLineNumber(wpId)
			val wsType = 1
			UIUtils.headerSparkPage(
				s"Details for Watchpoint at Line Number:$str_code ", content, parent, showVisualization = true, onload = s"createCode();initWebSocket($wsType, $wpId)")
		}
	}

	def datarow(str: CapturedRecord): Seq[Node] = {
		<tr class="info">
			<td>
				{str.record}
			</td>
		</tr>

	}

	def getDisabledLines(): Seq[Node] = {
		<input type="hidden" id="disablelines" value={PredicateClassVersion.codelines.map(_.toString).reduce(_ + "," + _)}></input>
	}

	def renderTableDiv(s: Seq[Node]): Seq[Node] = {
		<div>
			<h2>Captured Data Records</h2>
			<div id="table_wp" style="width:300px" class="bdd_panel">
				{s}
			</div>

			<input type="hidden" id="websocketport" name="portws"
			       value={parent.getDriverWebSocketPort}/>
		</div>
	}


	def getWatchPointLineNumber(id: Int): String = {
		val this_wp = BDHandlerDriverSide.extractWatchpointRDD(id).getCreationSite
		this_wp.substring(this_wp.indexOf(":") + 1)
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
				From Task
				{wpObject._1.toString}
			</a>
		</div>
	}
}

object WatchpointPage {
	def renderContent(wpId: Int) = {
		val wpHeadersAndCssClasses: Seq[(String, String)] =
			Seq(
				("Captured Data Records", ""))
		val unzipped = wpHeadersAndCssClasses.unzip
		UIUtils.listingTable[CapturedRecord](
			unzipped._1,
			datarow,
			BDHandlerDriverSide.getWatchpointIterator(wpId).toIterable)

	}

	def datarow(str: CapturedRecord): Seq[Node] = {
		<tr class="info">
			<td>
				{str.record}
			</td>
		</tr>

	}
}
