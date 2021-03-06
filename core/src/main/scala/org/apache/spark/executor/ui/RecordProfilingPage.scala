package org.apache.spark.executor.ui

import javax.servlet.http.HttpServletRequest

import org.apache.spark.bdd.BDRecordProfiler
import org.apache.spark.internal.Logging
import org.apache.spark.ui.{UIUtils, WebUIPage}

import scala.xml.Node

/**
 * Created by ali on 1/23/16.
 */
class RecordProfilingPage(parent: ExecutorWebUI) extends WebUIPage("") with Logging {

	val wid = parent.executor.getExecutorID
	val interval = 3000

	def render(request: HttpServletRequest): Seq[Node] = {
		val rddid = request.getParameter("rdd")
		if (rddid == null) {
			renderWithOutRDD()
		} else {
			renderWithRDD(rddid.toInt)
		}
	}

	def renderWithRDD(rddid: Int): Seq[Node] = {
		val title = "Record Level Profiling on Executor : " + wid
		val content =
			<div>
				<br/>
				<br/>
				<br/>
				<div id="chartContainer" style="height: 400px; width: 700px;">
				</div>

				<input type="hidden" id="websocketport" name="portws" value={parent.getExecutorWebSocketPort.toString}/>
			</div>
		val wsType = 3
		UIUtils.basicSparkPage(content, title, onLoad = s"chartRender($rddid , $wid  ) ; initWebSocket($wsType, $rddid)")

	}

	def renderWithOutRDD(): Seq[Node] = {
		val title = "Record Level Profiling on Executor : " + wid
		if (BDRecordProfiler.getRDDs().isEmpty) {
			val content = <div>
				<br/>
				<br/>
				<br/>
				<div class="alert alert-warning" role="alert">
					<strong>Warning!</strong>
					No straggling data records captured in this task yet.
				</div>
			</div>
			UIUtils.basicSparkPage(content, title)
		} else {
			val content = <div>
				<br/>
				<br/>
				<br/>
				<div class="btn-group">
					<button class="btn btn-large btn-success dropdown-toggle" type="button" data-toggle="dropdown">Select Rdd to see profiled data
						<span class="caret"></span>
					</button>
					<ul class="dropdown-menu">
						{BDRecordProfiler.getRDDs().toIterable.map(r => dropRow(r))}
					</ul>
				</div>
			</div>
			UIUtils.basicSparkPage(content, title)
		}
	}

	def dropRow(rdd: Int) = Seq[Node] {
		val rddlink = s"""/?rdd=$rdd"""
		<li>
			<a href={rddlink}>
				{s"""RDD[$rdd]"""}
			</a>
		</li>
	}

	def renderData(request: HttpServletRequest): String = {
		val rddid = request.getParameter("rdd").toInt
		val title = "Record Level Profiling"
		BDRecordProfiler.getUiProfileData(rddid)
	}
}
