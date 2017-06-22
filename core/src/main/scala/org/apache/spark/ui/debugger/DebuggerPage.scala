package org.apache.spark.ui.debugger

import javax.servlet.http.HttpServletRequest

import org.apache.commons.lang3.StringEscapeUtils
import org.apache.spark.bdd._
import org.apache.spark.ui.scope.{RDDOperationCluster, RDDOperationNode, RDDOperationGraph}
import org.apache.spark.ui.scope.RDDOperationGraph._
import org.apache.spark.ui.{ToolTips, UIUtils, WebUIPage}

import scala.collection.mutable.StringBuilder
import scala.xml.{NodeSeq, Node}


private[ui] class DebuggerPage(parent: DebuggerTab) extends WebUIPage("") {
	private val listener = parent.listener

	def render(request: HttpServletRequest): Seq[Node] = {
		val doUrl = "%s/debugger/do".format(UIUtils.prependBaseUri(parent.basePath))
		val resume = doUrl + "?command=resume"
		val stepover = doUrl + "?command=stepover"

		//      var dumpWP = "%s/debugger/".format(UIUtils.prependBaseUri(parent.basePath)) + "watchpoint"
		//        val renderedContentForStages = objectManager.renderContentForStages(renderStageObject)

		// val regex_prev = TaskExecutionManager.getExpression
		// val current_Crash: Tuple4[String, Int, Int, Int] = TaskExecutionManager.getCurrentCrashCulprit
		//  println(current_Crash.toString())
		// val path_d3_js = UIUtils.prependBaseUri("/static/d3.min.js")
		// val path_debugger_js = UIUtils.prependBaseUri("/static/bdd/js/debugger.js")


		val path_dag_viz = UIUtils.prependBaseUri("/static/spark-dag-viz.js")
		val fileContent = scala.io.Source.fromFile(parent.getSparkContext.lc.getBigDebugConfiguration().SOURCECODE_PATH).mkString
		val filename = parent.getSparkContext.lc.getBigDebugConfiguration().SOURCECODE_PATH.split("/").last


		//  println(current_Crash)
		val operationGraphListener = parent.operationGraphListener
		var content: Seq[Node] =
			<div>
				<div id="container2">
					<div id="container1">
						<div id="col1">

							<!--      <script src={path_d3_js} charset="utf-8"></script>
          <script src={path_debugger_js} charset="utf-8"></script>
          <script src={path_dag_viz} charset="utf-8"></script>
-->

							<div class="well well-large" style="text-align:center;border:1px solid red">
								<h4>Breakpoint Controls</h4>
								<div>
									<a class="btn  btn-success" type="button" href={resume}>Resume</a>
									<a class="btn btn-success" type="button" href={stepover}>Step Over</a>
								</div>
								<br/>
								<div>
									<input type="hidden" id="crashlineinfo" value={getCrashLineNumber()}></input>
									<input type="hidden" id="breaklineinfo" value={getLineNumber(TaskExecutionManager.getCurrentBreakpointRDDDetails())}></input> <h4>Current Breakpoint location is after the
									{TaskExecutionManager.getCurrentBreakpointRDDDetails()}
								</h4>
								</div>
							</div>
							<br/>
							<br/>
							<div>
								{UIUtils.showDagVizForJob(
								0, operationGraphListener.getOperationGraphForJob(0))}
							</div>
							<input type="hidden" id="websocketport" name="portws"
							       value={parent.getDriverWebSocketPort}/>
						</div>

						<div id="col2">
							<div>
								<h3>
									{filename}
								</h3>
							</div>
							<div>

								<textarea id="code" name="code" style="display: none;">
									{fileContent}
								</textarea>
							</div>
						</div>

					</div>
				</div>
				<h3>Task Level Latency</h3>
			</div>

		content = content ++ renderedTaskProfiling
		content = content ++ <div class="well well-large" style="text-align:center;border:1px solid red">
			<br/>
			<h4>Dump WatchPoints</h4>{renderWatchpointObject}
		</div>

		val wstype = 2
		UIUtils.headerSparkPage("Debugger", content, parent, onload = s"createCode();initWebSocket($wstype);chartRender()")
	}

	def renderedTaskProfiling(): Seq[Node] = {
		val title = "Task Execution Time  "
		val content =
			<div>
				<br/>
				<br/>
				<br/>
				<div id="chartContainer" style="height: 400px; width: 700px;">
				</div>
			</div>
		content
	}

	def renderWatchpointObject(): Seq[Node] = {
		val watchpoitnKeys = TaskExecutionManager.getWPKeys
		def dropRow(wp: (Int, Int)) = Seq[Node] {
			val dumpWP = "%s/debugger/".format(UIUtils.prependBaseUri(parent.basePath)) + "watchpoint?id=" + wp._2.toString + "&tid=" +
				wp._1.toString
			<li>
				<a href={dumpWP}>Dump Watch Point
					{wp._2.toString}
					From Tash
					{wp._1.toString}
				</a>
			</li>
		}
		val output: Seq[Node] = <div>
			<div class="dropdown">
				<button class="btn btn-success dropdown-toggle" type="button" data-toggle="dropdown">Select WatchPoint RDD to See Tapped Data
					<span class="caret"></span>
				</button>
				<ul class="dropdown-menu">
					{watchpoitnKeys.map(r => dropRow(r))}
				</ul>
			</div>
		</div>

		output
	}

	def getLineNumber(str: String): String = {
		if (str.contains("scala:")) {
			val start = str.indexOf("scala:")
			val v = str.substring(start + 6, str.length)
			return v.trim()
		} else {
			return "0"
		}
	}

	def getCrashLineNumber(): String = {
		val seq = TaskExecutionManager.crashedRDDList()
		var str = ""
		for (rdd <- seq) {
			val rdd_str = TaskExecutionManager.getRDDDetails(rdd)
			val start = rdd_str.indexOf("scala:")
			val v = rdd_str.substring(start + 6, rdd_str.length)
			str = str + v.trim() + " "
		}
		return str
	}
}

object DebuggerPageUtils {
	def showDagVizForJob(jobId: Int, graphs: Seq[RDDOperationGraph]): Seq[Node] = {
		showDagViz(graphs, forJob = true)
	}

	def makeDotFile(graph: RDDOperationGraph): String = {
		val dotFile = new StringBuilder
		dotFile.append("digraph G {\n")
		makeDotSubgraph(dotFile, graph.rootCluster, indent = "  ")
		graph.edges.foreach { edge => dotFile.append( s"""  ${edge.fromId}->${edge.toId};\n""")}
		dotFile.append("}")
		val result = dotFile.toString()
		result
	}

	/**
	 * Return a "DAG visualization" DOM element that expands into a visualization on the UI.
	 *
	 * This populates metadata necessary for generating the visualization on the front-end in
	 * a format that is expected by spark-dag-viz.js. Any changes in the format here must be
	 * reflected there.
	 */

	private def showDagViz(graphs: Seq[RDDOperationGraph], forJob: Boolean): Seq[Node] = {
		<div>
			<span id={if (forJob) "job-dag-viz" else "stage-dag-viz"}
			      class="expand-dag-viz" onclick={s"toggleDagViz($forJob);"}>
				<span class="expand-dag-viz-arrow arrow-closed"></span>
				<a data-toggle="tooltip" title={if (forJob) ToolTips.JOB_DAG else ToolTips.STAGE_DAG}
				   data-placement="right">
					DAG Visualization
				</a>
			</span>
			<div id="dag-viz-graph"></div>
			<div id="dag-viz-metadata" style="display:none">
				{graphs.map { g =>
				val stageId = g.rootCluster.id.replaceAll(RDDOperationGraph.STAGE_CLUSTER_PREFIX, "")
				val skipped = g.rootCluster.name.contains("skipped").toString
				<div class="stage-metadata" stage-id={stageId} skipped={skipped}>
					<div class="dot-file">
						{DebuggerPageUtils.makeDotFile(g)}
					</div>{g.incomingEdges.map { e => <div class="incoming-edge">
					{e.fromId}
					,
					{e.toId}
				</div>
				}}{g.outgoingEdges.map { e => <div class="outgoing-edge">
					{e.fromId}
					,
					{e.toId}
				</div>
				}}{g.rootCluster.getCachedNodes.map { n =>
					<div class="cached-rdd">
						{n.id}
					</div>
				}}
				</div>
			}}
			</div>
			<script type="text/javascript">toggleDagViz(true);</script>
		</div>
	}

	/** Return the dot representation of a node in an RDDOperationGraph. */
	private def makeDotNode(node: RDDOperationNode): String = {
		val label = s"${node.name} [${node.id}]\n${node.callsite}"
		s"""${node.id} [label="${StringEscapeUtils.escapeJava(label)}"]"""
	}

	/** Update the dot representation of the RDDOperationGraph in cluster to subgraph. */
	private def makeDotSubgraph(subgraph: StringBuilder,
	                            cluster: RDDOperationCluster,
	                            indent: String): Unit = {
		val fillcolor =  if( cluster.name.equalsIgnoreCase("watchpoint")) "#99FF99" else if(cluster.name.equalsIgnoreCase("simultedBreakpoint")) "#ffff7f" else  "#A0DFFF"
		val strokecolor =  if( cluster.name.equalsIgnoreCase("watchpoint")) "#7ACC7A" else if(cluster.name.equalsIgnoreCase("simultedBreakpoint"))  "#cccc00"  else "#3EC0FF"
		var nextLink = "rdd"

		subgraph.append(indent).append(s"subgraph cluster${cluster.id} {\n")

		if(cluster.name.contains("Stage"))
		subgraph.append(indent).append( s"""  label="${StringEscapeUtils.escapeJava(cluster.name)}";\n""")
		else{
			val crashRdds = TaskExecutionManager.crashedRDDList();
			if(!crashRdds.contains(cluster.childNodes(0).id)) nextLink = "rddst"
			if( cluster.name.equalsIgnoreCase("watchpoint")) nextLink=  "watchpoint"
			if( cluster.name.equalsIgnoreCase("simultedBreakpoint")) nextLink=  ""
			val color = if(crashRdds.contains(cluster.childNodes(0).id)) "#fa6b6b" else fillcolor
			val stroke = if(crashRdds.contains(cluster.childNodes(0).id)) "#f72121" else strokecolor
			val link = "/debugger/"+nextLink+"?id="  + cluster.childNodes(0).id
			subgraph.append(indent + s"""  label="${cluster.name}" \n ${indent} url="${link}" style="fill:${color}; stroke:${stroke}";\n""")

		}
		cluster.childNodes.foreach { node =>
			subgraph.append(indent).append(s"  ${makeDotNode(node)};\n")
		}
		cluster.childClusters.foreach { cscope =>
			makeDotSubgraph(subgraph, cscope, indent + "  ")
		}
		subgraph.append(indent).append("}\n")
	}

}

