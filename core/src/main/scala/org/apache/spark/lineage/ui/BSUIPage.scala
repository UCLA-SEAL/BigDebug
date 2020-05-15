package org.apache.spark.lineage.ui
import java.util
import javax.servlet.http.HttpServletRequest

import org.apache.commons.lang3.StringEscapeUtils
import org.apache.spark.internal.Logging
import org.apache.spark.ui.scope.{RDDOperationCluster, RDDOperationGraph, RDDOperationNode}
import org.apache.spark.ui.{ToolTips, UIUtils, WebUIPage}

import scala.collection.mutable.StringBuilder
import scala.xml.{Node, Unparsed}

/**
  * Created by ali on 1/23/16.
  */

class BSUIPage(parent: BigSiftWebUI, listener: BigSiftUIListenerBus) extends WebUIPage("") with Logging {
  def render(request: HttpServletRequest): Seq[Node] = {
    val title = "BigSift -- Automated Debugging For Apache Spark "

    val doUrl = "%s/do".format(UIUtils.prependBaseUri(parent.getBasePath))
    val placeholder_code = "def test(record : Any) : Boolean = {\n" +
         "  //Implement Test function here\n" +
         "}"
    val data = listener.getFaultLocalizationJSONData()
    val outputdata = {
      if (listener.initialOutputViz.isDefined){
        listener.getOutputForViz(true).get
      }
      else s"""{ "key": ${BigSiftWebUI.UIDATA} , "data" : [{}]}"""
    }


    val jobtime = {
      if (listener.initialJobTime.isDefined) {
        <div class="alert alert-success" >
          <strong>Original Job Time :</strong>{listener.initialJobTime.get / 1000000l}{" seconds"}
        </div>
      } else {
        Seq.empty
      }
    }
    val size = {
      if (listener.initialSize.isDefined) {
        <div class="alert alert-success" >
          <strong>Initial Size of Fault-Inducing Inputs : </strong>{listener.initialSize.get}{" records"}
        </div>
      } else {
        Seq.empty
      }
    }
    val debugtime = {
      if (listener.totalLocalizationTime.isDefined) {
        <div class="alert alert-success">
          <strong>Total Debugging Time :</strong>{listener.totalLocalizationTime.get / 1000000l}{" seconds"}
        </div>
      } else {
        Seq.empty
      }
    }



    val output = {
      if (listener.initialOutput.isDefined) {
        <div class="alert alert-danger">
          <strong>Application Output </strong>
          <div class="panel">
          <textarea name="record">
            {listener.initialOutput.get.replaceAll("~" , "\n")}
          </textarea>
          </div>
        </div>
      } else {
        Seq.empty
      }
    }


    val customStyle = "{height: 200px; width: 400px;}"
    val content =
      <div>
        <style type="text/css">
          .CodeMirror
          {customStyle}
        </style>
        <h5>
          {"Debugging Application : " + parent.getSparkConf().get("spark.app.name")}
        </h5>
        <div id="initSize">
          {size}
        </div>
        <div id="initJobTime">
          {jobtime}
        </div>

        <div id ="output">{output}</div>

        <div id="chartdivoutput"></div>

        <form method="GET" action={doUrl}>
          <div class="row-fluid">
          <div class="span6">
            <h5>Select one of the following test options: </h5>
            <br/>
            <label class="radio">
              <input type="radio" name="testoption" id="min" value="min" >Explain input records that lead to a minimum output</input>
              </label>

              <label class="radio">
                <input type="radio" name="testoption" id="max" value="max">Explain input records that lead to a maximum output</input>
                </label>

            <label class="radio">
              <input type="radio" name="testoption" id="5sigma" value="5sigma">Explain input records that lead to output values not in 5-Sigma range of median</input>
            </label>

            <label class="radio">
              <input type="radio" name="testoption" id="nan" value="nan">
                Explain input records that lead to a NaN or a Null</input>
            </label>

            <label class="radio">
              <input type="radio" name="testoption" id="udt" value="udt">
                Explain input records that lead to output values failing the test predicate in code box</input>
            </label>

          </div>
          <div class="span6">
            <h5>Write a test predicate below: </h5>
              <textarea id="code" name="code" style="display: none;">{placeholder_code}</textarea>
          </div>
        </div>
          <div class="col-md-4 text-center">
            <br/>
            <button type="submit" class="btn btn-large btn-success">Run BigSift!</button>
          </div>
        </form>
        <div id="finalDebugTime">
          {debugtime}
        </div>
        <div id="chartdiv"></div>
        <div>
          <table class="table table-condensed">

            <thead>
              <tr>
                <th>Fault-Inducing Input Records</th>
              </tr>
            </thead>
            <tbody id="tablebody">
              {if (listener.lastFaultInfo.isDefined) {
              listener.lastFaultInfo.get.topRecords.toIterator.map(s => dataRow(s))
            }}
            </tbody>
          </table>
        </div>
        <div>{BSUIPage.getPieDiv(listener)}</div>
        <input type="hidden" id="websocketport" name="portws" value={parent.getExecutorWebSocketPort.toString}/>
        <input type="hidden" id="initdata" name="initdata" value={data}/>
        <input type="hidden" id="initoutputdata" name="initoutputdata" value={outputdata}/>
        <script src={UIUtils.prependBaseUri("/static/piechart.js")}></script>
      </div>

    val headers = {
          <link rel="stylesheet" href={UIUtils.prependBaseUri("/static/amcharts/fl_chart.css")} type="text/css"/>
          <link rel="stylesheet" href={UIUtils.prependBaseUri("/static/codemirror.css")} type="text/css"/>
          <link rel="stylesheet" href={UIUtils.prependBaseUri("/static/theme/ambiance.css")} type="text/css"/>
          <link rel="stylesheet" href={UIUtils.prependBaseUri("/static/spark-dag-viz.css")} type="text/css"/>
          <link rel="stylesheet" href={UIUtils.prependBaseUri("/static/pie-dag.css")} type="text/css"/>
          <script src={UIUtils.prependBaseUri("/static/bigsift.js")}></script>
          <script src={UIUtils.prependBaseUri("/static/amcharts/Chart.min.js")}></script>
          <script src={UIUtils.prependBaseUri("/static/amcharts/amcharts.js")}></script>
          <script src={UIUtils.prependBaseUri("/static/amcharts/serial.js")}></script>
          <script src={UIUtils.prependBaseUri("/static/amcharts/themes/black.js")}></script>
            <script src={UIUtils.prependBaseUri("/static/amcharts/themes/light.js")}></script>
          <script src={UIUtils.prependBaseUri("/static/amcharts/plugins/dataloader/dataloader.min.js")}></script>
          <script src={UIUtils.prependBaseUri("/static/jquery.min.js")}></script>
          <script src={UIUtils.prependBaseUri("/static/amcharts/fl_chart.js")}></script>
          <script src={UIUtils.prependBaseUri("/static/codemirror.js")}></script>
          <script src={UIUtils.prependBaseUri("/static/mode/clike/clike.js")}></script>
          <script src={UIUtils.prependBaseUri("/static/addon/edit/matchbrackets.js")}></script>
          <script src={UIUtils.prependBaseUri("/static/addon/selection/active-line.js")}></script>
          <script src={UIUtils.prependBaseUri("/static/d3.js")}></script>
          <script src={UIUtils.prependBaseUri("/static/dagre-d3.min.js")}></script>
          <script src={UIUtils.prependBaseUri("/static/graphlib-dot.min.js")}></script>
          <script src={UIUtils.prependBaseUri("/static/spark-dag-viz.js")}></script>
          <script src={UIUtils.prependBaseUri("/static/pie-dag.js")}></script>
          <script src={UIUtils.prependBaseUri("/static/d3.v4.min.js")}></script>
          <script src={UIUtils.prependBaseUri("/static/viz-lite.js")}></script>
          <script src={UIUtils.prependBaseUri("/static/d3-graphviz.min.js")}></script>
    }
    BigSiftWebUI.basicSparkPage(content, title, onLoad = s"initbsWebSocket();initChart();initOutputChart();createCode();", headers)
  }


  def renderTableDiv(s: Seq[Node]): Seq[Node] = {
    <div>
      <h2>Captured Data Records</h2>
      <div id="table_wp" style="width:300px" class="bdd_panel">
        {s}
      </div>

      <input type="hidden" id="websocketport" name="portws"
             value={parent.getExecutorWebSocketPort.toString}/>
    </div>
  }


  def dataRow(str: String): Seq[Node] = {
    <tr class="error">
      <td>
        {str}
      </td>
    </tr>
  }

  def handleDebuggerCommand(request: HttpServletRequest): Unit = {
    val command: String = Option(request.getParameter("testoption")).getOrElse("")
    command match {
      case "min" =>
        listener.setTestType(1)
        logInfo("Use min test function")
        listener.notifyBigSiftWait()
      case "max" =>
        listener.setTestType(2)
        logInfo("Use max test function")
        listener.notifyBigSiftWait()
      case "nan" =>
        listener.setTestType(3)
        logInfo("Use nan test function")
        listener.notifyBigSiftWait()
      case "5sigma" =>
        listener.setTestType(4)
        logInfo("Use 5sigma test function")
        listener.notifyBigSiftWait()
      case "udt" =>
        logInfo("Use usedefined test function")
        listener.setTestType(5)
        val code = request.getParameter("code")
        listener.compilePredicate(code)
        listener.notifyBigSiftWait();
      case _ => {
        logInfo("Error : handleDebuggerCommand Invalid Command")
      }
    }
  }
}

object BSUIPage {
 var  listn : BigSiftUIListenerBus  = null

  def getPieDiv(listener: BigSiftUIListenerBus): Seq[Node] ={
    listn = listener
    if(listener.sparkContext.isDefined && listener.jobId!= -1) {
      <div>
        {showDagVizForJob(
        0, listener.sparkContext.get.ui.get.operationGraphListener.getOperationGraphForJob(listener.jobId))}
      </div>
    }else{
      Seq.empty
    }
  }

  def showDagVizForJob(jobId: Int, graphs: Seq[RDDOperationGraph]): Seq[Node] = {
    showDagViz(graphs, forJob = true)
  }

  def makeDotFile(graph: Seq[RDDOperationGraph]): String = {

    val list: util.ArrayList[Int] = new util.ArrayList[Int]()
    val dotFile = new StringBuilder
    dotFile.append(s""" digraph ""{\n """)
    graph.map { g =>
      makeDotSubgraph(dotFile, g.rootCluster, indent = "  " , list)
      g.edges.foreach {

        edge =>
          if(list.contains(edge.toId) || list.contains(edge.toId)){}else {
            dotFile.append( s"""  ${edge.fromId}->${edge.toId};\n""")
          }
      }
      dotFile.append("  ").append("}\n")
    }
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
      <span id={if (forJob) "job-data-dag-viz" else "stage-dag-viz"}
            class="expand-dag-viz" onclick={s"toggleDataDagViz($forJob);"}>
        <span class="expand-data-dag-viz-arrow arrow-closed"></span>
        <a data-toggle="tooltip" title={if (forJob) ToolTips.JOB_DAG else ToolTips.STAGE_DAG}
           data-placement="right">
          DAG Visualization  -- Intermediate Data Composition
        </a>
      </span>
      <div id="col3">
        <div id="graph" style="display:block"></div>
        <div id="dag-viz-metadata" style="display:none">
          <div class="stage-metadata">
            <div class="dot-file">{makeDotFile(graphs)}</div>
            {graphs.map{ g => g.incomingEdges.map { e => <div id="incoming-edge" style="display:none" >{e.fromId},{e.toId}</div>
          }}}
            {graphs.map{g => g.outgoingEdges.map { e => <div class="outgoing-edge">{e.fromId},{e.toId}</div>
          }}}
          </div>
        </div>
      </div>
    </div>
  }

  /** Return the dot representation of a node in an RDDOperationGraph. */
  private def makeDotNode(node: RDDOperationNode , name:String , list: util.ArrayList[Int]): String = {
    var red = 0.0f
  if(listn.dagInfo.isDefined) {
    var map = listn.dagInfo.get
    var green = -1.0f
    var set: Boolean = false
    var old = 0
    var tempg = -1f
    var tempf = -1f
    for ((id, t, f) <- map) {
      if (node.id == id-1) {
        green = (t - f).toFloat / t.toFloat
        red = f.toFloat / t.toFloat
      }
      if (node.id < id-1 && node.id > old-1) {
        green = tempg
        red = tempf
      }
      tempg = (t - f).toFloat / t.toFloat
      tempf = f.toFloat / t.toFloat
      old = id;
    }
  }

    val label = s"${name}"
    s"""${node.id} [label="${StringEscapeUtils.escapeJava(label)}" , fillcolor="lightcoral;${red}:palegreen" ]"""
  }
  /** Update the dot representation of the RDDOperationGraph in cluster to subgraph. */
  private def makeDotSubgraph(subgraph: StringBuilder,
                              cluster: RDDOperationCluster,
                              indent: String , list : util.ArrayList[Int]): Unit = {

    if(cluster.name.contains("Stage")) {
      subgraph.append(indent).append(s"subgraph cluster${cluster.id} {\n")
      subgraph.append(indent).append( s"""  label="${StringEscapeUtils.escapeJava(cluster.name)}"\n""")
      subgraph.append(indent).append( s"""  node [shape=circle, style="wedged"]\n""")
      cluster.childClusters.foreach { cscope =>
        makeDotSubgraph(subgraph, cscope, indent + "  " , list)
      }
    }else {
      cluster.childNodes.foreach { node =>
        if(!cluster.name.contains("tap"))
        subgraph.append(indent).append(s"  ${makeDotNode(node , cluster.name , list)};\n")
        else
          list.add(node.id)
      }
    }
  }

  /** Return a script element that automatically expands the DAG visualization on page load. */
  def expandDagVizOnLoad(forJob: Boolean): Seq[Node] = {
    <script type="text/javascript">
      {Unparsed("$(document).ready(function() { toggleDataDagViz(" + forJob + ") });")}
    </script>
  }
}




