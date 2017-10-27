package org.apache.spark.lineage.ui

import javax.servlet.http.HttpServletRequest

import org.apache.spark.internal.Logging
import org.apache.spark.ui.{UIUtils, WebUIPage}

import scala.xml.Node

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

        <div id ="output">
          {output}
        </div>

        <form method="GET" action={doUrl}>
          <div class="row-fluid">
          <div class="span6">
            <h5>Select one of the following test options: </h5>
            <br/>
            <br/>
            <label class="radio">
              <input type="radio" name="testoption" id="min" value="min" >Debug the minimum output value</input>
              </label>

              <label class="radio">
                <input type="radio" name="testoption" id="max" value="max">Debug the maximum output value</input>
                </label>

            <label class="radio">
              <input type="radio" name="testoption" id="5sigma" value="5sigma">Debug all output values that are not in 5-Sigma range of median</input>
            </label>

            <label class="radio">
              <input type="radio" name="testoption" id="nan" value="nan">
                Debug NaN and Null output values</input>
            </label>

            <label class="radio">
              <input type="radio" name="testoption" id="udt" value="udt">
                Debug output values that fail the test predicate in code box</input>
            </label>

          </div>
          <div class="span6">
            <h5>Write a test predicate below: </h5>
              <textarea id="code" name="code" style="display: none;">{placeholder_code}</textarea>
          </div>
        </div>
          <div class="col-md-4 text-center">
            <br/>
            <br/>
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

        <input type="hidden" id="websocketport" name="portws" value={parent.getExecutorWebSocketPort.toString}/>
        <input type="hidden" id="initdata" name="initdata" value={data}/>

      </div>
    val headers = {
        <link rel="stylesheet" href={UIUtils.prependBaseUri("/static/amcharts/fl_chart.css")} type="text/css"/>
               <link rel="stylesheet" href={UIUtils.prependBaseUri("/static/codemirror.css")} type="text/css"/>
               <link rel="stylesheet" href={UIUtils.prependBaseUri("/static/theme/ambiance.css")} type="text/css"/>

             <script src={UIUtils.prependBaseUri("/static/bigsift.js")}></script>
        <script src={UIUtils.prependBaseUri("/static/amcharts/Chart.min.js")}></script>
        <script src={UIUtils.prependBaseUri("/static/amcharts/amcharts.js")}></script>
        <script src={UIUtils.prependBaseUri("/static/amcharts/serial.js")}></script>
        <script src={UIUtils.prependBaseUri("/static/amcharts/themes/black.js")}></script>
        <script src={UIUtils.prependBaseUri("/static/amcharts/plugins/dataloader/dataloader.min.js")}></script>
        <script src={UIUtils.prependBaseUri("/static/jquery.min.js")}></script>
        <script src={UIUtils.prependBaseUri("/static/jquery-1.11.1.min.js")}></script>
        <script src={UIUtils.prependBaseUri("/static/amcharts/fl_chart.js")}></script>
             <script src={UIUtils.prependBaseUri("/static/codemirror.js")}></script>
             <script src={UIUtils.prependBaseUri("/static/mode/clike/clike.js")}></script>
             <script src={UIUtils.prependBaseUri("/static/addon/edit/matchbrackets.js")}></script>
             <script src={UIUtils.prependBaseUri("/static/addon/selection/active-line.js")}></script>
    }
    BigSiftWebUI.basicSparkPage(content, title, onLoad = s"initbsWebSocket();initChart();createCode();", headers)
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
        logInfo("Use min test function")
      case "max" =>
        logInfo("Use max test function")
      case "nan" =>
        logInfo("Use nan test function")
      case "5sigma" =>
        logInfo("Use 5sigma test function")
      case "udt" =>
        val code = request.getParameter("code")
        logInfo("Use usedefined test function")
        listener.notifyBigSiftWait();
      case _ => {
        logInfo("Error : handleDebuggerCommand Invalid Command")
      }
    }
  }
}




