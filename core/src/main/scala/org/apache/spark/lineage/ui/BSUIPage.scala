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
    val content =
      <div>
        <h5>
          {"Debugging Application : " + parent.getSparkConf().get("spark.app.name")}
        </h5>
        <div id="initSize">
          {size}
        </div>
        <div id="initJobTime">
          {jobtime}
        </div>
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
        <script src={UIUtils.prependBaseUri("/static/bigsift.js")}></script>
        <script src={UIUtils.prependBaseUri("/static/amcharts/Chart.min.js")}></script>
        <script src={UIUtils.prependBaseUri("/static/amcharts/amcharts.js")}></script>
        <script src={UIUtils.prependBaseUri("/static/amcharts/serial.js")}></script>
        <script src={UIUtils.prependBaseUri("/static/amcharts/themes/black.js")}></script>
        <script src={UIUtils.prependBaseUri("/static/amcharts/plugins/dataloader/dataloader.min.js")}></script>
        <script src={UIUtils.prependBaseUri("/static/jquery.min.js")}></script>
        <script src={UIUtils.prependBaseUri("/static/jquery-1.11.1.min.js")}></script>
        <script src={UIUtils.prependBaseUri("/static/amcharts/fl_chart.js")}></script>
    }
    BigSiftWebUI.basicSparkPage(content, title, onLoad = s"initbsWebSocket();initChart()", headers)
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
}



