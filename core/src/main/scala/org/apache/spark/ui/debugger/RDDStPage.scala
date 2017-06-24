package org.apache.spark.ui.debugger

import javax.servlet.http.HttpServletRequest

import org.apache.spark.bdd.{PredicateClassVersion, TaskExecutionManager}
import org.apache.spark.ui.{UIUtils, WebUIPage}

import scala.xml.Node

/**
 * Created by ali on 2/19/16.
 */


/** Page showing statistics and task list for a given stage */
private[ui] class RDDStPage(parent: DebuggerTab) extends WebUIPage("rddst") {
  private val listener = parent.listener
  private val doUrl = "%s/debugger/do".format(UIUtils.prependBaseUri(parent.basePath))


  def render(request: HttpServletRequest): Seq[Node] = {
    val wpId = request.getParameter("id").toInt
    val cnt = request.getParameter("cnt") // See if this call is only for content

    //println("ID :" + wpId)


      val customStyle = "{height: 300px; width: 600px;}"
      val content : Seq[Node] =
        <div>
          <style type="text/css">
            .CodeMirror {customStyle}
          </style>
          <h3>Runtime Code Fix</h3>
          <form method="GET" action={doUrl}>
            <input type="hidden" name="command" value="setnewrddfunc"></input>
            <input type="hidden" name="rddid" value={wpId.toString}></input>
            <textarea id="code" name="code" style="display: none;">{PredicateClassVersion.code_template_st}
            </textarea>
            <br/>
            <button type="submit" class="btn btn-lg btn-success" >Patch the code!</button>
          </form>
        </div>
      val str_code = getRDDDetails(wpId)
      val wsType = 1 //RDD   doPoll(4000,$tableRenderLink )
      UIUtils.headerSparkPage(
        s"Details for $str_code ", content,  parent,showVisualization = true, onload = s"createCode();")
  }

  //  def renderResolutionOptions(r: (String, Int, Int, Int)): Seq[Node] = {
  //
  //
  //  }

  def getRDDDetails(id: Int): String = {
    val rdd = TaskExecutionManager.getRDDFromId(id)
    if (rdd != null) {
      return rdd.getCreationSite
    }
    ""
  }


}
