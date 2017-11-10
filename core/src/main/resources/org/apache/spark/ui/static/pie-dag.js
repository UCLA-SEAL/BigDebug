/*
 * Show or hide PIE DAG visualization.
 */
function toggleDataDagViz(forJob) {
    var arrowSelector = ".expand-data-dag-viz-arrow";
    $(arrowSelector).toggleClass('arrow-closed');
    $(arrowSelector).toggleClass('arrow-open');
    var shouldShow = $(arrowSelector).hasClass("arrow-open");
    if (shouldShow) {
        d3.select("#graph").style("display", "block");
        renderDataDAG();
        
    } else {
        d3.select("#graph").style("display", "none");
    }
    
}
