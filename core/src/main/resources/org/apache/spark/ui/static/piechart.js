
var dot = "";
d3.select("#dag-viz-metadata").selectAll(".stage-metadata").each(function (d, i) {
        var metadata = d3.select(this);
         dot = metadata.select(".dot-file").text();
        console.log(dot);
}
);
var svgContainer = d3.select("#col3").select("#graph")//.append("svg")//.attr("id","dag");
                     //.attr("width", 300)
                    // .attr("height", 900);
var pieDagContainer = svgContainer.append("g").attr("id","pieDagContainer");
var graphviz = pieDagContainer.graphviz();

//receive cross stage edges
var crossStageEdges = [];
d3.selectAll("#incoming-edge").each(function (v) {
            var edge = d3.select(this).text().split(","); // e.g. 3,4 => [3, 4]
            crossStageEdges.push(edge);
        });
//render the PIE DAG
function renderDataDAG() {
    var transition1 = d3.transition()
        .ease(d3.easeLinear)
        .duration(0);

    graphviz
        .dot(dot)
        .transition(transition1)
        .render();

   drawCrossStageEdges(crossStageEdges, svgContainer);
   resizeDataSvg(svgContainer);
}

function resizeDataSvg(svg) {
    var allClusters = d3.select("#col3").select("#graph").select("polygon")["_groups"][0];
    var startX = -VizConstants.svgMarginX +
        toFloat(d3.min(allClusters, function (e) {
            return getAbsolutePositionPolygon(d3.select(e)).x;
        }));
    var startY = -VizConstants.svgMarginY +
        toFloat(d3.min(allClusters, function (e) {
            return getAbsolutePositionPolygon(d3.select(e)).y;
        }));
    var endX = VizConstants.svgMarginX +
        toFloat(d3.max(allClusters, function (e) {
            var t = d3.select(e);
            return getAbsolutePositionPolygon(t).width;
        }));
    var endY = VizConstants.svgMarginY +
        toFloat(d3.max(allClusters, function (e) {
            var t = d3.select(e);
            return getAbsolutePositionPolygon(t).height;
        }));
    var width = endX - startX;
    var height = endY - startY;
    svg.attr("viewBox", startX + " " + startY + " " + width + " " + height)
        .attr("width", width)
        .attr("height", height);
}

/*
 * Helper function to draw edges that cross stage boundaries.
 * We need to do this manually because we render each stage separately in dagre-d3.
 */
function drawCrossStageEdges(edges, svgContainer) {
    if (edges.length == 0) {
        return;
    }
    // Draw the paths first
    var edgesContainer = svgContainer.append("g").attr("id", "cross-stage-edges");
    for (var i = 0; i < edges.length; i++) {
        var fromRDDId = edges[i][0];
        var toRDDId = edges[i][1];
        connectRDDs(fromRDDId, toRDDId, edgesContainer, svgContainer);
    }
}


function getAbsolutePositionPolygon(d3selection) {


    if (d3selection.empty()) {
        throw "Attempted to get absolute position of an empty selection.";
    }
    var s = d3selection.attr("points");
    function pairwise(arr, func){
        for(var i=0;i<arr.length-1;i++){
            func(arr[i], arr[i+1])
        }
    }
    var arr = s.split(" ");
    var max_X = -1;
    var max_Y= -1;
    var min_x = 1000;
    var min_y=1000;
    pairwise(arr, function(current,next){
        var p1 = current.split(",");
        var p2 = next.split(",");
        var x = parseFloat(p1[0]);
        var y = parseFloat(p2[0]);
        var dx = Math.abs(parseFloat(p1[0]) - parseFloat(p2[0]));
        var dy = Math.abs(parseFloat(p1[1]) - parseFloat(p2[1]));
        if(dx > max_X) max_X = dx;
        if(dy>max_Y) max_Y = dy;
        if(x< min_x) min_x =x;
        if(y< min_y) min_y = y;
     });
    return {
        width: max_X,
        height: max_Y,
        x:min_x,
        y:min_y
    };
}


/*
 * Helper function to compute the absolute
 * position of the specified element in our graph.
 */
function getAbsolutePosition(d3selection) {
    if (d3selection.empty()) {
        throw "Attempted to get absolute position of an empty selection.";
    }
    var obj = d3selection.select("ellipse");
    var _x = toFloat(obj.attr("cx")) || 0;
    var _y = toFloat(obj.attr("cy")) || 0;
    return {
        x: _x,
        y: _y
    };
}
/* Helper function to connect two RDDs with a curved edge. */
function connectRDDs(fromRDDId, toRDDId, edgesContainer, svgContainer) {
    var fromNodeId = "g#node" + fromRDDId;
    var toNodeId = "g#node" + toRDDId;
    
    //transform clust2 manually because we render the two clusters in the same g element
	/*svgContainer.select("g#clust2").attr("transform","translate(30,0)");
	svgContainer.select("g#node7").attr("transform","translate(30,0)");
	svgContainer.select("g#node8").attr("transform","translate(30,0)");
	svgContainer.select("g#edge6").attr("transform","translate(30,0)");
*/
    var fromPos = getAbsolutePosition(svgContainer.select(fromNodeId));
    var toPos = getAbsolutePosition(svgContainer.select(toNodeId));

    // On the job page, RDDs are rendered as dots (circles). When rendering the path,
    // we need to account for the radii of these circles. Otherwise the arrow heads
    // will bleed into the circle itself.
    var delta1 = toFloat(svgContainer.select(fromNodeId).select("ellipse").attr("rx"));
    var delta2 = toFloat(svgContainer.select(toNodeId).select("ellipse").attr("rx"));
    if (fromPos.x < toPos.x) {
        fromPos.x += delta1;
        toPos.x -= delta2;
    } else if (fromPos.x > toPos.x) {
        fromPos.x -= delta1;
        toPos.x += delta2;
    }

    toPos.x += 20;

    var points;
    if (fromPos.y == toPos.y) {
        // If they are on the same rank, curve the middle part of the edge
        // upward a little to avoid interference with things in between
        // e.g.       _______
        //      _____/       \_____
        points = [
            [fromPos.x, fromPos.y],
            [fromPos.x + (toPos.x - fromPos.x) * 0.2, fromPos.y],
            [fromPos.x + (toPos.x - fromPos.x) * 0.3, fromPos.y - 20],
            [fromPos.x + (toPos.x - fromPos.x) * 0.7, fromPos.y - 20],
            [fromPos.x + (toPos.x - fromPos.x) * 0.8, toPos.y],
            [toPos.x, toPos.y]
        ];
    } else {
        // Otherwise, draw a curved edge that flattens out on both ends
        // e.g.       _____
        //           /
        //          |
        //    _____/
        points = [
            {x: fromPos.x, y: fromPos.y},
            {x: fromPos.x + (toPos.x - fromPos.x) * 0.4, y: fromPos.y},
            {x: fromPos.x + (toPos.x - fromPos.x) * 0.6, y: toPos.y},
            {x: toPos.x, y: toPos.y}
        ];
    }

    //define mark arrow
    var defs = svgContainer.append("defs");

    var arrowMarker = defs.append("marker")
                            .attr("id","arrow")
                            .attr("markerUnits","strokeWidth")
                            .attr("markerWidth","14")
                            .attr("markerHeight","14")
                            .attr("viewBox","0 0 14 14") 
                            .attr("refX","6")
                            .attr("refY","6")
                            .attr("orient","auto");

    var arrow_path = "M2,2 L14,7 L2,12 L2,2";
                            
    arrowMarker.append("path")
                .attr("d",arrow_path)
                .attr("fill","#000");

    var line = d3.line()
                 .x(function(d) {return d.x; })
                 .y(function(d) {return d.y; })
                 .curve(d3.curveBasis);

    //transform the line according to g#graph0 element
    //get the x y values
    var current = svgContainer.select("g#graph0");
    var currentx = current.attr("transform");
    var split = currentx.split(" ");
    var x = split[3].split("(")[1];
    var y = split[4].split(")")[0];

    //render the line
    edgesContainer.append("path")
                .style("fill","none")
                .style("stroke","black")
                .style("stroke-width","1px").attr("d", line(points))
                .attr("transform","translate("+ x +","+ y +")")
                .attr("marker-end", "url(#arrow)");

}


/* Helper function to convert attributes to numeric values. */
function toFloat(f) {
    if (f) {
        return parseFloat(f.toString().replace(/px$/, ""));
    } else {
        return f;
    }
}

//dot data
var dots = [
    [
        'digraph  ""{',
        'subgraph clusterstage_0 {',
        '    label="Stage 0"',
       '    node [shape=circle, style="wedged"]',
        '    a0 [label="hadoopFile", fillcolor="lightcoral;0.8:palegreen"]',
        '    a1 [label="map", fillcolor="yellow;0.8:orange"]',
        '    a2 [label="filterWithProfiling", fontsize=8, fillcolor="yellow;0.3:orange", tooltip = "success:0.3 \nfail:0.7"]',
        '    a3 [label="flatMapWithProfiling", fontsize=8, fillcolor="lightcoral;0.8:palegreen"]',
        '    a4 [label="watchpoint", fillcolor="lightcoral;0.8:palegreen"]',
        '    a5 [label="map", fillcolor="lightcoral;0.8:palegreen"]',
        '    a0->a1;a1->a2->a3->a4->a5',
        '}',
        'subgraph clusterstage_1 {',
        '        label="Stage 1"',
       '    node [shape=circle, style="wedged"]',
        '    b0 [label = "reduceByKey", fontsize=12.5, fillcolor="yellow;0.5:orange"]',
        '    b1 [label = "map", fillcolor="yellow;0.5:orange"]',
        '    b0->b1',
        '}',
        '}'
    ],
];


