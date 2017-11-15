var chart;
var outputChart;

function initChart() {
    var response = document.getElementById('initdata').value;
    /**
     * Parse CSV
     */
    var data = JSON.parse(response)["data"];
    console.log(data);
    /**
     * Create the chart
     */
    chart = AmCharts.makeChart("chartdiv", {
        "type": "serial",
        "theme": "light",
        "colors": ["#FF4C4C"],
        "dataProvider": data,
        "valueAxes": [{
            "gridColor": "#FFFFFF",
            "gridAlpha": 0.2,
            "dashLength": 0,
            "title": "Number of Fault-Inducing Records",
            "position": "left",
            "logarithmic": true,
            "minimum": "1"
        }],
        "gridAboveGraphs": true,
        "startDuration": 0.5,
        "graphs": [{
            "balloonText": "<span style='font-size:14px; color:#000000;'><b>[[value]]</b></span>",
            "fillAlphas": 0.6,
            "lineAlpha": 0.4,
            "title": "Number of Fault-Inducing Records",
            "valueField": "size"
        }],
        "plotAreaBorderAlpha": 0,
        "marginTop": 10,
        "marginLeft": 0,
        "marginBottom": 0,
        "chartScrollbar": {},
        "chartCursor": {
            "cursorAlpha": 0
        },
        "categoryField": "time",
        "categoryAxis": {
            "startOnAxis": true,
            "labelRotation": 90,
            "gridPosition": "start",
            "gridAlpha": 0,
            "tickPosition": "start",
            "tickLength": 10,
            "title": "Time (s)"
        },
        "zoomOutOnDataUpdate": true,
        "listeners": [{
            "event": "init",
            "method": function (e) {
                //add click event on the plot area
                e.chart.chartDiv.addEventListener("click", function () {
                    //track cursor's last known position
                    if (e.chart.lastCursorPosition !== undefined) {
                        //get the category value of the last known cursor position
                        var temp = e.chart.dataProvider[e.chart.lastCursorPosition]["toprecords"];
                        if (temp !== undefined) {
                            document.getElementById("tablebody").innerHTML = temp.map(function (s) {
                                    return "<tr class=\"error\"> <td>" + s + "</td></tr>"
                                }
                            ).reduce(function (v, d) {
                                    return v + d;
                                }
                            )
                        }
                    }
                })
            }
        }, {
            "event": "changed",
            "method": function (e) {
                e.chart.lastCursorPosition = e.index;
            }
        }]
    });
};


function updateChart(data) {
    chart.dataProvider = data;
    chart.validateData();
}

var percentColors = [
    { pct: 0.0, color: { r: 0xff, g: 0x00, b: 0 } },
    { pct: 0.5, color: { r: 0xff, g: 0xff, b: 0 } },
    { pct: 1.0, color: { r: 0x00, g: 0xff, b: 0 } } ];

var getColorForPercentage = function(pct) {
    for (var i = 1; i < percentColors.length - 1; i++) {
        if (pct < percentColors[i].pct) {
            break;
        }
    }
    var lower = percentColors[i - 1];
    var upper = percentColors[i];
    var range = upper.pct - lower.pct;
    var rangePct = (pct - lower.pct) / range;
    var pctLower = 1 - rangePct;
    var pctUpper = rangePct;
    var color = {
        r: Math.floor(lower.color.r * pctLower + upper.color.r * pctUpper),
        g: Math.floor(lower.color.g * pctLower + upper.color.g * pctUpper),
        b: Math.floor(lower.color.b * pctLower + upper.color.b * pctUpper)
    };
    return 'rgb(' + [color.r, color.g, color.b].join(',') + ')';
    // or output as hex if preferred
}


var redefineColors = function(chart2) {
    var dataProvider = chart2.dataProvider;
    var min = Number.MAX_VALUE;
    var max = Number.MIN_VALUE
    for (var i = 0; i < dataProvider.length; i++) {
        if(dataProvider[i].value < min){
            min = dataProvider[i].value;
        }
        if(dataProvider[i].value > max){
            max = dataProvider[i].value;
        }
    }

    var med = (max-min )/ 2;
    for (var i = 0; i < dataProvider.length; i++) {
        var temp = dataProvider[i].value;
        dataProvider[i].color = getColorForPercentage( 1 - (Math.abs(temp - med)/ ( med - min) ) ) ;
    }


}

/**
 * AmCharts plugin: Auto-calculate color based on value
 * The plugin relies on custom chart propety: `colorRanges`
 */
AmCharts.addInitHandler(function(chart) {
    redefineColors(chart);
}, ["serial"]);



function updateOutputChart(data) {
    outputChart.dataProvider = data;
    redefineColors(outputChart);
    outputChart.validateData();
}


function initOutputChart(){


    var response = document.getElementById('initoutputdata').value;
    /**
     * Parse CSV
     */
    var data = JSON.parse(response)["data"];
    console.log(data);


    /* define the chart*/
    outputChart = AmCharts.makeChart("chartdivoutput" , {
        "type": "serial",
        "theme": "light",
        "dataProvider": data,

        "valueAxes": [{
            "gridColor": "#FFFFFF",
            "gridAlpha": 0.2,
            "dashLength": 0,
            "title": "Value"
        }],

        "gridAboveGraphs": true,
        "startDuration": 0.5,

        "graphs": [ {
            "id": "graph1",
            "alphaField": "alpha",
            "balloonText": "<span style='font-size:12px;'>[[title]] of [[category]]:<br><span style='font-size:20px;'>[[value]]</span> [[additional]]</span>",
            "fillColorsField": "color",
            "fillAlphas": 0.9,
            "lineAlpha": 0.2,
            "title": "Value",
            "type": "column",
            "valueField": "value",
            "dashLengthField": "dashLengthColumn"
        }],
        "zoomOutOnDataUpdate": true,
        "chartCursor": {
            "categoryBalloonEnabled": false,
            "cursorAlpha": 0,
            "zoomable": true
        },
        "categoryField": "key",
        "categoryAxis": {
            "gridPosition": "start",
            "labelRotation": 45
        }
    });

    redefineColors(outputChart);
    outputChart.validateData();


}