var chart;

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