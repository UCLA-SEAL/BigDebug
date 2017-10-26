/**
 *
 val UIDATA = 1
 val JOBTIME = 2
 val SIZE = 3;
 val DEBUGTIME = 4;
 * ***/

function initbsWebSocket(){
    var port = document.getElementById('websocketport').value
    var host = window.location.hostname

    var ws = new WebSocket("ws://" + host + ":" + port)

            ws.onopen = function(){ ws.send("Initializing Bigsift Socket at "+ port  );
            };
            ws.onmessage = function(e){
                var server_message = e.data;
               // console.log(server_message);
                if(!server_message.startsWith("E")){
                   var json = JSON.parse(server_message);
                    var key = json["key"];
                    switch(key){
                        case 1:
                            updateChart(json["data"]);
                            break;
                        case 2:
                            document.getElementById('initJobTime').innerHTML = "<div class=\"alert alert-success\"> <strong>Original Job Time : </strong>" +json["data"]+ " seconds" +  "</div>";
                            break;
                        case 3:
                            document.getElementById('initSize').innerHTML =  "<div class=\"alert alert-success\"> <strong>Initial Size of Fault-Inducing Inputs : </strong>" +json["data"]+ " records" +  "</div>";
                            break;
                        case 4:
                            document.getElementById('finalDebugTime').innerHTML ="<div class=\"alert alert-success\"> <strong>Total Debugging Time : </strong>" +json["data"]+ " seconds" +  "</div>";
                            break;
                        default:
                            console.log("Key not found : " + key)


                    }
                };
            }
}

var editor = undefined;
var list = [];
function unhighlight(e) {
    var f = function (a) {
        e.removeLineClass(a, 'background', 'line-bp')
    };
    list.map(f)
    list = [];
}
function highlightLine(lineNumber) {
    var myeditor = editor;
    unhighlight(myeditor);
    list.push(lineNumber);
    myeditor.addLineClass(lineNumber, 'background', 'line-bp');
};
function highlightLine_error(lineNumber) {
    var myeditor = editor;
    myeditor.addLineClass(lineNumber - 1, 'background', 'line-error');
};
function createCode() {
    editor = CodeMirror.fromTextArea(document.getElementById("code"), {
        lineNumbers: true,
        matchBrackets: true,
        // theme: "ambiance",
        mode: "text/x-scala",
        styleActiveLine: true,
        viewportMargin: Infinity,
        gutters: ["CodeMirror-linenumbers", "breakpoints"]
    });
    editor.on("gutterClick", function (cm, n) {
        var info = cm.lineInfo(n);
        cm.setGutterMarker(n, "breakpoints", info.gutterMarkers ? null : makeMarker());
    });

    if (document.getElementById("breaklineinfo") !== null) {
        highlightLine(parseInt(document.getElementById("breaklineinfo").value))
        var crashes = document.getElementById("crashlineinfo").value.split(" ")
        for (var i = 0; i < crashes.length; i++) {
            var val = parseInt(crashes[i])
            if (!isNaN(val)) highlightLine_error(val)
        }
    }
    if(document.getElementById("disablelines") !== null){
        var dis_lines =  document.getElementById("disablelines").value.split(",").map(function(a){
            return parseInt(a)
        });
        dis_lines.map(function(a){
            editor.addLineClass(a, 'background', 'line-dis');
        });
        editor.on('beforeChange',function(cm,change) {
            if ( ~dis_lines.indexOf(change.from.line) ) {
                change.cancel();
            }
        });
    }
};