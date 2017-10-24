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
