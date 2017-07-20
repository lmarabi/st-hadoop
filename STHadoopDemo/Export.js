/* 
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
function validateEmail(email) {
    var re = /^(([^<>()\[\]\\.,;:\s@"]+(\.[^<>()\[\]\\.,;:\s@"]+)*)|(".+"))@((\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}])|(([a-zA-Z\-0-9]+\.)+[a-zA-Z]{2,}))$/;
    return re.test(email);
}


var nodes, edges;
function respond_export() {
    if (request.readyState !== 4)
        return;
    var out = request.responseText;
    out = out.split("/");
    nodes = out[0];
    edges = out[1];
    if (nodes === '' && edges === ''
	&& nodes === " " && edges === " "
	&& nodes === null && edges === null) {
        var e = document.getElementById("result");
        if (e !== null)
            e.parentNode.removeChild(e);
        a = document.createElement("p");
        a.setAttribute("id", "result");
        a.appendChild(document.createTextNode("No " + out[2] + " found!"));
        document.getElementById("view2").appendChild(a);
    }
    else {
        var v = document.getElementById("view");
        var d = document.getElementById("down");
        var e;
        while (e = document.getElementById("result"))
            e.parentNode.removeChild(e)
        if (v === null && d === null) {
            a = document.createElement('br');
            document.export.appendChild(a)
            a = document.createElement('input');
            a.setAttribute('type', "button");
            a.setAttribute('id', "view");
            a.setAttribute('value', "Visualize");
            a.setAttribute('onclick', "submit_view()");
            document.export.appendChild(a)
  //          a = document.createElement('input');
  //          a.setAttribute('type', "submit");
  //          a.setAttribute('name', "down");
  //          a.setAttribute('value', "Download");
  //          a.setAttribute('onclick', "submit_down()");
  //          document.down.appendChild(a)
        }
        a = document.createElement("p");
        a.setAttribute("id", "result");
        a.appendChild(document.createTextNode("Done!"));
        document.getElementById("view2").appendChild(a);
    }
}
function submit_view() {
    request = new XMLHttpRequest();
    request.onreadystatechange = respond_view;
    request.open("POST", "control.php", true /* asynchronous? */)
    request.setRequestHeader("Content-type", "application/x-www-form-urlencoded")
    request.send("action=view&node="+nodes+"&edge="+edges);
    var e;
    while (e = document.getElementById("result"))
        e.parentNode.removeChild(e)
    var a = document.createElement("p");
    a.setAttribute("id", "result");
    a.appendChild(document.createTextNode("Drawing..."));
    document.getElementById("view2").appendChild(a);
}
function respond_view() {
    if (request.readyState !== 4)
        return;
    var out = request.responseText
    out = out.split("/")
    nodes = out[0]
    edges = out[1]
    Import(nodes, edges)
    var e;
    while (e = document.getElementById("result"))
        e.parentNode.removeChild(e)
    a = document.createElement("p");
    a.setAttribute("id", "result");
    a.appendChild(document.createTextNode("Done!"));
    document.getElementById("view2").appendChild(a);
}
