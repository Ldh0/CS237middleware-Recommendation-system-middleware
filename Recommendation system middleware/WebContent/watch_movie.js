function GetRequest() {
    let url = location.search;
    let theRequest = new Object();
    let strs;
    if (url.indexOf("?") !== -1) {
        let str = url.substr(1);
        strs = str.split("&");
        for (var i = 0; i < strs.length; i++) {
            theRequest[strs[i].split("=")[0]] = decodeURI(strs[i].split("=")[1]);
        }
    }
    return theRequest;
}

let movieTitle = GetRequest()["title"];

$(document).ready(function(){
    console.log("start to initial");
    $.ajax({
        url: "/project/api/watchMovieServlet?title=" + movieTitle,
        dataType: "json",
        method: "GET",
        success: (result) => watch_success()
    })
});

function watch_success() {
    console.log("watch success");
    let html = "<h4>Watch" + movieTitle + "</h4>";
    $("#watchMovie").append(html);
}
