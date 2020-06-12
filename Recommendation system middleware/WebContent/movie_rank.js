$(document).ready(function(){
    console.log("start to initial");
    $.ajax({
        url: "/project/api/rankServlet",
        dataType: "json",
        method: "GET",
        success: (result) => fetch_success(result)
    })
});

function fetch_success(result) {
    console.log("rank success");
    let type = result["type"];
    let html = "<ol>";
    console.log(result);
    let rankList = result[type].split(",");
    if(rankList != null){
        for(let i=0;i<rankList.length;i++){
            html += "<li>" + rankList[i] + "</li>";
        }
    }
    html += "</ol>";
    $("#movieRank").append(html);
}