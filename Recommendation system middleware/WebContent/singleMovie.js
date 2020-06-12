document.write("<script language=javascript src='goback.js'></script>");

$(document).ready(function () {
    addUrlToSession();
    $.ajax({
        url: "/project1/api/getSingleMovie?id=" + movieId,
        dataType: "json",
        method: "GET",
        success: (result) => pushSinglemovie(result)
    })
});

function addToCart(movieId){
    console.log("add to cart");
    $.ajax({
        url: "/project1/api/addToCart?operation=add&movieId=" + movieId,
        dataType: "json",
        method: "GET",
        success: alert("Add one item to cart")
    });
}

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

let movieId = GetRequest()["id"];

function pushSinglemovie(movie) {
    console.log("Try to pull data from database");
    let htmlBody = $("#html_body");

    let html = "<button type='button' onclick='window.location.href=\"shoppingCart.html\"'>Checkout</button>";
    html += "<h1>" + movie["title"] + "</h1>" + "<br>";
    html += "<h3>year: " + movie["year"] + "</h3>" + "<br>";
    html += "<h3>director: " + movie["director"] + "</h3>" + "<br>";
    html += "<h3>genres: " + movie["genres"] + "</h3>" + "<br>";
    html += "<h3>stars: </h3><div style=\"display: inline;\">";
    let starList = movie["stars"].split(",");
    for (let j = 0; j < starList.length; j += 2) {
        html += "<a href='singleStar.html?id=" + starList[j] + "'>" +
            starList[j + 1] + "</a>";
        if(j<starList.length - 2){
            html += " , ";
        }
    }
    html += "</div><br>";
    // for(let j=0; j<movies[i]["stars"].length; j++){
    //     html += "<a href='single-star.html?id=" + movies[i]["stars"][j] + "'>" +
    //         movies[i]["stars"][j] + "</a> ";
    // }
    html += "<h3>rating: " + movie["rating"] + "</h3>";
    html += "<div><button type='button' onclick='javascript:addToCart(\"" + movieId + "\");'>add to cart</button></div>";
    html += "<input type='button' name='goBack' value='Go back' onclick='javascript:goBackToLast();'/>";
    htmlBody.append(html);
}


