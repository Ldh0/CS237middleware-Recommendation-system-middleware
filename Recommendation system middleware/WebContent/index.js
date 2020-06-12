$(document).ready(function(){
    console.log("start to initial");
    $.ajax({
        url: "/project/api/initServlet",
        dataType: "json",
        method: "GET",
        success: (result) => initial_success()
    })
});

function  initial_success() {
    console.log("initial success");
}

function watch_movie(){
    window.location.href = "watch_movie.html?title=" + $('#title').val();
}

function show_rank(){
    window.location.href = "movie_rank.html";
}

