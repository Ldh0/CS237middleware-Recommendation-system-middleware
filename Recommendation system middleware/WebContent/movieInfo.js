
$(document).ready(function(){


    addUrlToSession();
    $.ajax({
        url: "/project1/api/getMovies",
        dataType: "json",
        method: "GET",
        success: (result) => pushMovieList(result)
    })
});

function pushMovieList(movies){
    console.log("Try to pull data from database");
    let movieTbody = $("#movie_tbody");

    for(let i=0; i<Math.min(20, movies.length); i++){
        var html = "";
        html += "<tr>" +
            "<td><a href='singleMovie.html?id=" + movies[i]["id"] + "'>" +
            movies[i]["title"] + "</a></td>" +
            "<td>" + movies[i]["year"] + "</td>" +
            "<td>" + movies[i]["director"] + "</td>" +
            "<td>";
        var genresList = movies[i]["genres"].split(",");
        for(let j=0; j<Math.min(genresList.length, 3);j++){
            html += genresList[j];
        }
        html += "</td><td>";
        var starList = movies[i]["stars"].split(",");
        for(let j=0; j<Math.min(starList.length, 6);j+=2){
            html += "<a href='singleStar.html?id=" + starList[j] + "'>" +
                starList[j+1] + "</a> ";
        }
        // for(let j=0; j<movies[i]["stars"].length; j++){
        //     html += "<a href='single-star.html?id=" + movies[i]["stars"][j] + "'>" +
        //         movies[i]["stars"][j] + "</a> ";
        // }
        html += "</td>" +
            "<td>" + movies[i]["rating"] + "</td>" +
            "</tr>";
        movieTbody.append(html);
    }

}

