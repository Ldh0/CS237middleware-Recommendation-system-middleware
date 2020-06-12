package servlet;


import com.google.gson.JsonObject;
import domain.Server;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.List;
import java.util.Stack;

@WebServlet(urlPatterns = "/api/rankServlet")
public class RankServlet extends HttpServlet {

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        System.out.println("push rank");

        //String type = request.getParameter("type");
        String type = "";

        response.setContentType("application/json"); // Response mime type
        PrintWriter printWriter = response.getWriter();
        JsonObject responseJson = new JsonObject();

        try {
            if(Arrays.asList('a', 'b', 'c').contains(request.getSession().getAttribute("FisrtChar"))){
                type = "local";
            }else{
                type = "cloud";
            }
            Server server = (Server) request.getSession().getAttribute("server");
            responseJson.addProperty("local", join(server.get_local_rank()));
            responseJson.addProperty("cloud", join(server.get_cloud_rank()));
            responseJson.addProperty("status", "success");
            responseJson.addProperty("type", type);
            printWriter.write(responseJson.toString());
            response.setStatus(200);
        } catch (Exception e) {
            responseJson.addProperty("status", "fail");
            responseJson.addProperty("errorMessage", e.getMessage());
            System.out.println(e.getMessage());
            printWriter.write(responseJson.toString());
            response.setStatus(200);
        }
        printWriter.close();
    }

    private String join(List<String> list){
        if(list==null){
            return "";
        }
        String string = "";
        for(String ele:list){
            string += ele + ",";
        }
        return list.isEmpty() ? "":string.substring(0, string.length()-1);
    }
}