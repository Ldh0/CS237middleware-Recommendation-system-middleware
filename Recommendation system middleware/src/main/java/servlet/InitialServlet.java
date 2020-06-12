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
import java.util.Stack;

@WebServlet(urlPatterns = "/api/initServlet")
public class InitialServlet extends HttpServlet {

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        System.out.println("Init_Servlet");


        response.setContentType("application/json"); // Response mime type
        PrintWriter printWriter = response.getWriter();
        JsonObject responseJson = new JsonObject();
        Server server = (Server) request.getSession().getAttribute("server");
        if(server==null) {
            server = new Server();
        }
        try {
            request.getSession().setAttribute("server", server);
            responseJson.addProperty("status", "success");
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
}