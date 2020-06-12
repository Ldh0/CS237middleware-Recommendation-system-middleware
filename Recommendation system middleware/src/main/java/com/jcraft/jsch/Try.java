package com.jcraft.jsch;

import java.io.InputStream;

import static java.lang.Thread.sleep;

public class Try {
    public static void main(String args[]) {
        JSch jSch = new JSch();
        try {
            jSch.addIdentity("C:\\awspem.pem");
            jSch.setConfig("StrictHostKeyChecking", "no");

            System.out.println("try to connect");
            //enter your own EC2 instance IP here
            Session session=jSch.getSession("hadoop", "ec2-5-170-66-163.compute-1.amazonaws.com", 22);
            System.out.println("connecting");
            session.connect();

            System.out.println("run stuff");
            //run stuff
            String command =
                    "spark-submit --class cn.edu360.spark.JavaWordCount s3n://sparktry/original-SparkTest-1.0-SNAPSHOT.jar s3n://sparktry/1.txt ./example/output2;" +
                    "hdfs dfs -cat example/output2/part-00000;" +
                    "hdfs dfs -cat example/output2/part-00001;";
            Channel channel = session.openChannel("exec");
            ((ChannelExec) channel).setCommand(command);
            ((ChannelExec) channel).setErrStream(System.err);
            channel.connect();

            InputStream input = channel.getInputStream();
            //start reading the input from the executed commands on the shell
            byte[] tmp = new byte[1024];
            while (true) {
                while (input.available() > 0) {
                    int i = input.read(tmp, 0, 1024);
                    if (i < 0) break;
                    System.out.println(new String(tmp, 0, i));
                }
                if (channel.isClosed()){
                    System.out.println("exit-status: " + channel.getExitStatus());
                    break;
                }
                sleep(1000);
            }

            channel.disconnect();
            session.disconnect();
        } catch (Exception e) {
            System.out.println("Something wrong in Try.java");
            System.err.println(e.getMessage());
            System.err.println(e.getCause());
        }
    }
}
