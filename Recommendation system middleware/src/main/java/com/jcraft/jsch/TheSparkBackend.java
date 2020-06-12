package com.jcraft.jsch;
import java.io.*;

import static java.lang.Thread.sleep;

public class TheSparkBackend {
    //Parameters definition. These parameters are related to SSH connection including file upload and running Spark
    private String pemFile;
    private String username = "hadoop";//"hadoop" is the user name when user is using Spark, no need to change it
    private String host;
    private int port = 22;//22 is the SSH port

    public TheSparkBackend(String pemFileLocation, String pemFileName, String host) {
        this.pemFile = pemFileLocation + pemFileName;
        this.host = host;
    }

    /**
     *
     * @param localAbsolutePathOfTheFile: the absolute local path on one's computer of the file he needs to send to AWS
     * @param AWSAbsolutePathOfTheFile: the absolute path on AWS cluster where he needs to send his file
     * @param nameOfTheFile: the file name
     */
    public void sendOneFileToAWS(String localAbsolutePathOfTheFile, String AWSAbsolutePathOfTheFile, String nameOfTheFile) {
        try {
            Session session = getConnection(); System.out.println("Trying to connect!");
            session.connect(); System.out.println("Successfully connected!");

            System.out.println("Trying to send a file named " + nameOfTheFile + " from local director \"" + localAbsolutePathOfTheFile + "\" to AWS director \"" + AWSAbsolutePathOfTheFile + "\"");
            copyLocalToRemote(session, localAbsolutePathOfTheFile, AWSAbsolutePathOfTheFile + "/" + nameOfTheFile, nameOfTheFile); System.out.println("Successfully sent!");

            session.disconnect();
        } catch (Exception e) {
            errorReport("sendOneFileToAWS()", e);
        }
    }

    /**
     *
     * @param localAbsolutePathOfTheFile: the absolute local path on one's computer of the file he needs to store the file downloaded from AWS
     * @param AWSAbsolutePathOfTheFile: the absolute path on AWS cluster where he needs to download his file from
     * @param nameOfTheFile: the file name
     */
    public void getOneFileFromAWS(String localAbsolutePathOfTheFile, String AWSAbsolutePathOfTheFile, String nameOfTheFile) {
        try {
            Session session = getConnection(); System.out.println("Trying to connect!");
            session.connect(); System.out.println("Successfully connected!");

            System.out.println("Trying to download a file named " + nameOfTheFile + " from AWS directory \"" + AWSAbsolutePathOfTheFile + "\" to local directory \"" + localAbsolutePathOfTheFile + "\"");
            copyRemoteToLocal(session, AWSAbsolutePathOfTheFile, localAbsolutePathOfTheFile, nameOfTheFile); System.out.println("Successfully downloaded!");

            session.disconnect();
        } catch (Exception e) {
            errorReport("getOneFileFromAWS()", e);
        }
    }

    /**
     *
     * @param wordcountJarPath: the absolute path on AWS cluster where the jar file is stored
     * @param wordcountJarName: the wordcount jar file name
     * @param AWSAbsolutePathOfTheFile: the path of input files in HDFS, remember this path is different from the absolute path mentioned above
     *                                this function will automatically copy the files to HDFS
     * @param AWSAbsoluteOutputPath: the path of output files in HDFS
     */
    public void wordcountOnOneFile(String wordcountJarPath, String wordcountJarName,
                                   String AWSAbsolutePathOfTheFile, String AWSAbsoluteOutputPath) {
        try {
            Session session = getConnection(); System.out.println("Trying to connect!");
            session.connect(); System.out.println("Successfully connected!");

            String command = "" +
                    "rm -rf output; " +
                    "hdfs dfs -rm -r /example; " +
                    "hdfs dfs -mkdir /example; " +
                    "hdfs dfs -mkdir /example/input; " +
                    "hdfs dfs -put SparkInput /example/input; " +
                    "spark-submit " +
                    "--class cn.edu360.spark.JavaWordCount " + // The main class in wordcount jar file
                    wordcountJarPath + wordcountJarName + " " + // The path and name of the wordcount jar file
                    "/example/input/" + AWSAbsolutePathOfTheFile + " " + // The input file of the wordcount
                    AWSAbsoluteOutputPath + "; " + // The output path
                    "hdfs dfs -get /example/output /home/hadoop/output; " +
                    "rm -rf output/_SUCCESS; " +
                    "cat output/* > output/result; "; // Combine all output into one single file

            //execute the command
            System.out.println("Running spark-submit");
            System.out.println("Running command: " + command);
            Channel channel = session.openChannel("exec");
            ((ChannelExec) channel).setCommand(command);
            ((ChannelExec) channel).setErrStream(System.err);
            channel.connect();

            //getting back the console output
            System.out.println("Successfully running spark-submit, return result to console.");
            System.out.println("Output are stored at" + AWSAbsoluteOutputPath + ", use " +
                    "\"hdfs dfs -ls " + AWSAbsoluteOutputPath + "\" or " +
                    "\"hdfs dfs -cat " + AWSAbsoluteOutputPath + "filename\" to check.");
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
            System.out.println("Successfully returning result, disconnecting.");

            channel.disconnect();
            session.disconnect();
        } catch (Exception e) {
            errorReport("wordcountOnOneFile()", e);
        }
    }

    private Session getConnection() {
        try {
            JSch jSch = new JSch();
            jSch.addIdentity(pemFile);
            jSch.setConfig("StrictHostKeyChecking", "no");
            return jSch.getSession(username, host, port);
        } catch (Exception e) {
            errorReport("getConnection()", e);
        }
        return null;
    }

    private void errorReport(String methodName, Exception e) {
        System.err.println("Exception is catched");
        System.err.println("Something wrong in " + methodName + " method of TheSparkBackend.java");
        System.err.println(e.getMessage());
        System.err.println(e.getCause());
    }

    //This function is used for uploading files to AWS using SCP.
    private static void copyLocalToRemote(Session session, String from, String to, String fileName) throws JSchException, IOException {
        boolean ptimestamp = true;
        from = from + File.separator + fileName;

        // exec 'scp -t rfile' remotely
        String command = "scp " + (ptimestamp ? "-p" : "") + " -t " + to;
        Channel channel = session.openChannel("exec");
        ((ChannelExec) channel).setCommand(command);

        // get I/O streams for remote scp
        OutputStream out = channel.getOutputStream();
        InputStream in = channel.getInputStream();

        channel.connect();

        if (checkAck(in) != 0) {
            System.exit(0);
        }

        File _lfile = new File(from);

        if (ptimestamp) {
            command = "T" + (_lfile.lastModified() / 1000) + " 0";
            // The access time should be sent here,
            // but it is not accessible with JavaAPI ;-<
            command += (" " + (_lfile.lastModified() / 1000) + " 0\n");
            out.write(command.getBytes());
            out.flush();
            if (checkAck(in) != 0) {
                System.exit(0);
            }
        }

        // send "C0644 filesize filename", where filename should not include '/'
        long filesize = _lfile.length();
        command = "C0644 " + filesize + " ";
        if (from.lastIndexOf('/') > 0) {
            command += from.substring(from.lastIndexOf('/') + 1);
        } else {
            command += from;
        }

        command += "\n";
        out.write(command.getBytes());
        out.flush();

        if (checkAck(in) != 0) {
            System.exit(0);
        }

        // send a content of lfile
        FileInputStream fis = new FileInputStream(from);
        byte[] buf = new byte[1024];
        while (true) {
            int len = fis.read(buf, 0, buf.length);
            if (len <= 0) break;
            out.write(buf, 0, len); //out.flush();
        }

        // send '\0'
        buf[0] = 0;
        out.write(buf, 0, 1);
        out.flush();

        if (checkAck(in) != 0) {
            System.exit(0);
        }
        out.close();

        try {
            if (fis != null) fis.close();
        } catch (Exception ex) {
            System.out.println(ex);
        }

        channel.disconnect();
        session.disconnect();
    }

    //This function is used for downloading files from AWS using SCP
    private static void copyRemoteToLocal(Session session, String from, String to, String fileName) throws JSchException, IOException {
        from = from + File.separator + fileName;
        String prefix = null;

        if (new File(to).isDirectory()) {
            prefix = to + File.separator;
        }

        // exec 'scp -f rfile' remotely
        String command = "scp -f " + from;
        Channel channel = session.openChannel("exec");
        ((ChannelExec) channel).setCommand(command);

        // get I/O streams for remote scp
        OutputStream out = channel.getOutputStream();
        InputStream in = channel.getInputStream();

        channel.connect();

        byte[] buf = new byte[1024];

        // send '\0'
        buf[0] = 0;
        out.write(buf, 0, 1);
        out.flush();

        while (true) {
            int c = checkAck(in);
            if (c != 'C') {
                break;
            }

            // read '0644 '
            in.read(buf, 0, 5);

            long filesize = 0L;
            while (true) {
                if (in.read(buf, 0, 1) < 0) {
                    // error
                    break;
                }
                if (buf[0] == ' ') break;
                filesize = filesize * 10L + (long) (buf[0] - '0');
            }

            String file = null;
            for (int i = 0; ; i++) {
                in.read(buf, i, 1);
                if (buf[i] == (byte) 0x0a) {
                    file = new String(buf, 0, i);
                    break;
                }
            }

            System.out.println("file-size=" + filesize + ", file=" + file);

            // send '\0'
            buf[0] = 0;
            out.write(buf, 0, 1);
            out.flush();

            // read a content of lfile
            FileOutputStream fos = new FileOutputStream(prefix == null ? to : prefix + file);
            int foo;
            while (true) {
                if (buf.length < filesize) foo = buf.length;
                else foo = (int) filesize;
                foo = in.read(buf, 0, foo);
                if (foo < 0) {
                    // error
                    break;
                }
                fos.write(buf, 0, foo);
                filesize -= foo;
                if (filesize == 0L) break;
            }

            if (checkAck(in) != 0) {
                System.exit(0);
            }

            // send '\0'
            buf[0] = 0;
            out.write(buf, 0, 1);
            out.flush();

            try {
                if (fos != null) fos.close();
            } catch (Exception ex) {
                System.out.println(ex);
            }
        }

        channel.disconnect();
        session.disconnect();
    }

    //This function is used by copyLocalToRemote().
    public static int checkAck(InputStream in) throws IOException {
        int b = in.read();
        // b may be 0 for success,
        //          1 for error,
        //          2 for fatal error,
        //         -1
        if (b == 0) return b;
        if (b == -1) return b;

        if (b == 1 || b == 2) {
            StringBuffer sb = new StringBuffer();
            int c;
            do {
                c = in.read();
                sb.append((char) c);
            }
            while (c != '\n');
            if (b == 1) { // error
                System.out.print(sb.toString());
            }
            if (b == 2) { // fatal error
                System.out.print(sb.toString());
            }
        }
        return b;
    }

    public static void main(String args[]) {
        //1.Fill in the corresponding information of the AWS cluster: pemfile location, pemfile name, host ip
        TheSparkBackend theSparkBackend = new TheSparkBackend("/Users/zhangzhenyuan/Downloads/", "spaaark.pem", "ec2-52-53-127-111.us-west-1.compute.amazonaws.com");

        //2.From local where to send one what file to AWS where: local absolute path, AWS absolute path, File name
        //One might need to use for loop to send multiple files.
        //AWS absolute path is needed to be created before running this code. For example "mkdir SparkInput" on AWS.
        theSparkBackend.sendOneFileToAWS("filesToUpload/", "SparkInput","InputData1.txt");
        theSparkBackend.sendOneFileToAWS("filesToUpload/", "SparkInput","InputData2.txt");
        //Send the wordcount jar to AWS
        theSparkBackend.sendOneFileToAWS("filesToUpload/", "wordcountJar", "original-SparkTest-1.0-SNAPSHOT.jar");

        //3.Execute the jar
        theSparkBackend.wordcountOnOneFile("wordcountJar/", "original-SparkTest-1.0-SNAPSHOT.jar",
                "SparkInput/","/example/output");

	//4.Download the output file to the project root path's directory "filesFromSparkOutput"
        theSparkBackend.getOneFileFromAWS("filesFromSparkOutput/", "output/", "result");
    }
}
