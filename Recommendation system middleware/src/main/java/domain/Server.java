package domain;

import com.jcraft.jsch.TheSparkBackend;

import java.io.*;
import java.util.*;

public class Server{

    private Map<String, Number> cloudBaseFreq;
    private Map<String, Number> cloudExtraFreq;
    private Map<String, Number> localBaseFreq;
    private Map<String, Number> localExtraFreq;
    private Set<String>  inputNameSet;
    private List<String> cloudRank;
    private List<String> localRank;
    private int cloudPushTime = 100;
    private int sleepTime = 90000;
    private int localPushThreshold = 10;
    private Calendar calendar;
    private String cloudPemFileLocation = "/Users/zhangzhenyuan/Downloads/";
    private String localPemFileLocation = "/Users/zhangzhenyuan/Downloads/";
    private String cloudPemFileName = "spaaark.pem";
    private String localPemFileName = "spaaark.pem";
    private String cloudAWSAddress = "ec2-54-183-153-57.us-west-1.compute.amazonaws.com";
    private String localAWSAddress = "ec2-13-56-77-146.us-west-1.compute.amazonaws.com";
    private String absolutePathOfInput = "/Users/zhangzhenyuan/Desktop/temp/ShareCS237-2/jsch-0.1.55/filesToUpload/";
    private String absolutePathOfAwsInput = "SparkInput/";
    private String absolutePathOfOutput = "/Users/zhangzhenyuan/Desktop/temp/ShareCS237-2/jsch-0.1.55/filesFromSparkOutput/";
    private String absolutePathOfOutputLocal = "/Users/zhangzhenyuan/Desktop/temp/ShareCS237-2/jsch-0.1.55/filesFromSparkOutputLocal/";
    private String absolutePathOfAwsOutput = "output";
    private String cloudNameOfInput = "";
    private String cloudNameOfOutput = "result";
    private String localNameOfOutput = "result";

    //initial helper function
    public void Initial(){
        this.cloudBaseFreq = new HashMap<String, Number>();
        this.cloudExtraFreq = new HashMap<String, Number>();
        this.localBaseFreq = new HashMap<String, Number>();
        this.localExtraFreq = new HashMap<String, Number>();
        this.inputNameSet = new HashSet<String>();
        this.cloudRank = new ArrayList<>();
        this.localRank = new ArrayList<>();
        this.pull_from_local();
    }

    //class initial function
    public Server(){
        //init all the parameters
        Initial();
        //do the init of cloud spark periodically
        System.out.println("Server initial...");
        new CloudPushThread().start();
        System.out.println("Server initialization success");
    }

    private class CloudPushThread extends Thread{

        @Override
        public void run() {
            // TODO Auto-generated method stub
            int count = cloudPushTime;
            while(count > 0) {
                count -= 1;
                push_to_cloud();
                pull_from_cloud();
                try {
                    sleep(sleepTime);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void push_to_cloud(){

        String fileName = get_freq_file(this.cloudExtraFreq);
        System.out.println("Generate input success");
        TheSparkBackend theSparkBackend = new TheSparkBackend(this.cloudPemFileLocation, this.cloudPemFileName, this.cloudAWSAddress);
        theSparkBackend.sendOneFileToAWS(absolutePathOfInput, "wordcountJar", "original-SparkTest-1.0-SNAPSHOT.jar");
        System.out.println("Connection to spark success");
        theSparkBackend.sendOneFileToAWS(this.absolutePathOfInput, this.absolutePathOfAwsInput, fileName);
        System.out.println("Send file to AWS success");
        theSparkBackend.wordcountOnOneFile("wordcountJar/","original-SparkTest-1.0-SNAPSHOT.jar", this.absolutePathOfAwsInput, "/example/output");
        this.cloudExtraFreq.clear();
        System.out.println("push to cloud success");
    }

    public void push_to_local(){

        String fileName = get_freq_file(this.localExtraFreq);
        System.out.println("Generate input success");
        TheSparkBackend theSparkBackend = new TheSparkBackend(this.localPemFileLocation, this.localPemFileName, this.localAWSAddress);
        theSparkBackend.sendOneFileToAWS(absolutePathOfInput, "wordcountJar", "original-SparkTest-1.0-SNAPSHOT.jar");
        System.out.println("Connection to spark success");
        theSparkBackend.sendOneFileToAWS(this.absolutePathOfInput, this.absolutePathOfAwsInput, fileName);
        System.out.println("Send file to AWS success");
        theSparkBackend.wordcountOnOneFile("wordcountJar/","original-SparkTest-1.0-SNAPSHOT.jar", this.absolutePathOfAwsInput, "/example/output");
        this.localExtraFreq.clear();
        System.out.println("push to local success");
    }

    public void pull_from_cloud(){
        TheSparkBackend theSparkBackend = new TheSparkBackend(this.cloudPemFileLocation, this.cloudPemFileName, this.cloudAWSAddress);
        System.out.println("Connection to spark success");
        theSparkBackend.getOneFileFromAWS(this.absolutePathOfOutput, this.absolutePathOfAwsOutput, this.cloudNameOfOutput);
        System.out.println("Fetch file from AWS success");
        this.read_cloud_rank_from_file(this.absolutePathOfOutput + this.cloudNameOfOutput);
        System.out.println("pull from cloud success");
    }

    public void pull_from_local(){
        TheSparkBackend theSparkBackend = new TheSparkBackend(this.localPemFileLocation, this.localPemFileName, this.localAWSAddress);
        System.out.println("Connection to spark success");
        theSparkBackend.getOneFileFromAWS(this.absolutePathOfOutputLocal, this.absolutePathOfAwsOutput, this.localNameOfOutput);
        System.out.println("Fetch file from AWS success");
        this.read_local_rank_from_file(this.absolutePathOfOutputLocal + this.localNameOfOutput);
        System.out.println("pull from local success");
    }

    public void read_cloud_rank_from_file(String fileName){
        File file = new File(fileName);
        try{
            if(!file.exists()){
                System.out.println("No rank file in system");
            }
        }catch(Exception e){
            e.printStackTrace();
        }

        Map<String, Integer> countMap = new HashMap<String, Integer>();
        BufferedReader reader = null;
        String[] tmp;
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            int line = 1;
            while ((tempString = reader.readLine()) != null) {
                if(tempString.length()>=2){
                    tempString = tempString.substring(1,tempString.length()-1);
                    tmp = tempString.split(",");

                    countMap.put(tmp[0], Integer.valueOf(tmp[1]));
                }
                line++;
            }
            reader.close();
            List<Map.Entry<String, Integer>> list = new ArrayList<Map.Entry<String, Integer>>(countMap.entrySet());
            list.sort(new Comparator<Map.Entry<String, Integer>>() {
                @Override
                public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                    return o2.getValue().compareTo(o1.getValue());
                }
            });
            this.cloudRank = new ArrayList<>();
            for (int i=0;i<list.size();i++) {
                System.out.println(list.get(i).getKey());
                this.cloudRank.add(list.get(i).getKey());
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
    }

    public void read_local_rank_from_file(String fileName){
        File file = new File(fileName);
        try{
            if(!file.exists()){
                System.out.println("No rank file in system");
            }
        }catch(Exception e){
            e.printStackTrace();
        }

        Map<String, Integer> countMap = new HashMap<String, Integer>();
        BufferedReader reader = null;
        String[] tmp;
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            int line = 1;
            while ((tempString = reader.readLine()) != null) {
                if(tempString.length()>=2){
                    tempString = tempString.substring(1,tempString.length()-1);
                    tmp = tempString.split(",");

                    countMap.put(tmp[0], Integer.valueOf(tmp[1]));
                }
                line++;
            }
            reader.close();
            List<Map.Entry<String, Integer>> list = new ArrayList<Map.Entry<String, Integer>>(countMap.entrySet());
            list.sort(new Comparator<Map.Entry<String, Integer>>() {
                @Override
                public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                    return o2.getValue().compareTo(o1.getValue());
                }
            });
            this.localRank = new ArrayList<>();
            for (int i=0;i<list.size();i++) {
                System.out.println(list.get(i).getKey());
                this.localRank.add(list.get(i).getKey());
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
    }

    public void rank_freq(Map<String, Number> map, String mode){

    }

    public void add_freq_to_map(Map<String, Number> map, String title){
        if(map.containsKey(title)){
            map.put(title, map.get(title).intValue() + 1);
        }else{
            map.put(title, 1);
        }
    }

    public void judge_threshold(String title){
        if(this.localExtraFreq.get(title).intValue() > this.localPushThreshold){
            push_to_local();
            pull_from_local();
        }
    }

    //local only record abc
    //cloud record everything
    public void add_freq(String title){
        System.out.println("add " + title + " 1 time");
        if(Arrays.asList('a', 'b', 'c').contains(title.charAt(0))){
            add_freq_to_map(this.localExtraFreq, title);
            judge_threshold(title);
        }
        add_freq_to_map(this.cloudExtraFreq, title);

        System.out.println("add to frequency success");
    }

    public List<String> get_cloud_rank(){
        System.out.println("get cloud rank");
        return this.cloudRank;
    }

    public List<String> get_local_rank(){
        System.out.println("get local rank");
        return this.localRank;
    }

    public String get_freq_file(Map<String, Number> map){
        this.calendar = Calendar.getInstance();
        String cur = String.valueOf(this.calendar.getTimeInMillis());
        while(this.inputNameSet.contains(cur)){
            cur = String.valueOf(this.calendar.getTimeInMillis());
        }
        String fileName = cur + ".txt";
        File file = create_file_with_name(fileName);
        write_freq_file(gen_freq_string(map), absolutePathOfInput + fileName);
        return fileName;
    }

    public File create_file_with_name(String name){
        File file = new File(name);
        try{
            if(!file.exists()){
                file.createNewFile();
                System.out.println("Create file success");
            }
        }catch(Exception e){
            e.printStackTrace();
        }
        return file;
    }

    public void write_freq_file(String content, String filePath){
        FileWriter fw = null;
        try {
            fw = new FileWriter(filePath, true);
            fw.write(content);
            fw.flush();
            fw.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String gen_freq_string(Map<String, Number> map){
        String string  = "";
        String word;

        int times;

        for(Map.Entry<String, Number> entry : map.entrySet()){
            word = entry.getKey();
            times = entry.getValue().intValue();
            for(int i=0; i<times; i++){
                string += word + "\n";
            }
        }
        return string;
    }

//    public static void main(String[] args) {
//        String fileName = "test.txt";
//        Server server = new Server();
//        server.create_file_with_name(fileName);
//        server.write_freq_file("123", fileName);
//    }
}
