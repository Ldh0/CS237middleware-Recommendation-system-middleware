package domain;

import java.util.Map;
import java.util.TimerTask;

public class CloudTimeTasker extends TimerTask {

    Map<String, Number> cloudExtraFreq;

    CloudTimeTasker(Map<String, Number> cloudExtraFreq){
        this.cloudExtraFreq = cloudExtraFreq;
    }

    @Override
    public void run() {

    }


}
