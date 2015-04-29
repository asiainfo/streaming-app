package com.asiainfo.ocdc.streaming.tool;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by leo on 4/28/15.
 */
public class Test1 {
    public static void main(String args[]){
        List<String> message = new ArrayList<String>();
        message.add("zhang3");
        List<String> bm = message;
        message = new ArrayList<String>();
        message.add("li4");
        System.out.println(bm.get(0));
    }

}
