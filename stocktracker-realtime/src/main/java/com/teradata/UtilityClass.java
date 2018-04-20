package com.teradata;

public class UtilityClass {

    public static String concatString(String s1, String s2){

        String s3 = s1.replace('"',' ').trim()+"_"+s2.replace('"',' ').trim();


        //System.out.println(s3);
        return s3;
    }
}
