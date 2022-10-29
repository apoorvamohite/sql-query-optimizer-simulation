package com.qosim;


import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */

/**
 *
 * @author apoor
 */
public class DbUtil {
    
    protected static List<String> readFileLines(String filePath){
        List<String> fileLines = new ArrayList<String>();
        try{
            File file=new File(filePath);   
            Scanner sc = new Scanner(file);     //file to be scanned  
            while (sc.hasNextLine())        //returns true if and only if scanner has another token  
                fileLines.add(sc.nextLine());
            return fileLines;
        } catch(Exception e){
            System.out.println("There was an error reading the file");
            return null;
        }
    }
    
    protected static void formatTable(List<List<String>> arr){
        for(List<String> row: arr){
            System.out.println(centerString(20,row.get(0))+"|"+ centerString(15,row.get(1))+"|"+  centerString(15,row.get(2)));
        }
    }
    
    //https://stackoverflow.com/questions/8154366/how-to-center-a-string-using-string-format
    public static String centerString (int width, String s) {
        return String.format("%-" + width  + "s", String.format("%" + (s.length() + (width - s.length()) / 2) + "s", s));
    }
    
    public static String leftJustifyString (int width, String s) {
        return String.format("%-"+width+"s", s);
    }
    
    public static String rightJustifyString (int width, String s) {
        return String.format("%"+width+"s", s);
    }
}
