package org.example;

import java.io.BufferedReader;
import java.io.*;

public class FileConfiguration {

    public static String FileReader(String file_name) throws FileNotFoundException {
        File file = new File(file_name);
        FileReader fileReader = new FileReader(file);
        BufferedReader bufferedReader = new BufferedReader(new FileReader(file_name));
        StringBuilder stringBuilder = new StringBuilder();
        String line;
        while (true){
            try {
                if (!((line =bufferedReader.readLine()) != null))
                    break;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            stringBuilder.append(line);
        }

//        System.out.println("line >> " + stringBuilder.toString());
        return stringBuilder.toString();

    }


}
