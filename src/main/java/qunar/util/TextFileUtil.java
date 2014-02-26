package qunar.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/**
 * read text file util
 * 
 * @author liyong19861014@gmail.com
 * 
 */
public class TextFileUtil {

    public static String read(String filePath) {
        StringBuffer sb = new StringBuffer();
        try {
            BufferedReader in = new BufferedReader(new FileReader(filePath));
            try {
                String s;
                while ((s = in.readLine()) != null) {
                    sb.append(s).append("\n");
                }
            } finally {
                in.close();
            }
        } catch (IOException e) {
//            e.printStackTrace();
        }
        return sb.toString();
    }

    public static String read(File file) {
        StringBuffer sb = new StringBuffer();
        try {
            BufferedReader in = new BufferedReader(new FileReader(file));
            try {
                String s;
                while ((s = in.readLine()) != null) {
                    sb.append(s).append("\n");
                }
            } finally {
                in.close();
            }
        } catch (IOException e) {
//            e.printStackTrace();
        }
        return sb.toString();
    }
}
