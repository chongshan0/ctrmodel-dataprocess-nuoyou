package process.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class FileUtil {

    /**
     * 将list按行写入到txt文件中
     * 如果没有文件就创建
     */
    public static void write(List<String> strings, String path) throws IOException {
        File file = new File(path);
        if (!file.isFile()) {
            file.createNewFile();
        }
        BufferedWriter writer = new BufferedWriter(new FileWriter(path));
        for (String line : strings) {
            writer.write(line + "\r\n");
        }
        writer.close();
        System.out.println("写文件完成" + path);
    }

    /**
     * 将list按行写入到txt文件中，如果没有文件就创建
     * 带文件的第一行
     */
    public static void writeWithHeadline(String headLine, List<String> strings, String path) throws IOException {
        File file = new File(path);
        if (!file.isFile()) {
            file.createNewFile();
        }
        BufferedWriter writer = new BufferedWriter(new FileWriter(path));
        writer.write(headLine + "\r\n");    //headLine
        for (String line : strings) {
            writer.write(line + "\r\n");
        }
        writer.close();
        System.out.println("写文件完成" + path);
    }
}
