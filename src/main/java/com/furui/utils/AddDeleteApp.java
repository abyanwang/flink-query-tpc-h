package com.furui.utils;

import java.io.*;
import java.util.Random;

public class AddDeleteApp {

    public static void main(String[] args) throws IOException {
        processCustomerData("/Users/free/Projects/ipdatabig/customer.tbl", "/Users/free/Projects/ipdatabig/customer.tbl", 0.01);
    }

    private static void processCustomerData(String inputPath, String outputPath, double selectRatio) throws IOException {
        // 用try-with-resources自动关闭流，避免资源泄漏
        try (BufferedReader br = new BufferedReader(new FileReader(inputPath));
             BufferedWriter bw = new BufferedWriter(new FileWriter(outputPath))) {

            String line;
            int totalLine = 0; // 总行数
            int selectedLine = 0; // 选中的行数

            // 逐行读取并处理
            while ((line = br.readLine()) != null) {
                totalLine++;
                // 跳过空行
                if (line.trim().isEmpty()) {
                    bw.write(line); // 空行直接写入
                    bw.newLine();
                    continue;
                }

                // 生成0~1的随机数，判断是否选中当前行
                if (new Random().nextDouble() <= selectRatio) {
                    // 选中：在末尾添加DELETE|
                    line = line + "DELETE|";
                    selectedLine++;
                }

                // 写入处理后的行
                bw.write(line);
                bw.newLine();
            }

            // 打印统计信息
            System.out.println("处理完成统计：");
            System.out.println("总行数：" + totalLine);
            System.out.println("选中并添加DELETE|的行数：" + selectedLine);
            System.out.println("实际选取比例：" + String.format("%.2f%%", (double) selectedLine / totalLine * 100));

        } catch (IOException e) {
            throw new IOException("文件读写失败：" + e.getMessage(), e);
        }
    }
}
