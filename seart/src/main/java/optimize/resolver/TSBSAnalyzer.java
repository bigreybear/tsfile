package optimize.resolver;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TSBSAnalyzer {


  public static void main(String[] args) {
    if (args.length < 1) {
      args = new String[] {"mtreedata/tsbs_sample.txt"};
      // System.out.println("Usage: java TagValueCounter <filename>");
      // System.exit(1);
    }

    String filename = args[0];
    Map<String, Set<String>> tagValues = new HashMap<>();

    try (BufferedReader reader = new BufferedReader(new FileReader(filename))) {
      String line;
      while ((line = reader.readLine()) != null) {
        // 只处理以"tags,"开头的行
        if (line.startsWith("tags,")) {
          processTags(line, tagValues);
        }
      }

      // 输出结果，按照值的多样性排序（从少到多）
      List<Map.Entry<String, Set<String>>> sortedEntries = new ArrayList<>(tagValues.entrySet());
      sortedEntries.sort(Comparator.comparingInt(e -> e.getValue().size()));

      System.out.println("标签统计结果 (按唯一值数量排序):");
      System.out.println("===================================");
      for (Map.Entry<String, Set<String>> entry : sortedEntries) {
        System.out.printf("%-25s: %d 种不同取值%n", entry.getKey(), entry.getValue().size());
        // // 如果想查看具体的取值，可以取消下面这行的注释
        System.out.println("  取值: " + entry.getValue());
      }

    } catch (IOException e) {
      System.err.println("读取文件时出错: " + e.getMessage());
      e.printStackTrace();
    }
  }

  private static void processTags(String line, Map<String, Set<String>> tagValues) {
    String tagsContent = line.substring(5);

    String[] tags = tagsContent.split(",");

    for (String tag : tags) {
      if (tag.contains("=")) {
        String[] parts = tag.split("=", 2);
        String tagName = parts[0].trim();
        String tagValue = parts[1].trim();
        // if (tagValue.equals("")) {
        //   System.out.println("AA");
        // }
        tagValues.computeIfAbsent(tagName, k -> new HashSet<>()).add(tagValue);
      }
    }
  }
}
