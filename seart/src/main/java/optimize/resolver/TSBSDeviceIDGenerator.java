package optimize.resolver;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TSBSDeviceIDGenerator {
  // Define tag order as constants to avoid repeated creation
  private static final String[] ORDERED_TAGS = {
      "fleet", "model", "device_version", "load_capacity",
      "fuel_capacity", "nominal_fuel_consumption", "driver", "name"
  };
  private static final String ROOT = "root";

  public static void main(String[] args) {
    if (args.length < 2) {
      System.out.println("用法: java DeviceIdentifierGenerator <输入文件> <输出文件>");
      // System.exit(1);
      args = new String[] {"mtreedata/tsbs_sample.txt", "mtreedata/tsbs_sample_dev_id.txt"};
    }

    String inputFile = args[0];
    String outputFile = args[1];

    // Use Set to track unique device identifiers, but write to file immediately when found
    Set<String> seenIdentifiers = new HashSet<>();
    int uniqueCount = 0;

    // Reuse these objects to reduce GC pressure
    String[] paths = new String[9];
    paths[0] = "root";

    try (BufferedReader reader = new BufferedReader(new FileReader(inputFile));
         BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {

      String line;
      int idx;
      while ((line = reader.readLine()) != null) {
        boolean nullFlag = false;
        // Only process lines starting with "tags,"
        if (line.startsWith("tags,")) {
          // Clear previous data
          Arrays.fill(paths, null);
          paths[0] = ROOT;

          // Skip "tags," prefix
          String tagsContent = line.substring(5);

          // Parse all tag-value pairs
          int start = 0;
          int end;
          int equalsPos;

          // Manually split string to avoid creating String[] arrays
          while (start < tagsContent.length()) {
            end = tagsContent.indexOf(',', start);
            if (end == -1) end = tagsContent.length();

            String pair = tagsContent.substring(start, end);
            equalsPos = pair.indexOf('=');

            if (equalsPos != -1) {
              String tagName = pair.substring(0, equalsPos).trim();
              String tagValue = pair.substring(equalsPos + 1).trim();

              if (!tagValue.isEmpty()) {
                // skip as this is an incomplete entry, some field are left out
                idx = findIndexInArray(ORDERED_TAGS, tagName);
                if (idx < 0) throw new RuntimeException();
                paths[idx + 1] = tagValue;
              } else {
                nullFlag = true;
              }
            }
            start = end + 1;
          }

          if (nullFlag || paths[1] == null)
            continue;

          // String identifier = deviceId.toString();
          String identifier = joinStrings(paths, ".");

          // If this is a new identifier, write it to file immediately
          if (seenIdentifiers.add(identifier)) {
            writer.write(identifier);
            writer.newLine();
            uniqueCount++;
          }
        }
      }

      System.out.println("成功生成 " + uniqueCount + " 个 Device ID " + outputFile);

    } catch (IOException e) {
      System.err.println("处理文件时出错: " + e.getMessage());
      e.printStackTrace();
    }
  }

  public static int findIndexInArray(String[] array, String target) {
    if (array == null || target == null) {
      return -1;
    }

    for (int i = 0; i < array.length; i++) {
      if (target.equals(array[i])) {
        return i;
      }
    }

    return -1; // Not found
  }

  public static String joinStrings(String[] array, String delimiter) {
    if (array == null || array.length == 0) {
      return "";
    }

    StringBuilder result = new StringBuilder();

    // Append first element without preceding delimiter
    result.append(array[0]);

    // Append remaining elements with delimiter
    for (int i = 1; i < array.length; i++) {
      result.append(delimiter);
      result.append(array[i]);
    }

    return result.toString();
  }
}
