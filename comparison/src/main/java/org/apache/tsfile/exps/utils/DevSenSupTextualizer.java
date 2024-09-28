package org.apache.tsfile.exps.utils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

public class DevSenSupTextualizer {

  public static void main(String[] args) throws IOException, ClassNotFoundException {

    String prjDir = "E:\\ExpDataSets\\Source-TsFile\\";
    String srcFile =  "CCS.sup";
    String dstFile = prjDir + srcFile + ".txt";

    DevSenSupport support = DevSenSupport.deserialize(prjDir + srcFile);

    try (BufferedWriter writer = new BufferedWriter(new FileWriter(dstFile))) {
      for (Map.Entry<String, Set<String>> entry : support.map.entrySet()) {
        writer.write(entry.getKey());
        writer.newLine();

        String values = String.join(" ", entry.getValue());
        writer.write(values);
        writer.newLine();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
