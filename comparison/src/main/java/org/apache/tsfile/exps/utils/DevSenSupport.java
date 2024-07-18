package org.apache.tsfile.exps.utils;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/* Record mappings between devices and sensors for sparse tables. */
public class DevSenSupport implements Serializable {
  private static final long serialVersionUID = 6395645743397020735L;
  public final Map<String, Set<String>> map = new HashMap<>();

  private void add(String d, String s) {
    if (s == null) {
      // for time chunk
      return;
    }
    if (!map.containsKey(d)) {
      map.put(d, new HashSet<>());
    }
    map.get(d).add(s);
  }

  void addByChunks(String d, List<TsFileSequentialConvertor.ChunkData> chunkData) {
    chunkData.forEach(c -> add(d, c.name));
  }

  public static void serialize(DevSenSupport dss, String path) throws IOException {
    ObjectOutputStream oos = new ObjectOutputStream(Files.newOutputStream(Paths.get(path)));
    oos.writeObject(dss);
    oos.close();
  }

  public static DevSenSupport deserialize(String path) throws IOException, ClassNotFoundException {
    try (ObjectInputStream ois = new ObjectInputStream(Files.newInputStream(Paths.get(path)))) {
      return (DevSenSupport) ois.readObject();
    }
  }

  public static void main(String[] args) throws IOException, ClassNotFoundException {
    DevSenSupport s = deserialize("E:\\ExpDataSets\\new_arrow_src\\REDD.sup");
    System.out.println("CHECK SUP");
  }
}
