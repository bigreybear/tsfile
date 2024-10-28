package loader;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PathTxtLoader {

  public static String FILE_PATH = "mtreedata/text_series.txt";

  private final BufferedReader reader;

  public PathTxtLoader(String file) throws FileNotFoundException {
    this.reader = new BufferedReader(new FileReader(file));
  }

  public List<String> getLines() throws IOException {
    return getLines(100);
  }

  public List<String> getLines(int size) throws IOException {
    List<String> res = new ArrayList<>();
    String nextLine;
    while (res.size() < size && (nextLine = reader.readLine()) != null) {
      res.add(nextLine);
    }
    return res;
  }

  public void close() throws IOException {
    reader.close();
  }

  public List<String> getAllLines() throws Exception {
    List<String> res = new ArrayList<>();
    String nextLine;
    while ((nextLine = reader.readLine()) != null) {
      res.add(nextLine);
    }
    return res;
  }

  public static void main(String[] args) throws IOException {}
}
