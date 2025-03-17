package optimize.util;

import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.time.LocalDateTime;

public class LoggedPrintStream extends PrintStream {
  String loggedFile;
  StringBuilder builder = new StringBuilder();

  public LoggedPrintStream(OutputStream out, String loggedFile) {
    super(out);
    this.loggedFile = loggedFile;
  }

  @Override
  public void println(String x) {
    super.println(x);
    builder.append(LocalDateTime.now()).append("\n").append(x).append("\n");
  }

  @Override
  public void println(Object x) {
    println(x.toString());
  }

  @Override
  public void flush() {
    super.flush();
    try (FileWriter fileWriter = new FileWriter(loggedFile, true)) {
      fileWriter.write(builder.toString());
    } catch (IOException e) {
      e.printStackTrace();
    }
    builder.delete(0, builder.length());
  }
}
