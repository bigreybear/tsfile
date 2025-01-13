package optimize;

import org.openjdk.jol.info.ClassLayout;

public class TempTest {

  int pot;
  byte a, b, c, d;

  public static void main(String[] args) {
    TempTest tt = new TempTest();
    System.out.println(ClassLayout.parseInstance(tt).toPrintable());
  }
}
