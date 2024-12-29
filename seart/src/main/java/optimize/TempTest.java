package optimize;

import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.info.GraphLayout;
import org.openjdk.jol.info.GraphStats;

import java.util.HashMap;
import java.util.Map;

public class TempTest {

  private Main[] st = new Main[3];

  public static void main(String[] args) {
    System.out.println("Hello");


    TempTest[] a = new TempTest[3];
    a[0] = new TempTest();
    a[0].st[0] = new Main();
    a[1] = new TempTest();
    a[2] = new TempTest();


    System.out.println(GraphLayout.parseInstance(a[0].st[0]).totalSize());
    System.out.println(GraphLayout.parseInstance(a).totalSize());
    // System.out.println(GraphLayout.parseInstance(a).toPrintable());
    // Note(zx) note this method does not include objects within arrays
    System.out.println(GraphStats.parseInstance(a).totalSize());
    // System.out.println(ClassLayout.parseInstance(a).toPrintable());


    Map<String, Integer> map = new HashMap<>();
    map.compute("a", (k,v)->{
      if (v==null) return 111;
      else return 111+v;
    });

    System.out.println(map.get("a"));
    map.compute("a", (k,v)->{
      if (v==null) return 111;
      else return 111+v;
    });
    System.out.println(map.get("a"));
  }
}
