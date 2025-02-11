package optimize;

import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.info.GraphLayout;
import sun.misc.Unsafe;

import java.lang.reflect.Field;

public class UnsafePlayer {

  public static Unsafe unsafe;

  static {
    try {
      Field f = Unsafe.class.getDeclaredField("theUnsafe");
      f.setAccessible(true);
      unsafe = (Unsafe) f.get(null);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static void checkByte() {
    System.out.println("check byte.");
    byte[] arr = new byte[] {(byte) 1, (byte) 2, (byte) 3, (byte) 4, (byte) 5};
    System.out.println(ClassLayout.parseInstance(arr).toPrintable());
    unsafe.putInt(arr, 12, 66);
    unsafe.putInt(arr, 16, 0x12345678);
    System.out.println(arr.length);
    System.out.println(Integer.toHexString(arr[0] & 0xff));
  }

  public static void checkInt() {
    System.out.println("check int.");
    int[] arr = new int[] {1,2,3,4,5};
    System.out.println(ClassLayout.parseInstance(arr).toPrintable());
    unsafe.putInt(arr, 12, 66);
    unsafe.putInt(arr, 16, 0x12345678);
    System.out.println(arr.length);
    System.out.println(Integer.toHexString(arr[0]));
  }

  public static void checkShort() {
    System.out.println("check short.");
    short[] arr = new short[] {1,2,3,4,5};
    System.out.println(ClassLayout.parseInstance(arr).toPrintable());
    unsafe.putInt(arr, 12, 66);
    unsafe.putInt(arr, 16, 0x12345678);
    System.out.println(arr.length);
    System.out.println(Integer.toHexString(arr[0] & 0xffff));

  }

  public static void checkLong() {
    System.out.println("check long.");
    long[] arr = new long[] {1,2,3,4,5};
    System.out.println(ClassLayout.parseInstance(arr).toPrintable());
    unsafe.putInt(arr, 12, 66);
    unsafe.putInt(arr, 16, 0x12345678);
    System.out.println(arr.length);
    System.out.println(Long.toHexString(arr[0]));
  }

  public static void main(String[] args) throws Exception{
    // checkByte();
    // checkInt();
    // checkShort();
    // checkLong();


    short[] sa = new short[11];
    int[] ia = new int[11];
    long[] la = new long[11];

    System.out.println(ClassLayout.parseInstance(sa).toPrintable());
    System.out.println(ClassLayout.parseInstance(ia).toPrintable());
    System.out.println(ClassLayout.parseInstance(la).toPrintable());
  }
}
