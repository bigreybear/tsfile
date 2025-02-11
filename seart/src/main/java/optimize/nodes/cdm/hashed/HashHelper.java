package optimize.nodes.cdm.hashed;

public class HashHelper {

  public static int hash1(short s) {
    int hash = s * 0x4F3B;
    return (hash ^ (hash >> 8)) & 0xFFFF;
  }

  public static int hash1(int i) {
    int hash = (i ^ (i >> 16)) * 0x85EBCA77;
    return (hash ^ (hash >> 13)) & 0x7fff_ffff;
  }

  public static int hash1(long key) {
    int low = (int) (key & 0xFFFFFFFFL);
    int high = (int) ((key >> 32));
    return (low ^ high) & 0x7ffff_fff;
  }

  public static int rehash(int hash, short key) {
    return hash + 1;
  }

  public static int rehash(int hash, int key) {
    return hash + 1;
  }

  public static int rehash(int hash, long key) {
    return hash + 1;
  }
}
