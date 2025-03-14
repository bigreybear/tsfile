package optimize.nodes.cdm.hashed;

public class HashHelper {

  public static int hash1(short s) {
    int x = s & 0xFFFF;
    x = ((x >>> 8) ^ x) * 0x45d9f3b;
    x = ((x >>> 8) ^ x) * 0x45d9f3b;
    x = (x >>> 8) ^ x;
    return x & 0xFFFF;
  }

  public static int hash1(int key) {
    key = (key ^ (key >>> 16)) * 0x85ebca6b;
    key = (key ^ (key >>> 13)) * 0xc2b2ae35;
    return (key ^ (key >>> 16)) & 0x7FFFFFFF; // 保证结果为正
  }

  public static int hash1(long key) {
    key = (key ^ (key >>> 33)) * 0xff51afd7ed558ccdL;
    key = (key ^ (key >>> 33)) * 0xc4ceb9fe1a85ec53L;
    return ((int)(key ^ (key >>> 33))) & 0x7FFFFFFF; // 保证结果为正
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
