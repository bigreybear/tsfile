package optimize.nodes.hash;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import optimize.nodes.IInternal;
import optimize.nodes.INode;
import optimize.nodes.IStaticNode;

public class HNode implements IStaticNode, IInternal {
  // stored strings are iso encoded
  public byte[] pk;
  public Map<String, INode> children;

  public HNode() {}

  public HNode(int c) {
    children = new HashMap<>(c, 1.0f);
  }

  public HNode(String pk) {
    this.pk = pk.getBytes(StandardCharsets.UTF_8);
  }

  // 和 lnode 等效替换，所以 string 入参都是 utf 的，内部记 iso 编码的
  @Override
  public INode getChild(String name) {
    byte[] qk = name.getBytes(StandardCharsets.UTF_8);

    HNode cur = this;
    int inner = 0; // inner index within each node
    for (int i = 0 /* idx for qk */; i < qk.length; ) {
      if (cur.pk != null && inner < cur.pk.length) {
        if (cur.pk[inner] == qk[i]) {
          i++;
          inner++;
          continue;
        } else {
          throw new RuntimeException("Key not found");
        }
      }

      // fixme: should not allow a child from prefixed ptr holding any partial key - chaos must be
      // removed
      // qk not exhausted but no children map, should try prefix-way
      if (cur.children == null && cur instanceof PrefixedHNode) {
        cur = (HNode) ((PrefixedHNode) cur).prePtr;
        inner = 0;
        i++;
        continue;
      }

      if (cur.children == null) throw new RuntimeException("Null chilren.");

      INode res;
      if ((res =
              cur.children.get(
                  new String(Arrays.copyOfRange(qk, i, qk.length), StandardCharsets.ISO_8859_1)))
          != null) {
        // search key used
        return res;
      } else if ((res =
              cur.children.get(
                  new String(Arrays.copyOfRange(qk, i, i + 1), StandardCharsets.ISO_8859_1)))
          != null) {
        // search key not exhausted, res must be a HNode
        cur = (HNode) res;
        inner = 0;
        i++;
      } else {
        // fixme: ugly and inefficient to repeat this check
        if (cur instanceof PrefixedHNode) {
          cur = (HNode) ((PrefixedHNode) cur).prePtr;
          inner = 0;
          continue;
        }
        throw new UnsupportedOperationException("No key found: " + name);
      }
    }

    if (cur instanceof PrefixedHNode) {
      if (((PrefixedHNode) cur).prePtr == null) {
        throw new RuntimeException("Incomplete search key:" + name);
      }

      return ((PrefixedHNode) cur).prePtr;
    }

    throw new RuntimeException("Wrong node type.");
    // for (int i = 0; pk != null && i < pk.length; i++) {
    //   if (qk[i] != pk[i]) return null;
    // }
    //
    // String rs = pk == null
    //     ? name
    //     : new String(Arrays.copyOfRange(qk, pk.length, qk.length), StandardCharsets.ISO_8859_1);
    // return children.get(rs);
  }

  @Override
  public List<INode> getChildren() {
    return new ArrayList<>(children.values());
  }

  @Override
  public List<String> getKeys() {
    return new ArrayList<>(children.keySet());
  }

  @Override
  public byte[] getPartialKey() {
    return pk;
  }

  public INode replace(String s, INode nNode) {
    return children.put(s, nNode);
  }

  // basically an add without dup check
  public INode setChild(String s, INode n) {
    if (children == null) {
      this.children = new HashMap<>(1);
    }
    return children.put(s, n);
  }

  @Override
  public INode addChild(String name, INode child) {
    if (children == null) {
      this.children = new HashMap<>(1, 1.0f);
    }

    if (hasChild(name)) {
      throw new RuntimeException("Duplicated children: " + name);
    }
    return children.put(name, child);
  }

  public boolean hasChild(String name) {
    return children != null && children.containsKey(name);
  }

  public static void main(String[] args) {
    // 示例 2：无效的 UTF-8 字节数组
    byte[] invalidUtf8 = {(byte) 0xC3, (byte) 0x28}; // 无效的 UTF-8 序列
    String a2 = new String(invalidUtf8, StandardCharsets.ISO_8859_1);
    byte[] c2 = a2.getBytes(StandardCharsets.ISO_8859_1);
    System.out.println("无效 UTF-8 转换后是否相同: " + Arrays.equals(invalidUtf8, c2));
    // 输出: 无效 UTF-8 转换后是否相同: false
  }
}
