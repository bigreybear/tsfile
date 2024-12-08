package optimize.nodes.hash;

import optimize.nodes.IInternal;
import optimize.nodes.INode;
import optimize.nodes.IStaticNode;
import optimize.nodes.UNode;
import optimize.nodes.ref.HashRefNode;
import optimize.util.ByteArray;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Contrast to {@link HNode}, using ByteArray as hash key avoiding coding struggle. <p/>
 * About why it doesn't need a HLeaf: the key in each hash includes the trailing part, while
 * CDM and FDM needs a leaf holding the partial key after the split.
 */
public class HNodeV2 implements IStaticNode, IInternal, UNode {
  // stored strings are iso encoded
  public byte[] pk;
  public Map<ByteArray, INode> children;

  public HNodeV2() {}

  public HNodeV2(int c) {
    children = new HashMap<>(c, 1.0f);
  }

  public HNodeV2(String pk) {
    this.pk = pk.getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public List<byte[]> getKeyBytes() {
    return children.keySet().stream().map(ByteArray::getVal).collect(Collectors.toList());
  }

  @Override
  public INode replace(byte[] key, INode nNode) {
    return children.put(new ByteArray(key), nNode);
  }

  @Override
  public INode getChildByBytes(byte[] k) {
    return children.get(new ByteArray(k));
  }

  @Override
  public INode getChild(String name) {
    // equivalent to that of LNode
    final byte[] sk = name.getBytes(StandardCharsets.UTF_8);
    HNodeV2 cur = this;

    for (int i = 0; i < sk.length; i++) {
      if (cur.pk != null) {
        for (int j = 0; j < cur.pk.length; j++) {
          if (sk[i] == cur.pk[j]) i++;
          else throw new RuntimeException("Key not consistent on Partial key.");
        }
      }

      if (cur.children == null) throw new RuntimeException("Null chilren.");
      if (i == sk.length) {
        // prefixed child
        return cur.children.get(new ByteArray(new byte[0]));
      };

      // try all remaining key
      INode res = cur.children.get(new ByteArray(Arrays.copyOfRange(sk, i, sk.length)));
      if (res != null) {

        if (res instanceof HashRefNode) {

        }

        // Note(zx) sk exhausted, if the cur node has zero-len key, then that is the target
        //  meaning, there are some sibling prefixing the search key
        if (res instanceof HNodeV2) {
          if (res.getPartialKey() == null && ((HNodeV2) res).children.containsKey(new ByteArray(new byte[0]))) {
            return ((HNodeV2) res).children.get(new ByteArray(new byte[0]));
          }
        }
        return res;
      }

      // no remaining, use first byte
      res = cur.children.get(new ByteArray(Arrays.copyOfRange(sk, i, i+1)));
      cur = (HNodeV2) res;
    }

    throw new RuntimeException("No key found.");
  }

  @Override
  public List<INode> getChildren() {
    return new ArrayList<>(children.values());
  }

  @Override
  public List<String> getKeys() {
    return null;
  }

  @Override
  public byte[] getPartialKey() {
    return pk;
  }

  public INode replace(String s, INode nNode) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void add(byte[] key, INode uc) {
    if (children == null) children = new HashMap<>(1, 1.0f);

    children.put(new ByteArray(key), uc);
  }

  @Override
  public INode addChild(String name, INode child) {
    return null;
  }

  public boolean hasChild(String name) {
    return children != null && children.containsKey(name);
  }
}
