package optimize.nodes.hash;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import optimize.nodes.IMicroNode;
import optimize.nodes.ref.HashRefNodeVDev;
import optimize.util.ByteArray;

/**
 * Contrast to HNode, using ByteArray as hash key avoiding coding struggle.
 *
 * <p>About why it doesn't need a HLeaf: the key in each hash includes the trailing part, while CDM
 * and FDM needs a leaf holding the partial key after the split.
 */
public class HNodeVDev implements IMicroNode {
  // stored strings are iso encoded
  public byte[] pk;
  public Map<ByteArray, IMicroNode> children;

  public HNodeVDev() {}

  public HNodeVDev(int c) {
    children = new HashMap<>(c, 1.0f);
  }

  public HNodeVDev(String pk) {
    this.pk = pk.getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public List<byte[]> getKeyBytes() {
    return children.keySet().stream().map(ByteArray::getVal).collect(Collectors.toList());
  }

  @Override
  public void replace(byte[] key, IMicroNode nNode) {
    children.put(new ByteArray(key), nNode);
  }

  @Override
  public IMicroNode getChild(byte[] k) {
    return children.get(new ByteArray(k));
  }

  @Override
  public IMicroNode getLogicalChild(String name) {
    // equivalent to that of LNode
    final byte[] sk = name.getBytes(StandardCharsets.UTF_8);
    HNodeVDev cur = this;

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
      }
      ;

      // try all remaining key
      IMicroNode res = cur.children.get(new ByteArray(Arrays.copyOfRange(sk, i, sk.length)));
      if (res != null) {

        if (res instanceof HashRefNodeVDev) {}

        // Note(zx) sk exhausted, if the cur node has zero-len key, then that is the target
        //  meaning, there are some sibling prefixing the search key
        if (res instanceof HNodeVDev) {
          if (res.getParKey() == null
              && ((HNodeVDev) res).children.containsKey(new ByteArray(new byte[0]))) {
            return ((HNodeVDev) res).children.get(new ByteArray(new byte[0]));
          }
        }
        return res;
      }

      // no remaining, use first byte
      res = cur.children.get(new ByteArray(Arrays.copyOfRange(sk, i, i + 1)));
      cur = (HNodeVDev) res;
    }

    throw new RuntimeException("No key found.");
  }

  @Override
  public List<IMicroNode> getChildren() {
    return new ArrayList<>(children.values());
  }

  @Override
  public byte[] getParKey() {
    return pk;
  }

  @Override
  public void setParKey(byte[] _pk) {
    pk = _pk;
  }

  @Override
  public long getValue() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setChild(byte[] key, IMicroNode uc) {
    if (children == null) children = new HashMap<>(1, 1.0f);

    children.put(new ByteArray(key), uc);
  }
}
