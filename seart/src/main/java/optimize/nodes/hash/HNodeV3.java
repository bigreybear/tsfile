package optimize.nodes.hash;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import optimize.SearchStatus;
import optimize.nodes.IMicroNode;
import optimize.nodes.NodeInspector;
import optimize.nodes.NodeWithPartialKey;
import optimize.nodes.ref.HashRefNodeVDev;
import optimize.util.ByteArray;

import static optimize.Main.tableField;

/**
 * Contrast to HNode, using ByteArray as hash key avoiding coding struggle.
 *
 * <p>About why it doesn't need a HLeaf: the key in each hash includes the trailing part, while CDM
 * and FDM needs a leaf holding the partial key after the split.
 */
public class HNodeV3 extends NodeWithPartialKey implements IMicroNode {
  // stored strings are iso encoded
  public Map<ByteArray, IMicroNode> children;
  protected static ByteArray EMPTY_BA = new ByteArray(new byte[0]);

  public HNodeV3() {}

  public HNodeV3(int c) {
    children = new HashMap<>(c, 1.0f);
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
    return (k == null || k.length == 0) ? children.get(EMPTY_BA) : children.get(new ByteArray(k));
  }

  @Override
  public IMicroNode getLogicalChild(String name) {
    // equivalent to that of LNode
    final byte[] sk = name.getBytes(StandardCharsets.UTF_8);
    HNodeV3 cur = this;

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
        if (res instanceof HNodeV3) {
          if (res.getParKey() == null
              && ((HNodeV3) res).children.containsKey(new ByteArray(new byte[0]))) {
            return ((HNodeV3) res).children.get(new ByteArray(new byte[0]));
          }
        }
        return res;
      }

      // no remaining, use first byte
      res = cur.children.get(new ByteArray(Arrays.copyOfRange(sk, i, i + 1)));
      cur = (HNodeV3) res;
    }

    throw new RuntimeException("No key found.");
  }

  @Override
  public List<IMicroNode> getChildren() {
    return new ArrayList<>(children.values());
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

  public IMicroNode getHashChild(final byte[] key, final SearchStatus sts) {
    if (sts.getCurLen() == key.length) {
      sts.setFinished(true);
      IMicroNode res = getChild(null);
      return res == null ? this : res;
    }

    int curLen = pk == null ? sts.getCurLen() : checkPartialKey(key, sts.getCurLen(), -1);
    if (curLen == key.length) {
      sts.setFinished(true);
      return getChild(null);
    }

    // try the remaining bytes then the first byte
    IMicroNode res = getChild(Arrays.copyOfRange(key, curLen, key.length));
    if (res != null) {
      sts.setFinished(true); // fixme should set or not?
      if (res.getParKey() != null) {
        return res;
      }

      // sts.setCurLen(key.length);
      IMicroNode res2 = res.getChild(null);
      return res2 == null ? res : res2;
    }

    res = getChild(Arrays.copyOfRange(key, curLen, curLen + 1));
    sts.setCurLen(curLen + 1);
    return res;
  }

  @Override
  public void acceptInspector(NodeInspector noi) {
    if (getParKey() != null) noi.appendEntry("HNode_pk_len", getParKey().length);
    noi.appendEntry("HNode_chd_siz", children.size());
    noi.appendEntry("HNode_chd_key_len", children.keySet().stream().mapToInt(i->i.getVal().length).sum());
    try {
      noi.appendEntry("HNode_map_cap", ((Object[]) tableField.get(children)).length);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }
}
