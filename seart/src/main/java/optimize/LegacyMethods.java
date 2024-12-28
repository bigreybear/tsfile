package optimize;

import static optimize.nodes.cdm.CNodeHelper.bytes2Int;
import static optimize.nodes.cdm.CNodeHelper.extractBytes;
import static optimize.util.ArrayHelper.removeTrailingZeros;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import optimize.nodes.IMicroNode;
import optimize.nodes.ITSNode;
import optimize.nodes.cdm.CLeaf;
import optimize.nodes.cdm.CNode;
import optimize.nodes.cdm.CNode4;
import optimize.nodes.cdm.ICNode;
import optimize.nodes.fdm.FLeaf;
import optimize.nodes.fdm.IFNode;
import optimize.nodes.hash.HNodeV3;
import optimize.nodes.logic.LLeaf;
import optimize.nodes.ref.CDMRefNodeVDev;
import optimize.nodes.ref.FDMRefNodeVDev;
import optimize.nodes.ref.HashRefNodeVDev;
import optimize.util.ByteArray;

public class LegacyMethods {
  public static long searchCDMLegacy(String p, IMicroNode root) {
    String[] path = p.split("\\.");
    ICNode cur = (ICNode) root;
    int channel = -1;
    int[] brPos;
    byte[] pk, curBrKeys, checkBrKeys;
    for (int oi = 1; oi < path.length; oi++) {
      final byte[] sk = path[oi].getBytes(StandardCharsets.UTF_8);
      int idx = 0;

      if (cur instanceof CLeaf) {
        pk = cur.getParKey();
        if (pk != null) {
          for (int j = 0; j < pk.length; j++) {
            if (pk[j] != sk[idx]) throw new RuntimeException("Inconsistent key.");
            idx++;
          }
        }

        if (idx < sk.length) throw new RuntimeException("Should exhaust partial key on CLeaf.");
        if (((CLeaf) cur).ptr instanceof LLeaf) {
          return ((CLeaf) cur).ptr.getValue();
        }
        cur = (ICNode) ((CLeaf) cur).ptr;
        continue;
      }

      while (cur instanceof CNode4 && idx < sk.length) {
        pk = cur.getParKey();
        if (pk != null) {
          for (int j = 0; j < pk.length; j++) {
            if (sk[idx] != pk[j]) throw new RuntimeException("Key not exists: " + path[oi]);
            idx++;
          }

          if (idx == sk.length) {
            if (cur instanceof CLeaf) {
              cur = (ICNode) ((CLeaf) cur).ptr;
              break;
            }

            channel = cur.getBrKeyIdx(0);
            cur = cur.getPtr(channel);
            break;
          }
        }

        brPos = cur.getBranchingPos();
        if (brPos == null) break;

        curBrKeys = extractBytes(sk, brPos);
        channel = cur.getBrKeyIdx(bytes2Int(curBrKeys));
        if (channel < 0) throw new RuntimeException("Key not found: " + path[oi]);
        checkBrKeys = cur.assembleKeyAt(channel);
        for (int i = 0; i < checkBrKeys.length && idx < sk.length; i++) {
          if (checkBrKeys[i] != sk[idx]) throw new RuntimeException();
          idx++;
        }

        cur = cur.getPtr(channel);

        // if (((CNode4) cur).ptrs[channel] != null) {
        //   cur = cur.getPtrByPos(channel);
        // } else {
        //   INode res = ((CNode)cur).ptrs[channel];
        //   if (res instanceof LLeafVDev) return res.getValue();
        //   else throw new UnsupportedOperationException();
        // }
      }

      if (cur instanceof CDMRefNodeVDev) {
        return ((CDMRefNodeVDev) cur).getValFrom(sk, idx);
      }

      if (idx == sk.length && !(cur instanceof CLeaf)) {
        cur = cur.getPtr(0);
        if (cur instanceof CLeaf) {
          // a finaly leaf, just return the value
          if (((CLeaf) cur).ptr instanceof LLeaf) {
            return ((CLeaf) cur).ptr.getValue();
          } else {
            // the partial key must be for next segment, just continue
            if (cur.getParKey() != null && cur.getParKey().length > 0) {
              continue;
            } else {
              // no partial key, and not final, no branching (leaf), so must proceed once more
              cur = (ICNode) ((CLeaf) cur).ptr;
            }
            continue;
          }
        }
        // a prefixed node must be a leaf
        else throw new RuntimeException("Illegal route.");
      }

      if (cur instanceof CLeaf) {
        if (cur.getParKey() != null) {
          pk = cur.getParKey();
          for (int j = 0; j < pk.length; j++) {
            if (pk[j] != sk[idx]) throw new RuntimeException("Key Inconsistent");
            idx++;
          }
        }
        if (idx == sk.length) {
          // cur = (ICNode) ((CLeaf) cur).ptr;
          ICNode res = ((CLeaf) cur).ptr;
          if (res instanceof LLeaf) return res.getValue();
          cur = res;
          continue;
        } else {
          throw new UnsupportedOperationException();
        }
      }

      // if (idx == sk.length && !(cur instanceof CLeaf)) {
      //   // sk exhausted, so there is an immediate-prefix node
      //   cur = cur.getPtrByPos(cur.getBrKeyIdx(0));
      // }

      while (cur instanceof CNode && idx < sk.length) {
        int pidx = idx;
        pk = cur.getParKey();
        if (pk != null) {
          for (int j = 0; j < pk.length; j++) {
            if (sk[idx] != pk[j]) throw new RuntimeException("Key not consistent with partial key");
            idx++;
          }
        }

        channel = cur.getBrKeyIdx(removeTrailingZeros(extractBytes(sk, cur.getBranchingPos())));
        checkBrKeys = ((CNode) cur).assembleKeyAt(channel, pidx, sk.length);
        for (int j = 0; j < checkBrKeys.length; j++) {
          if (sk[idx] != checkBrKeys[j])
            throw new UnsupportedOperationException("Inconsistent on assemble key.");
          idx++;
        }

        if (((CNode) cur).getPtr(channel) instanceof ICNode) {
          cur = cur.getPtr(channel);
        } else {
          ICNode res = ((CNode) cur).getPtr(channel);
          if (res instanceof LLeaf) return res.getValue();
          else throw new UnsupportedOperationException();
        }
      }

      if (idx == sk.length) {
        if (cur instanceof CLeaf) {
          if (cur.getParKey() != null && cur.getParKey().length != 0) {
            continue;
          }

          // next level as normal
          if (((CLeaf) cur).ptr instanceof LLeaf) {
            return ((CLeaf) cur).ptr.getValue();
          }
          cur = (ICNode) ((CLeaf) cur).ptr;
          continue;
        }

        if (cur instanceof CDMRefNodeVDev) {
          // next level as template
          continue;
        }
      }

      // if (idx == sk.length && !(cur instanceof CLeaf)) {
      //   // sk exhausted, so there is an immediate-prefix node
      //   cur = cur.getPtrByPos(cur.getBrKeyIdx(0));
      // }
      if (cur instanceof CDMRefNodeVDev) {
        return ((CDMRefNodeVDev) cur).getValFrom(sk, idx);
      }

      if (cur == null) throw new RuntimeException("Key not found");
    }

    byte[] sk = path[path.length - 1].getBytes(StandardCharsets.UTF_8);
    int idx = 0;
    if (cur instanceof CLeaf) {
      pk = cur.getParKey();
      if (pk != null) {
        for (int j = 0; j < pk.length; j++) {
          if (pk[j] != sk[idx]) throw new RuntimeException("Inconsistent key.");
          idx++;
        }
      }

      if (idx < sk.length) throw new RuntimeException("Should exhaust partial key on CLeaf.");
      if (((CLeaf) cur).ptr instanceof LLeaf) {
        return ((CLeaf) cur).ptr.getValue();
      }
      cur = (ICNode) ((CLeaf) cur).ptr;
    }

    if (cur instanceof CDMRefNodeVDev) {
      return ((CDMRefNodeVDev) cur).getValFrom(sk, idx);
    }
    return cur.getValue();
  }

  public static long searchFDMLegacy(String p, ITSNode root) {
    String[] path = p.split("\\.");
    IFNode cur = (IFNode) root;
    IFNode res = cur;
    byte[] pk;

    boolean directLeaf = false, prefixedLeaf = false, resTemplate = false;
    for (int oi = 1; oi < path.length; oi++) {
      byte[] kbs = path[oi].getBytes(StandardCharsets.UTF_8);
      pk = cur.getParKey();

      for (int i = 0; i < kbs.length; ) {
        pk = cur.getParKey();
        i += IFNode.matchLen(pk, kbs, i);

        if (i == kbs.length && cur instanceof FLeaf) {
          directLeaf = true;
          break;
        }

        if (i == kbs.length) {
          res = cur.get((byte) 0);
          prefixedLeaf = true;
          break;
        }

        if (i < kbs.length) {
          res = cur.get(kbs[i]);
          i++;
          // the logic is, as kbs not exhausted, res should not be a FLeaf
          if (res != null) cur = res;
          else {
            // should be in template
            if (i == kbs.length) {
              return ((FDMRefNodeVDev) res)
                  .getValFrom(path[oi + 1].getBytes(StandardCharsets.UTF_8), 0);
            } else {
              return ((FDMRefNodeVDev) res).getValFrom(kbs, i);
            }
          }
        }
      }

      if (prefixedLeaf) {
        prefixedLeaf = false;
        res = res.getFValue();
        if (res instanceof LLeaf) {
          return res.getValue();
        }
        cur = res;
        continue;
      }

      if (directLeaf) {
        directLeaf = false;
        if (oi == path.length - 1) return cur.getFValue().getValue();
        if (cur.getFValue() instanceof FDMRefNodeVDev) {
          return ((FDMRefNodeVDev) cur.getFValue())
              .getValFrom(path[oi + 1].getBytes(StandardCharsets.UTF_8), 0);
        }
        cur = cur.getFValue();
        continue;
      }

      if (res.getParKey() == null) {
        if (res instanceof FLeaf) {
          res = res.getFValue();
          if (res instanceof LLeaf) {
            return res.getValue();
          }
          if (res instanceof FDMRefNodeVDev) {
            return ((FDMRefNodeVDev) res)
                .getValFrom(path[oi + 1].getBytes(StandardCharsets.UTF_8), 0);
          }
          cur = res;
          continue;
        } else if (res.get((byte) 0) != null) {
          res = res.get((byte) 0);
          cur = res.getFValue();
          continue;
        }
      }

      if (res instanceof FDMRefNodeVDev) {
        return ((FDMRefNodeVDev) res).getValFrom(path[oi + 1].getBytes(StandardCharsets.UTF_8), 0);
      }

      if (cur instanceof FLeaf) {
        if (cur.getFValue() instanceof LLeaf) return cur.getFValue().getValue();
        cur = cur.getFValue();
      }
    }
    return cur.getValue();
  }

  public static long searchHashLegacy(String p, IMicroNode root) {
    String[] path = p.split("\\.");
    HNodeV3 cur = (HNodeV3) root;
    IMicroNode res = null;
    byte[] EMPTY_ARRAY = new byte[0];
    byte[] pk;
    for (int _i = 1; _i < path.length; _i++) {

      // cur = cur.getChild(path[i]);
      // inlined
      final byte[] sk = path[_i].getBytes(StandardCharsets.UTF_8);
      for (int i = 0; i < sk.length; i++) {
        pk = cur.getParKey();
        if (pk != null) {
          for (int j = 0; j < pk.length; j++) {
            if (sk[i] == pk[j]) i++;
            else throw new RuntimeException("Key not consistent on Partial key.");
          }
        }

        if (cur.children == null) throw new RuntimeException("Null chilren.");
        if (i == sk.length) {
          // prefixed child
          res = cur.getChild(EMPTY_ARRAY);
          break;
        }
        ;

        // try all remaining key
        res = cur.children.get(new ByteArray(Arrays.copyOfRange(sk, i, sk.length)));
        if (res != null) {

          if (res instanceof HashRefNodeVDev) {
            // current segment exhausted, next node all for template
            return getValFromHashTemplate(
                (HashRefNodeVDev) res, path[_i + 1].getBytes(StandardCharsets.UTF_8), 0);
          }

          // Note(zx) sk exhausted, if the cur node has zero-len key, then that is the target
          //  meaning, there are some sibling prefixing the search key
          if (res instanceof HNodeV3) {
            if (res.getParKey() == null && ((HNodeV3) res).children.containsKey(EMPTY_ARRAY)) {
              res = ((HNodeV3) res).children.get(EMPTY_ARRAY);
              break;
            }
          }
          break;
        }

        // no remaining, use first byte
        res = cur.children.get(new ByteArray(Arrays.copyOfRange(sk, i, i + 1)));

        if (res instanceof HashRefNodeVDev) {
          return getValFromHashTemplate((HashRefNodeVDev) res, sk, i + 1);
        }

        cur = (HNodeV3) res;
      }
      if (res instanceof LLeaf) {
        return res.getValue();
      }
      if (res instanceof HashRefNodeVDev) {
        return getValFromHashTemplate(
            (HashRefNodeVDev) res, path[_i + 1].getBytes(StandardCharsets.UTF_8), 0);
      }
      cur = (HNodeV3) res;
      // inlined end

      if (cur == null) throw new RuntimeException("Key not found");
    }
    return cur.getValue();
  }

  private static long getValFromHashTemplate(HashRefNodeVDev res, byte[] sk, int i) {
    byte[] _pk = res.pk;
    for (int _in = 0; _pk != null && _in < _pk.length; _in++) {
      if (_pk[_in] == sk[i]) i++;
      else throw new RuntimeException("Key not consistent on Partial key.");
    }

    int _order = (int) res.template.getChild(Arrays.copyOfRange(sk, i, sk.length)).getValue();
    return res.values[_order];
  }
}
