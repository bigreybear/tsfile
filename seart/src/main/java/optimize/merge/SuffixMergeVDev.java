package optimize.merge;

import static optimize.Main.REPORT_CHANNEL;
import static optimize.nodes.cdm.frame.LegacyCNode.buildCDMTemplate;
import static optimize.nodes.ref.FDMRefNodeVDev.buildFDMTemplate;
import static optimize.nodes.ref.HashRefNodeVDev.buildHashTemplate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import optimize.TSTree;
import optimize.nodes.ILeaf;
import optimize.nodes.IMicroNode;
import optimize.nodes.cdm.CLeaf;
import optimize.nodes.cdm.frame.LegacyCNode;
import optimize.nodes.cdm.ICNode;
import optimize.nodes.fdm.FLeaf;
import optimize.nodes.fdm.IFNode;
import optimize.nodes.hash.HNodeV3;
import optimize.nodes.logic.LLeaf;
import optimize.nodes.ref.CDMRefNodeVDev;
import optimize.nodes.ref.FDMRefNodeVDev;
import optimize.nodes.ref.HashRefNodeVDev;
import optimize.traversal.MergedTreeTraversalVDev;
import optimize.util.ByteArray;

public class SuffixMergeVDev {

  static class OccMark {
    IMicroNode firstOcc;
    IMicroNode template;
    IMicroNode fmrParent;
    byte[] fmrParentKey;
    byte[][] foParKeyArr;
    AtomicLong count = new AtomicLong();

    OccMark() {}
  }

  static final Map<ByteArray, OccMark> HASH_TEMPLATES = new HashMap<>();
  static final Map<ByteArray, OccMark> FDM_TEMPLATES = new HashMap<>();
  static final Map<ByteArray, OccMark> CDM_TEMPLATES = new HashMap<>();
  static final Map<ByteArray, OccMark> NOMERGE_TEMPLATES = new HashMap<>();

  public static void reportHashSuffixMerge() {
    REPORT_CHANNEL.append(
        String.format(
            "template num: %d, occ: %d \n",
            HASH_TEMPLATES.size(),
            HASH_TEMPLATES.values().stream().mapToLong(i -> i.count.get()).sum()));
  }

  public static void reportFDMSuffixMerge() {
    REPORT_CHANNEL.append(
        String.format(
            "template num: %d, occ: %d \n",
            FDM_TEMPLATES.size(),
            FDM_TEMPLATES.values().stream().mapToLong(i -> i.count.get()).sum()));
  }

  public static void reportCDMSuffixMerge() {
    REPORT_CHANNEL.append(
        String.format(
            "template num: %d, occ: %d \n",
            CDM_TEMPLATES.size(),
            CDM_TEMPLATES.values().stream().mapToLong(i -> i.count.get()).sum()));
  }

  public static void reportNoMergeSuffixMerge() {
    REPORT_CHANNEL.append(
        String.format(
            "template num: %d, occ: %d \n",
            NOMERGE_TEMPLATES.size(),
            NOMERGE_TEMPLATES.values().stream().mapToLong(i -> i.count.get()).sum()));
  }

  public static void collectSuffixes(TSTree tree, MapType mt, final boolean replace) {
    if (mt == null) {

      return;
    }

    switch (mt) {
      case CDM:
        // todo framework finished, fill in implementation
        MergedTreeTraversalVDev.CDMMergeTraverse(
            null,
            null,
            (ICNode) tree.root,
            null,
            (par, key, cur, stk) -> {
              if (cur instanceof CLeaf) return;
              List<byte[]> kbs = cur.getKeyBytes();
              if (kbs == null || kbs.size() < 2) return;
              byte[][] keys = kbs.toArray(new byte[0][0]);

              // rule: only non-branching child included
              for (byte[] b : keys) {
                if (cur.getChild(b) instanceof LLeaf) continue;

                if (cur.getChild(b) instanceof CLeaf
                    && ((CLeaf) cur.getChild(b)).ptr instanceof LLeaf) continue;
                //             // any single non-LLeaf child will terminate the process
                else return;
              }

              ByteArray tptID = ByteArray.join(keys, (byte) 0);
              if (CDM_TEMPLATES.containsKey(tptID)) {
                OccMark mark = CDM_TEMPLATES.get(tptID);
                if (mark.template == null) {
                  mark.template = buildCDMTemplate(cur);

                  CDMRefNodeVDev crn = new CDMRefNodeVDev();
                  crn.embedTemplate((ICNode) mark.firstOcc, (LegacyCNode) mark.template);
                  if (replace) {
                    mark.fmrParent.replace(mark.fmrParentKey, crn);
                  }
                }
                if (par == null) {
                  throw new RuntimeException("should not be single tree");
                } else {
                  CDMRefNodeVDev frn = new CDMRefNodeVDev();
                  frn.embedTemplate(cur, (LegacyCNode) mark.template);
                  mark.count.incrementAndGet();

                  if (replace) {
                    par.replace(key, frn);
                  }
                }
              } else {
                OccMark mark = new OccMark();
                mark.firstOcc = cur;
                mark.fmrParent = par;
                mark.fmrParentKey = key;
                CDM_TEMPLATES.put(tptID, mark);
              }
            });
        reportCDMSuffixMerge();
        return;
      case FDM:
        MergedTreeTraversalVDev.FDMMergeTraverse(
            null,
            null,
            (IFNode) tree.root,
            (byte) 0,
            (par, key, cur, stk) -> {
              byte[] keys = cur.getKeysFromFDM();
              if (cur instanceof LLeaf || keys == null || keys.length < 2) return;
              for (byte b : keys) {
                if ((cur.get(b) instanceof FLeaf)
                    && (((FLeaf) cur.get(b)).getFValue() instanceof LLeaf)) continue;
                else return;
              }

              ByteArray tptID = ByteArray.join(keys, (byte) 0);
              if (FDM_TEMPLATES.containsKey(tptID)) {
                OccMark mark = FDM_TEMPLATES.get(tptID);
                if (mark.template == null) {
                  mark.template = buildFDMTemplate(cur);

                  // replace the first occ
                  FDMRefNodeVDev frn = new FDMRefNodeVDev();
                  frn.embedTemplate((IFNode) mark.firstOcc, (IFNode) mark.template);

                  if (replace) {
                    mark.fmrParent.replace(mark.fmrParentKey, frn);
                  }
                }

                if (par == null) {
                  throw new RuntimeException("should not be single tree");
                } else {
                  FDMRefNodeVDev frn = new FDMRefNodeVDev();
                  frn.embedTemplate(cur, (IFNode) mark.template);
                  mark.count.incrementAndGet();

                  if (replace) {
                    // if (par.get(key) != cur &&
                    //     ((IFNode) par.get(key)).getFValue() != cur) {
                    //   throw new UnsupportedOperationException();
                    // }
                    par.replace(key, frn);
                  }
                }
              } else {
                OccMark mark = new OccMark();
                mark.firstOcc = cur;
                mark.fmrParent = par;
                mark.fmrParentKey = new byte[1];
                mark.fmrParentKey[0] = key;
                FDM_TEMPLATES.put(tptID, mark);
              }
            });
        reportFDMSuffixMerge();
        return;
      case HASH:
        MergedTreeTraversalVDev.HashMergeTraverse(
            null,
            null,
            (IMicroNode) tree.root,
            null,
            (par, key, cur, stk) -> {
              if (cur.getChildren() == null) return;
              List<IMicroNode> children = cur.getChildren();
              if (children.size() < 2) return;
              for (IMicroNode c : children) {
                if (!(c instanceof ILeaf)) return;
              }

              ByteArray tptID = ByteArray.join(cur.getKeyBytes(), (byte) 0);
              if (HASH_TEMPLATES.containsKey(tptID)) {
                // check if template exists
                OccMark mark = HASH_TEMPLATES.get(tptID);
                if (mark.template == null) {
                  // template not exists, build one
                  mark.template = buildHashTemplate((HNodeV3) cur);
                }
                // already exists, just replace
                if (par == null) {
                  throw new RuntimeException("should not be single tree");
                } else {
                  HashRefNodeVDev hrn = new HashRefNodeVDev();
                  hrn.embedTemplate((HNodeV3) cur, (HNodeV3) mark.template);
                  mark.count.incrementAndGet();
                  // may only traversal to align the cost
                  if (replace) {
                    // todo remove debug print
                    // System.out.println("Before rep: " +
                    // GraphLayout.parseInstance(par).totalSize());
                    par.replace(key, hrn);
                    // System.out.println("After rep: " +
                    // GraphLayout.parseInstance(par).totalSize());
                  }
                }
              } else {
                // mark first occ
                OccMark mark = new OccMark();
                mark.firstOcc = cur;
                HASH_TEMPLATES.put(tptID, mark);
              }
            });
        if (replace) reportHashSuffixMerge();
    }
  }
}
