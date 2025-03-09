package optimize.nodes.cdm.one;

import optimize.SearchStatus;
import optimize.merge.MapType;
import optimize.merge.PrefixMergeStrategy;
import optimize.merge.skeleton.MiniTreeRep;
import optimize.nodes.IMicroNode;
import optimize.nodes.NodeInspector;
import optimize.nodes.NodeWithPartialKey;
import optimize.nodes.cdm.ICNode;
import optimize.util.ByteArray;
import optimize.util.InfixGroup;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import static optimize.merge.CDMPrefixMerge.recMergeCDM;
import static optimize.nodes.cdm.frame.CNodeBase.NO_ORPHAN_CLEAF;

public abstract class CNodeOneBase extends NodeWithPartialKey implements ICNode {
  // public int pos = -1;  // added for transformation from MiniTreeRep Note(zx) why this?
  ICNode[] ptrs;

  abstract void initByMap(Map<ByteArray, ICNode> m);

  public static ICNode buildNode(byte[] parKey, Map<ByteArray, ICNode> m) {
    // build for greed merge
    CNodeOneBase r;
    if (m.size() < 17) {
      r = new CNode1FBS();
    } else if (m.size() < 192) {
      // align with FNode48
      r = new CNode1F48();
    } else {
      r = new CNode1F256();
    }
    r.initByMap(m);
    r.setParKey(parKey);
    return r;
  }

  @Override
  public ICNode[] getPtrArr() {
    return ptrs;
  }

  @Override
  public List<IMicroNode> getChildren() {
    return Arrays.stream(ptrs).filter(Objects::nonNull).collect(Collectors.toList());
  }

  abstract protected void compactInit(byte[] sbk);
  abstract protected void setPointer(byte[] sbk, int idx, ICNode c);

  // fixme Note(zx) a design diverge: where to set the remaining key after last branch?
  //  By CNode2/4/8 it is appended to the rmk. but CNode1X has no rmk.
  //  If CNode1X puts it into LLeaf, it requires careful handle.
  @Override
  public void setContent(InfixGroup group,
                         Function<byte[], IMicroNode> getLChild,
                         MapType mapType,
                         PrefixMergeStrategy mergeStrategy,
                         int height) {
    int[] posArr = group.getBranchingPos();
    if (posArr.length > 1)
      throw new UnsupportedOperationException("Invalid branch pos for CNode1");
    byte[] sortedBrKeys = group.sortedByteBranchKeys();
    compactInit(sortedBrKeys);
    setParKey(group.getCommonPrefix());
    List<byte[]> completeKeys;
    int sbkSize = group.countBranches();
    int pos = posArr[0];
    byte[] firstCompKey;
    for (int i = 0; i < sbkSize; i++) {
      completeKeys = group.getCompleteKeys(new byte[] {sortedBrKeys[i]});

      if (!NO_ORPHAN_CLEAF) {
        throw new RuntimeException("CNode1FX cannot co-exist with orphan/trivial CLeaf.");
      }

      // if only one key, set the ptr and push down par key
      if (completeKeys.size() == 1) {
        firstCompKey = completeKeys.get(0);
        IMicroNode c = getLChild.apply(firstCompKey);
        if (pos + 1 < firstCompKey.length)
          c.setParKey(Arrays.copyOfRange(firstCompKey, pos + 1, firstCompKey.length));
        setPointer(sortedBrKeys, i, (ICNode) c);
        continue;
      }

      // otherwise merge on going
      setPointer(
          sortedBrKeys,
          i,
          (ICNode) recMergeCDM(
              getLChild,
              completeKeys,
              pos + 1,
              mapType,
              mergeStrategy,
              height)
      );
    }
  }

  abstract protected ICNode getPointer(byte b);

  @Override
  public ICNode proceedQueryCDM(byte[] key, SearchStatus sts) {
    if (sts.getCurLen() == key.length) {
      ICNode ptr = getPointer((byte) 0);
      sts.setFinished(true);
      return ptr == null ? this : ptr;
    }

    int curLen = checkPartialKey(key, sts.getCurLen());
    if (curLen == key.length) {
      sts.setFinished(true);
      return getPointer((byte)0);
    }

    sts.setCurLen(curLen + 1);
    return getPointer(key[curLen]);
  }

  abstract protected String codeName();

  @Override
  public void acceptInspector(NodeInspector noi) {
    noi.appendEntry("ptr_num_" + codeName(), ptrs.length);
  }

  @Override
  public int[] getBranchingPos() {
    throw new UnsupportedOperationException();
  }


  // region For MiniTreeRep

  @Override
  public void fillContent(MiniTreeRep rep, int[] posArr) {

  }


  // endregion
}
