package optimize.nodes.cdm;

import optimize.SearchStatus;
import optimize.merge.PrefixMergeStrategy;
import optimize.nodes.IMicroNode;
import optimize.nodes.NodeWithPartialKey;
import optimize.util.InfixGroup;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static optimize.merge.CDMPrefixMerge.recMergeCDM;
import static optimize.nodes.cdm.CNodeBase.NO_ORPHAN_CLEAF;

public abstract class CNodeOneBase extends NodeWithPartialKey implements ICNode{


  abstract protected void setPointer(byte b, ICNode c);

  // fixme Note(zx) a design diverge: where to set the remaining key after last branch?
  //  By CNode2/4/8 it is appended to the rmk. but CNode1X has no rmk.
  //  If CNode1X puts it into LLeaf, it requires careful handle.
  @Override
  public void setContent(InfixGroup group,
                         Function<byte[], IMicroNode> getLChild,
                         PrefixMergeStrategy mergeStrategy,
                         int height) {
    List<byte[]> completeKeys;
    int[] posArr;
    if ((posArr = group.getBranchingPos()).length > 1)
      throw new UnsupportedOperationException("Invalid branch pos for CNode1");
    int sbkSize = group.countBranches();
    int pos = posArr[0];
    byte[] sortedBrKeys = group.sortedByteBranchKeys();
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
        if (pos + 1 != firstCompKey.length)
          c.setParKey(Arrays.copyOfRange(firstCompKey, pos + 1, firstCompKey.length));
        setPointer(sortedBrKeys[i], (ICNode) c);
        continue;
      }

      // otherwise merge on going
      setPointer(
          sortedBrKeys[i],
          (ICNode) recMergeCDM(
              getLChild,
              completeKeys.toArray(new byte[0][0]),
              pos + 1,
              mergeStrategy,
              height
          )
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

    int curLen = checkPartialKey(key, sts.getCurLen(), sts.getCurLen());
    if (curLen == key.length) {
      sts.setFinished(true);
      return getPointer((byte)0);
    }

    sts.setCurLen(curLen + 1);
    return getPointer(key[curLen]);
  }
}
