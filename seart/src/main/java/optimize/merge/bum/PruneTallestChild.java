package optimize.merge.bum;

import optimize.merge.skeleton.PartitionInfo;
import optimize.nodes.ITSNode;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

@Deprecated
public class PruneTallestChild extends BottomUpMergeStrategy{

  @Override
  public void mergeAndUpdateInfo(ITSNode node, PartitionInfo info, List<ITSNode> children) {
    Set<Integer> brSet = new TreeSet<>();
    List<PartitionInfo> nonSealedChdInfoLst = new ArrayList<>(), allChdInfoLst = new ArrayList<>();
    for (ITSNode c : children) {
      allChdInfoLst.add(c.getInfoObj());
      if (!c.isLogicalLeaf() && !c.getInfoObj().isMiniRoot) {
        brSet.addAll(c.getInfoObj().brPosSet);
        nonSealedChdInfoLst.add(c.getInfoObj());
      }
    }

    while (brSet.size() > 8) {
      nonSealedChdInfoLst.sort(Comparator.comparingInt(i->-i.brPosSet.size()));
      for (PartitionInfo ci : nonSealedChdInfoLst) {
        if (!ci.isMiniRoot) {
          ci.sealMiniRoot(true, SealMark.tallChild);
          break;
        }
      }
      brSet = mergeConnectedBrPosSet(nonSealedChdInfoLst);
      brSet.add(info.brPos);
    }

    int ttlAcc = 0;
    for (ITSNode c : children) {
      if (c.isLogicalLeaf() || c.getInfoObj().isMiniRoot) {
        ttlAcc++;
      } else {
        ttlAcc += c.getInfoObj().acc;
      }
    }

    if (brSet.size() == 8) {
      // all children had been checked and sealed
      info.hyperLevel = maxChdHyperLevel(allChdInfoLst);
      info.sealMiniRoot(true, SealMark.exact8);
      info.acc = ttlAcc;
      info.brPosSet.addAll(brSet);
    } else {
      if (ttlAcc < 32 || ttlAcc/ brSet.size() < DIVERGE_FACTOR /* check acc with 2/4/8 stages*/) {
        // ready to include more
        info.hyperLevel = maxChdHyperLevel(allChdInfoLst);
        info.isMiniRoot = false;
        info.acc = ttlAcc;
        info.brPosSet.addAll(brSet);
      } else {
        // too short-and-fat
        // todo pick out and seal some fat child and merge others
        int maxHyperLevel = maxChdHyperLevel(nonSealedChdInfoLst);
        children.forEach(n -> {
          if (!n.isLogicalLeaf()) {
            n.getInfoObj().sealMiniRoot(true, SealMark.roughSeal);
          }
        });
        info.hyperLevel = maxHyperLevel + 1;
        info.acc = children.size();
        info.isMiniRoot = false;
        info.sealReason = String.format("Before seal all children acc: %d", ttlAcc);
      }
    }
  }
}
