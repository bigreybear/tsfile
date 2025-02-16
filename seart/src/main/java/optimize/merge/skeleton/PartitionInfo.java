package optimize.merge.skeleton;

import optimize.merge.bum.SealMark;
import optimize.nodes.ITSNode;
import optimize.nodes.fdm.IFNode;

import java.util.Set;
import java.util.TreeSet;

public class PartitionInfo {
  // these two shall not be changed
  public int dep = 0;
  public int brPos = 0; // the position the branch incurs

  // could be updated if sealed
  public boolean isMiniRoot = false;
  public int acc = 0;
  public int hyperLevel = 0;
  public Set<Integer> brPosSet = new TreeSet<>();

  // indicating can only be attached to IFNodes
  private final IFNode host;

  public PartitionInfo(IFNode _host) {
    host = _host;
  }

  public byte[] getKeys() {return host.getKeysFromFDM();}
  public ITSNode getChd(byte k) {return host.get(k);}

  @Deprecated
  public String sealReason = "";
  public SealMark mark;
  public byte[] fullKey;

  public void sealMiniRoot(boolean f, SealMark m) {isMiniRoot = f; mark = m;}

  @Override
  public String toString() {
    return String.format("hypLvl: %d, acc: %d, brPos: %d, brPosNum: %d (%s) isMiniRoot: %b",
        hyperLevel, acc, brPos, brPosSet.size(), brPosSet, isMiniRoot);
  }
}
