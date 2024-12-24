package optimize;

import optimize.nodes.IMicroNode;
import optimize.nodes.cdm.ICNode;
import optimize.nodes.fdm.IFNode;

// to carry multiple status amid searching
public final class SearchStatus {
  // progress on the byte array now being searched
  int curLen;
  ICNode icNode;
  IFNode ifNode;
  IMicroNode imNode;
  boolean isFinished;

  public SearchStatus reset() {
    curLen = 0;
    icNode = null;
    ifNode = null;
    imNode = null;
    return this;
  }

  public int getCurLen() {
    return curLen;
  }

  public SearchStatus setCurLen(int curLen) {
    this.curLen = curLen;
    return this;
  }

  public ICNode getIcNode() {
    return icNode;
  }

  public SearchStatus setIcNode(ICNode icNode) {
    this.icNode = icNode;
    return this;
  }

  public IFNode getIfNode() {
    return ifNode;
  }

  public SearchStatus setIfNode(IFNode ifNode) {
    this.ifNode = ifNode;
    return this;
  }

  public IMicroNode getImNode() {
    return imNode;
  }

  public SearchStatus setImNode(IMicroNode imNode) {
    this.imNode = imNode;
    return this;
  }

  public boolean isFinished() {
    return isFinished;
  }

  public SearchStatus setFinished(boolean finished) {
    isFinished = finished;
    return this;
  }
}
