package optimize;

import java.util.Collections;
import java.util.List;
import loader.PathTxtLoader;
import seart.utils.PathUtils;

public class TextSampler {

  public static void main(String[] args) throws Exception {
    MyDataSet ds = MyDataSet.TSBS;
    PathTxtLoader loader = new PathTxtLoader(ds.rfile /* source file */);
    List<String> ori = loader.getAllLines();
    Collections.shuffle(ori);
    List<String> randomSample = ori.subList(0, 5000);
    PathUtils.dumpStringCollection(ds.qfile, randomSample);
  }
}
