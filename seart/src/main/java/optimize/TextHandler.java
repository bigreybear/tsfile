package optimize;

import loader.PathTxtLoader;
import seart.utils.PathUtils;

import java.util.Collections;
import java.util.List;

public class TextHandler {

  public static void main(String[] args)throws Exception{
    PathTxtLoader loader = new PathTxtLoader(PathTxtLoader.FILE_PATH);
    List<String> ori = loader.getAllLines();
    Collections.shuffle(ori);
    List<String> randomSample = ori.subList(0, 5000);
    PathUtils.dumpStringCollection("mtreedata/baowu_query.txt", randomSample);
  }
}
