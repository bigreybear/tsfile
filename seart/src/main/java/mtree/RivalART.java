package mtree;

import ankur.art.ArtNode;
import ankur.art.ArtTree;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class RivalART {

  public static void main(String[] args) throws Exception{
    PathTxtLoader loader = new PathTxtLoader(PathTxtLoader.FILE_PATH);
    ArtTree tree = new ArtTree();

    List<String> bat;
    String rp;
    int cnt = 0;
    while (!(bat = loader.getLines()).isEmpty()) {
      for (String p : bat) {
        rp = PathTxtLoader.removeBacktick(p);
        tree.insert(rp.getBytes(StandardCharsets.UTF_8), (long) cnt);
        cnt++;
      }
    }


    ArtTree.calculateDepth(tree);
    //    System.out.println(tree);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ArtTree.serialize(((ArtNode) tree.root), baos);
    ReadWriteIOUtils.write(tree.root.offset, baos);
    System.out.println("baos.size(): " + baos.size());
    byte[] res = baos.toByteArray();
    ArtTree.traverse((ArtNode) tree.root, "");
    System.out.println("tree.totalNodes: " + tree.totalNodes());
    System.out.println("tree.totalDepth:" + tree.totalDepth());

    ArtNode r2 = (ArtNode) ArtTree.deserialize(ByteBuffer.wrap(res));
    ArtTree.traverseAfterDeserialize(r2, "");
    tree.root = r2;
    System.out.println(tree.totalNodes());
    System.out.println(tree.totalDepth());
    tree.collectStatistics();
    System.out.println("AAA");
  }
}
