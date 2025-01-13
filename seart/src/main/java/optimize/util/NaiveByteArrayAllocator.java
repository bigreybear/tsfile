package optimize.util;

// @Authored by CoPilot
public class NaiveByteArrayAllocator {
  // 底层的字节数组，作为“总内存池”
  private final byte[] buffer;

  // 用于记录空闲块信息，这里使用一个简单单链表来示意
  // 每个节点保存当前空闲块的起始位置（start）、长度（length），以及指向下一个空闲块的引用
  private static class FreeBlock {
    int start;
    int length;
    FreeBlock next;

    FreeBlock(int start, int length, FreeBlock next) {
      this.start = start;
      this.length = length;
      this.next = next;
    }
  }

  // 指向空闲链表的头部，初始化时整个数组都是空闲的
  private FreeBlock freeListHead;

  public NaiveByteArrayAllocator(int size) {
    this.buffer = new byte[size];
    // 初始只有一个空闲块，从 0 开始到 size
    this.freeListHead = new FreeBlock(0, size, null);
  }

  /** 从分配器中申请 length 大小的空间，返回在 buffer 中的起始 offset。 如果分配失败，返回 -1。 */
  public int allocate(int length) {
    FreeBlock prev = null;
    FreeBlock current = freeListHead;

    while (current != null) {
      // 找到一个能放下 length 大小的空闲块
      if (current.length >= length) {
        // 记录当前块的起始地址
        int allocatedStart = current.start;
        // 更新当前块的起始地址和剩余长度
        current.start += length;
        current.length -= length;

        // 如果当前块已经被完全用完，需要从链表中移除
        if (current.length == 0) {
          // 需要将当前块移除
          if (prev == null) {
            freeListHead = current.next;
          } else {
            prev.next = current.next;
          }
        }

        return allocatedStart;
      }

      prev = current;
      current = current.next;
    }

    // 没有足够大的空闲块，返回 -1
    return -1;
  }

  /** 将之前 allocate 的某块空间（从 offset 开始，长度为 length）归还到分配器管理的空闲链表中。 */
  public void free(int offset, int length) {
    // 先根据 offset 将这个空闲块插回链表
    if (freeListHead == null) {
      // 如果之前没有空闲块，就直接创建一个
      freeListHead = new FreeBlock(offset, length, null);
      return;
    }

    // 找到正确的插入位置，保证链表按 start 大小升序排列
    FreeBlock prev = null;
    FreeBlock current = freeListHead;
    while (current != null && current.start < offset) {
      prev = current;
      current = current.next;
    }

    // 插入位置就是 prev -> (newBlock) -> current
    FreeBlock newBlock = new FreeBlock(offset, length, current);
    if (prev == null) {
      freeListHead = newBlock;
    } else {
      prev.next = newBlock;
    }

    // 合并相邻空闲块，避免碎片化
    mergeAdjacentBlocks(prev, newBlock, current);
  }

  private void mergeAdjacentBlocks(FreeBlock left, FreeBlock mid, FreeBlock right) {
    // 如果与左侧相邻
    if (left != null && (left.start + left.length == mid.start)) {
      left.length += mid.length;
      left.next = mid.next;
      mid = left; // 让 mid 指针复用一下，以便下面可能和 right 合并
    }

    // 如果与右侧相邻
    if (right != null && (mid.start + mid.length == right.start)) {
      mid.length += right.length;
      mid.next = right.next;
    }
  }

  /** 向分配得到的地址写入数据。这里只是一个简易示例。 */
  public void putData(int offset, byte[] data) {
    if (offset < 0 || offset + data.length > buffer.length) {
      throw new IndexOutOfBoundsException("写入超出分配器范围");
    }
    System.arraycopy(data, 0, buffer, offset, data.length);
  }

  /** 从指定 offset 读取 length 长度的数据 */
  public byte[] getData(int offset, int length) {
    if (offset < 0 || offset + length > buffer.length) {
      throw new IndexOutOfBoundsException("读取超出分配器范围");
    }
    byte[] result = new byte[length];
    System.arraycopy(buffer, offset, result, 0, length);
    return result;
  }

  public int totalSize() {
    return buffer.length;
  }

  public byte[] getBackingArray() {
    return buffer;
  }
}
