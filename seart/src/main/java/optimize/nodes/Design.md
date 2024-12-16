对接口类型设计的需求

# 增加阶段
- 按照 Logical Node 增加，叶子是一种特殊的 Logical Node （Logical Leaf） 持有 long value

# 前缀合并
- 从底至顶替换，每个 Map<String, ...> 替换为多个 micro node
- micro node 可能有多种实现，每种分类成为 map type
  - 注意，每个 map type 下，可能还有多种实现，即 ART 所谓的 typed nodes
- micro node 之间用 byte[] 映射，~~映射的结果应该与自己同属一个 map type~~
  - 注意，映射结果如果考虑可能是 leaf，那就不可能保证同属一个 map type，如此递归泛型无意义
- micro node 还要允许映射到 Logical Leaf
  - **_Q:_** 如果将 MicroNode<?>[] 作为 children，会有什么问题吗

# 后缀合并

# 查询