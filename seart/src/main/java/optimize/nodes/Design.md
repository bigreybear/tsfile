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

关于 micro leaf 和 orphan leaf：

什么是 orphan leaf
：某种 micro leaf 且 pk == null，属于完全无用的存在，仅仅是某种标识作用，因而可考虑优化

不用 orphan leaf 如何标识分隔符点号
：用 search key 的进度位置考虑，但是调整 setFinished 的时机。
对于 CNode 和 CNode4 并不是在每次搜索后，通过判断 curLen == key.len 来判断，而是
1）在 CLeaf 中匹配 pk 后设置；
2）进入 CNode/4 时，如果 curLen == key.len，那么就在当前节点搜索 empty key，并设置。如果存在，则返回对应 ptr，否则返回自身。
这种做法实际依仗的是，不允许任何 segment 存在空键，因此如果有 empty key，而当前 search key 未处理完，则一定应该读取。
这种做法对于 prefixed key 会增加一次循环，但实际并不增加对象读取；相反，如果允许 orphan leaf，那么在更多场景下可能增加节点。

# 后缀合并

# 查询