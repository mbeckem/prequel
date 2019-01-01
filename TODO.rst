Open Tasks
==========

Design
------
- Better programming model for anchor creation/destruction, for example static create(...) destroy(...) fn.
- Replace c-style callbacks with std::function ones.
- Rethink some API design decisions that dont make much sense in modern C++,
  for example functions returning bool and taking an output reference.

Block manager (engine)
----------------------
- Async prefetch for block reads. Engine should have function for reading that returns a future.
  Real I/O should be done in a worker thread.

Data structures
---------------
- Implement Queue and Priority Queue

- b+Tree should be able to handle non-unique keys (e.g. through overflow pages at the leaf level)

- A variant of the b+tree should support variable size entries (up to a maximum size to guarantee some fanout),

  for example "any size but no more than capacity/4". The heap or custom overflow pages can be used to place big values out-of-line.

- The b+tree or a variant of it should be able to handle augmentations (additional information about children that
  are stored together with the child pointers in the internal nodes).

  Possible applications:
  - Ordered sequence (without an index key!), each node's augmentation is the number of entries in its subtree.
    Can be used to implement dynamic arrays with log(n) access time (and better add/remove performance than O(N)).
    Can also be used to implement a chunk tree (for example: file storage) that makes a collection of chunks
    appear linear.
  - Some maximum value (e.g. max possible allocation size) in a subtree. Can be used to implement allocators
    that index pages together with their free space.

- Integrity checking support. Currently, every datastructure has complete control over the contents
  of its own data blocks. This prevents applications from inserting custom metadata into those blocks,
  for example to check for unexpected corruption (magic numbers, checksums, owners, etc.)

Other
-----
- Review examples (some places are more complex than needed)
