=======
Prequel
=======

Overview
========

Prequel is a library for developing custom persistent storage systems on secondary storage (i.e. on disk).
It comes with a few useful abstractions and data structures to make programming in that environment
nearly as simple as developing data structures for main memory. Existing datastructures (and possibly
user-written ones) can be combined to create simple or powerful storage solutions, like file formats, file systems
or databases.

Please note that this is a work in progress that is **NOT** suitable production use.

Features
========

I/O Management
--------------
- Buffer management (reading, pinning and dirtying of file blocks and so on) through implementations of the *engine* interface.

  - *file_engine* uses a fixed size cache (LRU) to cache file blocks in memory.
  - *mmap_engine* uses mmap (on Linux, Windows is not yet implemented) to access file blocks. It does not have its own
    cache because it can rely on the virtual memory system of its host OS.
  - *transaction_engine* is like the file engine, but comes with a write ahead journal to implement transactions.
  - The interface is very general, so the user can provide her own engine implementations.
- Simple extensible VFS layer for platform specific I/O (read, write, truncate etc.).
  Currently supported: windows, unix. Custom implementations can be provided by the user by implementing the vfs interface.

Binary serialization
--------------------

Structs can be serialized to a well defined format:

- All integers are encoded as big endian
- The order of fields in a serialized struct is well defined
- Nesting of types is supported, as is the use of some standard classes
  like *std::optional<T>*, *std::variant<Ts...>* and *std::tuple<T>*, provided that
  all involved types are themselves serializable.

Together, these properties make it easy to guarantee a platform-independent file format.

Data structures
---------------

Prequel comes with a collection of simple data structures idented for the use in secondary storage.
Data structures are written uses the *engine* interface, so they will keep functioning with a custom
implementation of that interface.

Included data structures:

- *allocator*. Implements block extent allocation (like malloc/realloc/free).
- *array*. A dynamic array (either with exponential or linear growth) with contiguous storage.
- *btree*. An ordered index type for values with unique keys. Can be used to implement map- or set-like indices.
- *extent*. A range of contiguous blocks in secondary storage. Can be resized (but possibly moves in that case).
- *hash_table*. An index type for for values with unique keys. Uses linear hashing to grow the table, so
  it will not produce long pause times for rehashing.
- *heap*. An unordered heap file of variable-sized records. Objects can be allocated and freed and are referred to
  using heap references (~~ pointers).
- *list*. An ordered linked list of blocks. Supports oredered iteration and fast insertion at both ends.
- *stack*. A stack of values (LIFO).

License
=======

MIT. See *LICENSE* file.
