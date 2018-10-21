B-Tree
======

B-Tree Reference
----------------

``#include <prequel/btree.hpp>``
 
.. doxygenclass:: prequel::btree
    :members:


Raw B-Tree Reference
--------------------

``#include prequel/raw_btree.hpp``

This class provides the untyped implementation for :cpp:class:`prequel::btree`.
You can use it directly if you need more control than the typed B-Tree can provide.
One use case are runtime-sized structures stored within a tree (e.g. configured through
user input) - these cannot be mapped to standard C++ types.

.. doxygenclass:: prequel::raw_btree
    :members:
