Filehandle Reference Counting Design
====================================

We need some way to manage open file handles --  we want to

1. Not open multiple file handles to the same file
2. Not hold unneeded file handles
3. Not close file handles that might be needed "soon" (since Xrootd can take
   many 10s of seconds to open files in the pathlogical case)

Problem
-------

There are a couple of constraints that make this difficult.

Firstly, Spark opens and closes partitions in temporally sequential order
similar to

1. Open partition A
2. Close partition A
3. Open partition B
4. Close partition B

However, there's no hints that Spark passes down to the data source to say what
partitions will be arriving in the future, so a naiive implementation would
close the file after step 2, then immediately have to reopen it in step 3. This
will violate 1&3.

Secondly, it's not only the top-level `TFile` interface that holds os-level
file handles, `TBaskets` also hold onto their file handles so they can fufill
the callbacks that get the raw basket bytes. We need to be careful to not close
files that have these additional references, to keep from pulling the rug out
from those users.

Additionally, Java's `finalize` interface doesn't match what we need and isn't
suitable for doing "close the filehandle when the last reference goes out of
scope".

Finally, the executor code needs to be multithreaded, since multiple tasks can
be running within the JVM.

Current design
--------------

The current file reading/handling architechture is layered like so:

```
            |TFile| --\                                   /-|HadoopFile|
    |FileBackedBuf| ------ |ROOTFile| - |FileInterface| -<
|TBasket| --/                                             \-|NIOFile|
 |Cursor| -/
```

* `FileInterface` is a very simple interface which provides `read()` and
  `close()`. `HadoopFile` and `NIOFile` hold the OS-level file handles.
* `ROOTFile` wraps `FileInterface` objects and provides methods to produce
  both `FileBackedBuf` and `Cursor` objects from a file. 
* `FileBackedBuf` provides caching and returns Java NIO `ByteBuffer` objects
  from the file contents.
* `Cursor` is a generalized "file pointer", which tracks an offset, and lets
  higher-level application code read out ints/floats/arrays. It performs reads
  from classes that implement `BackingBuf` (which includes `FileBackedBuf`).
* `TFile` is the "ROOT" view of a file, it decodes and tracks the streamers
  as well as the `TDirectory`
* `TBasket` is the "ROOT" view of a basket. It holds a reference to a `Cursor`
  which is backed by a `FileBackedBuf` which points to the payload bytes in the
  input file.

Currently, all upper-level code accesses files through `ROOTFile` who is the
only client of `FileInterface` objects. We can use this to implement safe
handling of file handles.

Solution
--------

To try and fulfill the different requirements and constraints, we keep some
different caches.

* `HashMap<String, WeakRef<ROOTFile>>` - Uniquely maps a path to a `ROOTFile`
* `TimedCache<String, FileInterface>` - Stores "orphaned" `FileInterface`
  objects, i.e. `FileInterface` objects who don't have a corresponding
  `ROOTFile` object. Since `FileInterface`s are only accessed via `ROOTFile`,
  we can be sure it is safe to close these objects.

Tying these two together is a [PhantomReference](https://docs.oracle.com/javase/8/docs/api/java/lang/ref/PhantomReference.html)
attached to the `ROOTFile`. We subclass the phantom reference to hold a
reference to the underlying `FileInterface`. Once every other reference to
a particular `ROOTFile` disappears, the phantom reference sends an event to the
associated reference queue. Since we know we only have a single `ROOTFile` for
a given path, that `ROOTFile` is the only thing holding a reference to the
underlying `FileInterface`. We can then use the phantom reference functionality
to add the `FileInterface` for to the TimedCache. If a `ROOTFile` is created
before the timer expires, it will remove the `FileInterface` from the
TimedCache. Otherwise, the destructor in TimedCache will call the `close()`
functionality in `FileInterface`. Once every other reference to
a particular `ROOTFile` disappears, the phantom reference sends an event to the
associated reference queue. Since we know we only have a single `ROOTFile` for
a given path, that `ROOTFile` is the only thing holding a reference to the
underlying `FileInterface`. We can then use the phantom reference functionality
to add the `FileInterface` for to the TimedCache. If a `ROOTFile` is created
before the timer expires, it will remove the `FileInterface` from the
TimedCache. Otherwise, the destructor in TimedCache will call the `close()`
functionality in `FileInterface`.

### State machine

Each file path can be in one of the following states within a JVM

1. Unassigned - No references to the path exist
2. Open - `ROOTFile` and `FileInterfaces` objects referencing the path exist
3. PhantomOpen - `ROOTFile` is [phantom-reachable](https://www.logicbig.com/tutorials/core-java-tutorial/gc/phantom-reference.html)
   and has reached the top of the accompanying [ReferenceQueue](https://docs.oracle.com/javase/8/docs/api/java/lang/ref/ReferenceQueue.html)
4. TimedClose - The `ROOTFile` is unreachable, and the `FileInterface` is
   available to be closed after some timeout.

Valid transitions are

1. Unassigned - Open
2. Open - PhantomOpen
3. PhantomOpen - TimedClose, Open
4. TimedClose - Unassigned, Open

### Concurrency

Serialize the whole RefCount object instead of doing something more fine
grained.

### Background tasks

Timing out the cache and checking the phantom reference queue has to happen
periodically. Since the timing isn't fully important, don't bother with a
background thread. Just sprinkle in the checks in places in the code.
