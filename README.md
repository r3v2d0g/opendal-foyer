# `opendal-foyer`

An `opendal` `Layer` that uses `foyer` to cache reads.

## Missing features

Currently, none of the read and stat capabilities (i.e. `If-Match`, 
`If-None-Match`, etc.) are supported by this crate. We do not plan on adding 
support for these, as our use-case does not require it, but welcome 
contributions to do so.

Similarly, only the `read()` and `stat()` operations are currently integrated
with the cache. We welcome contributions to integrate the other operations.

Finally, the caching layer currently expects files to never change (i.e. the
cache does not get invalidated when modifying a file's content, renaming it, 
etc.).
