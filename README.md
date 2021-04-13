# misc
Miscellaneous scripts, utils, etc.

Elasticsearch INdex dumper 
This was written to provide a quick way of dumping an entire index and mappings to a JSON file.
It was written as (at the time), Elasticsearch didn't have a way to dump an index running across multiples nodes in a cluster to a single node (without using an NFS mount).
This simple program allowed this - it was not intended for use across huge datasets!
Caveat : It  may not work with latest versions of Elasticsearch
