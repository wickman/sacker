sacker is a simple cloud blob manager

concepts
--------

sacker associates a package *name* and an autoincrementing package *version*
to a *sha*-addressable blob of data stored somewhere.  furthermore, sacker
can label specific package versions with a *tag*.  there is one reserved
tag named "latest" which resolves to the latest package version.

sacker uses a *ledger* to store the associations between package names,
package tags, package versions and their corresponding shas.  suitable
mechanisms for ledger implementations are those that provide semi-strong or
strong consistency guarantees such as zookeeper, gitdb or dynamo.

sacker uses a *store* to store blobs of data, addressable only by the sha of
the blob of data.  suitable backends for sacker stores are data warehouses
like hdfs or object stores like s3.

sacker comes with both a ledger and store based on s3.  the s3 ledger is
weakly consistent and should only be used for testing purposes.


query operations
----------------

    sacker list                      : list all packages known to the ledger
    sacker versions <package>        : list package versions
    sacker tags     <package>        : list package tags
    sacker info     <package> <spec> : print information about package at <spec>

*spec* can either be a version number ("7", "23") or a tag name (e.g.
"live", "devel", "latest".)


file operations
---------------

    sacker add      <package> <filename> : add package and autoincrement latest version
    sacker download <package> <spec>     : download package at <spec>


tagging operations
------------------

    sacker tag      <package> <version> <tag> : assign a tag to package at <version>
    sacker untag    <package> <tag>           : remove tag from package


configuring
-----------

the ledger and store can be configured using the file ~/.sacker.json with
two keys "ledger" and "store", e.g.

    {
      "ledger": "s3://<storage-bucket-name>",
      "store": "s3://<ledger-bucket-name>"
    }

this filename can also be overridden with the $SACKER_CONFIG environment variable.  access
to s3 is performed with boto and honors standard AWS_* environment variables.


example workflows
-----------------


development using "live" and "latest"
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

things.

stuff. this is cool. does this even work?
