sacker is a simple cloud blob manager

concepts
--------

sacker associates a package **name** and a monotonically increasing package
**version** to a **sha**-addressable blob of data stored somewhere.
furthermore, sacker can label specific package versions with a **tag**.
there is one reserved tag named "latest" which resolves to the latest
package version.

sacker uses a **ledger** to store the associations between package names,
package tags, package versions and their corresponding shas.  suitable
mechanisms for ledger implementations are those that provide semi-strong or
strong consistency guarantees such as zookeeper, gitdb or dynamo.

sacker uses a **store** to store blobs of data, addressable only by the sha of
the blob of data.  suitable backends for sacker stores are data warehouses
like hdfs or object stores like s3.

sacker comes with both a ledger and store based on s3.  the s3 ledger
is compatible with write-only clients and thus may be a suitable
ledger if you want to push package artifacts from a third-party CI
provider.


query operations
----------------

    sacker list                      : list all packages known to the ledger
    sacker versions <package>        : list package versions
    sacker tags     <package>        : list package tags
    sacker info     <package> <spec> : print information about package at <spec>

**spec** can either be a version number ("7", "23") or a tag name (e.g.
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


**canary workflow using "live" and "latest"**

A common pattern is to continuously deploy the package pointed to by
"latest" into canary/staging.  Uploading a file using `sacker add
frontend-server build/frontend-server.jar` will upload the new version of
`frontend-server` and return its version number, e.g.  23.  A cron job doing
`sacker download frontend-server latest` will pick up this version.

Once a particular version has been stable in staging/canary for sufficiently
long, it can be graduated to "live" by tagging it `sacker tag frontend-server 23 live`.
Production servers can either periodically `sacker download frontend-server live`
or a deployment against this version can be initiated.


example s3 ledger/store iam policy for ci
-----------------------------------------

a suitable s3 sacker iam policy would be for write-only access to the store
and write-only access to a specific set of keys.  see
`examples/ci-aws-iam-policy.json` for an example of such a policy.
