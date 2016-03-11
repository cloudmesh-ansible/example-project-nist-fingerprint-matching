# NIST Fingerprint Example

This example shows how to deploy the NIST Fingerprint [dataset] (Special Database 4) and [tools] (NBIS) to the cluster.


[dataset]: http://www.nist.gov/srd/nistsd4.cfm
[tools]: http://www.nist.gov/itl/iad/ig/nigos.cfm


# Requirements

- A recent copy of the [Big Data Stack] repository
- A virtual cluster launched and the Ansible `inventory.txt` defined

[Big Data Stack]: https://github.com/futuresystems/big-data-stack

# Using

From the root of the repository 

- `ansible-playbook play-hadoop.yml` to install hadoop to the cluster
- `ansible-playbook addons/spark.yml addons/hbase.yml` to install Spark and HBase
- `ansible-playbook examples/nist_fingerprint/site.yml` to install the datasets and software

Shorthand for the above would be:

```
$ ansible-playbook play-hadoop.yml addons/spark.yml addons/hbase.yml examples/nist_fingerprint/site.yml
```

At this point:

- The database will be on the local filesystem of the frontends in `/tmp/nist`
- The database will be on HDFS under `/nist`
- The NBIS tools will be installed on all the hadoop nodes under `/usr/local`
- Spark and HBase will be available to use
- The example analysis code will be deployed to the `hadoop` user's home directory

Log into the frontend node and switch to the `hadoop` user.
You can run the analysis by:

```
$ sbt package
$ spark-submit \
    --deploy-mode cluster \
    --master yarn \
    --driver-class-path $(hbase classpath) \
    --conf spark.executor.extraClassPath=$(hbase classpath) \
    --class MINDTCT \
    target/scala-2.10/nbis_2.10-1.0.jar
```
