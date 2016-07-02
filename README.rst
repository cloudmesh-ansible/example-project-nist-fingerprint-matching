
##########################
 NIST Fingerprint Example
##########################

This example shows how to deploy the NIST Fingerprint dataset_ (Special Database 4) and tools_ (NBIS) to the cluster.


.. _dataset: http://www.nist.gov/srd/nistsd4.cfm
.. _tools: http://www.nist.gov/itl/iad/ig/nigos.cfm


########
 Status
########

Work-in-progress


********
Overview
********

==============
 Introduction
==============

Fingerprint recognition refers to the automated method for verifying a match between two fingerprints and that is used to identify individuals and verify their identity.
Fingerprints (Figure 1) are the most widely used form of biometric used to identify individuals.

.. figure:: images/fingerprints.png
   :alt: Example fingerprints
   :align: center

   **Figure 1** Example fingerprints



The automated fingerprint matching generally required the detection of different fingerprint features (aggregate characteristics of ridges, and minutia points) and then the use of fingerprint matching algorithm, which can do both one-to- one and one-to- many matching operations.
Based on the number of matches a proximity score (distance or similarity) can be calculated.


============
 Algorithms
============

For this work we will use the following algorithms:

- MINDTCT: The NIST minutiae detector, which automatically locates and records ridge ending and bifurcations in a fingerprint image. (http://www.nist.gov/itl/iad/ig/nbis.cfm)
- BOZORTH3: A NIST fingerprint matching algorithm, which is a minutiae based fingerprint-matching algorithm. It can do both one-to- one and one-to- many matching operations. (http://www.nist.gov/itl/iad/ig/nbis.cfm)

==========
 Datasets
==========

We use the following NIST dataset for the study:

- Special Database 14 - NIST Mated Fingerprint Card Pairs 2. (http://www.nist.gov/itl/iad/ig/special_dbases.cfm)


====================
 Specific Questions
====================

#. Match the fingerprint images from a probe set to a gallery set and report a match scores?
#. What is the most efficient and high-throughput way to match fingerprint images from a probe set to a large fingerprint gallery set?


===================
 Development Tools
===================

- Apache Hadoop (with YARN)
- Apache Spark
- Apache HBase
- Apache Hive
- Scala


========
 Method
========

#. Launch a virtual cluster
#. Deploy the stack

   #. `Big Data Stack`_
   #. `deploy.yml`_

#. Prepare the dataset -- `dataset.yml`_

   #. adds dataset to HBase -- scala, spark, hbase
   #. partitions dataset into "probe" and "gallery"

#. Run the analysis -- `analysis.yml`_, scala, spark, hbase

   #. Load the probe sete
   #. Load the gallery set
   #. Compare each image in "probe" to "gallery"
   #. Store results in HBase

#. Use Hive to query


.. _deploy.yml: deploy.yml
.. _dataset.yml: dataset.yml
.. _analysis.yml: analysis.yml
.. _Big Data Stack: https://github.com/futuresystems/big-data-stack


###############
 Prerequisites
###############

#. Python 2.7
#. Pip
#. Virtualenv
#. Git
#. Cloud provider


############
Installation
############

#. Clone this repository::

     git clone --recursive git@github.com:cloudmesh/example-project-nist-fingerprint-matching

#. Create a virtualenv and activate it::

     virtualenv venv
     source venv/bin/activate

#. Install the requirements::

     pip install -r big-data-stack/requirements.txt


#######
 Using
#######

#. Launch a virtual cluster. Make sure the Ansible inventory file matches the specifications for the Big Data Stack.

#. Deploy Hadoop, Spark, HBase and Hive::

     cd big-data-stack
     ansible-playbook -i <path/to/inventory.txt> play-hadoop.yml \
       addons/spark.yml addons/hbase.yml addons/hive.yml
     cd -

#. Deploy the software::

     ansible-playbook -i <path/to/inventory.txt> software.yml

#. Deploy and partition the dataset::

     ansible-playbook -i <path/to/inventory.txt> dataset.yml

#. Run the analysis::

     ssh hadoop@frontend
     ./analysis.sh

#. Use Hive to query



####################
 Build Instructions
####################

These instructions are for manually building and bundling the source
code for loading the images into HBase and running the analysis::

  $ sbt package
  $ sbt assembly


##################
Running with Spark
##################

After building, the target jarfile to submit is located at::

  target/scala-2.10/NBIS-assembly-1.0.jar


When submitting, you need to tell Spark to provide HBase in the execution classpath using::

  --driver-class-path $(hbase classpath)

************
 Components
************

There are two components:

#. Loading the image data into HBase
#. Comparing a probe set to the gallery


In the command below the ``$MAIN_CLASS`` and ``$MAIN_CLASS_ARGS`` configure which component two run.
The possible configurations are

- ``MAIN_CLASS=LoadData``

  This runs the component that loads the data from local filesystem
  into HBase.  It require one argument: the path to the checksum file
  from which the list of images and their metadata files is extracted. For example::

    MAIN_CLASS_ARGS=/tmp/nist/NISTSpecialDatabase4GrayScaleImagesofFIGS/sd04/sd04_md5.lst

   
********************************
 Local Submission (for testing)
********************************

::

   spark-submit \
     --driver-class-path $(hbase classpath) \
     --class $MAIN_CLASS \
     target/scala-2.10/NBIS-assembly-1.0.jar \
     $MAIN_CLASS_ARGS

*************************************
 Cluster Submission (for production)
*************************************

This is the same as the local submission, but add::

  --master yarn --deploy-mode cluster


############
 DEPRECATED
############

At this point:

- The database will be on the local filesystem of the frontends in ``/tmp/nist``
- The database will be on HDFS under ``/nist``
- The NBIS tools will be installed on all the hadoop nodes under ``/usr/local``
- Spark and HBase will be available to use
- The example analysis code will be deployed to the ``hadoop`` user's home directory

Log into the frontend node and switch to the ``hadoop`` user.
You can run the analysis by::

  $ sbt package
  $ spark-submit \
      --deploy-mode cluster \
      --master yarn \
      --driver-class-path $(hbase classpath) \
      --conf spark.executor.extraClassPath=$(hbase classpath) \
      --class MINDTCT \
      target/scala-2.10/nbis_2.10-1.0.jar
