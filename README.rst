
.. sidebar:: **Table of Contents**

   .. contents::
      :local:


=====
About
=====

This example shows how to deploy the NIST Fingerprint dataset_ (Special Database 4) and tools_ (NBIS) to the cluster.
Additionally, this demonstrates how to use Apache Spark to perform a fingerprint matching using the NBIS tools, store the results in HBase and use Apache Drill to query the results.


.. _dataset: http://www.nist.gov/srd/nistsd4.cfm
.. _tools: http://www.nist.gov/itl/iad/ig/nigos.cfm


========
 Status
========

Work-in-progress


===============
 Overview [1]_
===============

--------------
 Introduction
--------------

Fingerprint recognition refers to the automated method for verifying a match between two fingerprints and that is used to identify individuals and verify their identity.
Fingerprints (Figure 1) are the most widely used form of biometric used to identify individuals.

.. figure:: images/fingerprints.png
   :alt: Example fingerprints
   :align: center

   **Figure 1** Example fingerprints



The automated fingerprint matching generally required the detection of different fingerprint features (aggregate characteristics of ridges, and minutia points) and then the use of fingerprint matching algorithm, which can do both one-to- one and one-to- many matching operations.
Based on the number of matches a proximity score (distance or similarity) can be calculated.


------------
 Algorithms
------------

For this work we will use the following algorithms:

- MINDTCT: The NIST minutiae detector, which automatically locates and records ridge ending and bifurcations in a fingerprint image. (http://www.nist.gov/itl/iad/ig/nbis.cfm)
- BOZORTH3: A NIST fingerprint matching algorithm, which is a minutiae based fingerprint-matching algorithm. It can do both one-to- one and one-to- many matching operations. (http://www.nist.gov/itl/iad/ig/nbis.cfm)

----------
 Datasets
----------

We use the following NIST dataset for the study:

- Special Database 14 - NIST Mated Fingerprint Card Pairs 2. (http://www.nist.gov/itl/iad/ig/special_dbases.cfm)


--------------------
 Specific Questions
--------------------

#. Match the fingerprint images from a probe set to a gallery set and report a match scores?
#. What is the most efficient and high-throughput way to match fingerprint images from a probe set to a large fingerprint gallery set?


=======
 Stack
=======

- Apache Hadoop (with YARN)
- Apache Spark
- Apache HBase
- Apache Drill
- Scala


========
 Method
========

#. Launch a virtual cluster
#. Deploy the stack

   #. `Big Data Stack`_
   #. `deploy.yml`_

#. Prepare the dataset

   #. adds dataset to HBase -- scala, spark, hbase
   #. partitions dataset into "probe" and "gallery"

#. Run the analysis

   #. Load the probe sete
   #. Load the gallery set
   #. Compare each image in "probe" to "gallery"
   #. Store results in HBase

#. Use Drill to query


.. _deploy.yml: deploy.yml
.. _dataset.yml: dataset.yml
.. _analysis.yml: analysis.yml
.. _Big Data Stack: https://github.com/futuresystems/big-data-stack


===============
 Prerequisites
===============

In order to run this example, you need to have the following on your system:

#. Python 2.7
#. Pip
#. Virtualenv
#. Git

Additionally, you should have access to a cloud provider (such as OpenStack or Amazon Web Services).
Any instances need to be accessible via SSH and have Python 2.7 installed.

#. Cloud provider

**Note**: Your controller node needs to be able to run Ansible.


============
 Quickstart
============

While detailed instructions are provided in a later section, if you
want to get started quickly here is what you need to do.

**note**: We assume the login user is ``ubuntu``, you may need to adjust the ansible commands to accomodate a different user name.


#. Have an account on github.com

#. Upload you public ssh key to `github.com <https://github.com/settings/keys>`_

#. Clone this repository::

     $ git clone --recursive git@github.com:cloudmesh/example-project-nist-fingerprint-matching

   **IMPORTANT**: make sure to include the ``--recursive`` flag else you will encounter errors during deployment.

#. Create a virtual environment and install the dependencies::

     $ virtualenv venv
     $ source venv/bin/activate
     $ pip install -r big-data-stack/requirements.txt

#. Start a virtual cluster (Ubuntu 14.04) with at least three nodes and obtain the IP addresses. We assume that the cluster is homogeneous.
#. In the ``big-data-stack`` directory, generate the ansible files using ``mk-inventory``::

     $ python mk-inventory -n mycluster 192.168.1.100  192.168.1.101 192.168.1.102

#. Make sure each node is accessible by ansible::

     $ ansible all -o -m ping

#. Deploy the stack (~ FIXME minutes)::

     $ ansible-playbook play-hadoop.yml addons/{spark,hbase,drill}.yml

#. Deploy the dataset and NBIS software (~ FIXME minutes)::

     $ ansible-playbook ../{dataset,software}.yml

#. Login to the first node and switch to the ``hadoop`` user::

     $ ssh ubuntu@192.168.1.100
     $ sudo su - hadoop

#. Load the images data into HBase (~ 10 minutes)::

     $ time spark-submit \
         --master yarn \
         --deploy-mode cluster \
         --driver-class-path $(hbase classpath) \
         --class LoadData \
         target/scala-2.10/NBIS-assembly-1.0.jar \
         /tmp/nist/NISTSpecialDatabase4GrayScaleImagesofFIGS/sd04/sd04_md5.lst

#. Run MINDTCT for ridge detection (~ FIXME minutes)::

     $ time spark-submit \
         --master yarn \
         --deploy-mode cluster \
         --driver-class-path $(hbase classpath) \
         --class RunMindtct \
         target/scala-2.10/NBIS-assembly-1.0.jar

#. Sample the images to select subsets as the probe and gallery images. In this case the probe set is 0.1% and the gallery set is 1% (~ FIXME minutes)::

     $ time spark-submit \
         --master yarn \
         --deploy-mode cluster \
         --driver-class-path $(hbase classpath) \
         --class RunMindtct \
         target/scala-2.10/NBIS-assembly-1.0.jar

#. Match the probe set to the gallery set (~ FIXME minutes)::

     $ time spark-submit \
         --master yarn \
         --deploy-mode cluster \
         --driver-class-path $(hbase classpath) \
         --class RunBOZORTH3 \
         target/scala-2.10/NBIS-assembly-1.0.jar \
         probe gallery

#. Use Drill to query::

     $ sqlline -u jdbc:drill:zk=mycluster0,mycluster1,mycluster2;schema=hbase

     > use hbase;
     > SELECT
       CONVERT_FROM(Bozorth3.Bozorth3.probeId, 'UTF8') probe,
       CONVERT_FROM(Bozorth3.Bozorth3.galleryId, 'UTF8') gallery,
       CONVERT_FROM(Bozorth3.Bozorth3.score, 'INT_BE') score
       FROM Bozorth3
       ORDER BY
       CONVERT_FROM(Bozorth3.Bozorth3.score, 'INT_BE')
       DESC
       LIMIT 10
       ;

==============
 Installation
==============

#. Clone this repository::

     git clone --recursive git@github.com:cloudmesh/example-project-nist-fingerprint-matching

#. Create a virtualenv and activate it::

     virtualenv venv
     source venv/bin/activate

#. Install the requirements::

     pip install -r big-data-stack/requirements.txt


=======
 Using
=======

#. Launch a virtual cluster. Make sure the Ansible inventory file matches the specifications for the Big Data Stack.

#. Deploy Hadoop, Spark, HBase and Drill::

     cd big-data-stack
     ansible-playbook -i <path/to/inventory.txt> play-hadoop.yml \
       addons/spark.yml addons/hbase.yml addons/drill.yml
     cd -

#. Deploy the software::

     ansible-playbook -i <path/to/inventory.txt> software.yml

#. Deploy and partition the dataset::

     ansible-playbook -i <path/to/inventory.txt> dataset.yml

#. Run the analysis::

     ssh hadoop@frontend
     ./analysis.sh

#. Use Drill to query



====================
 Build Instructions
====================

These instructions are for manually building and bundling the source
code for loading the images into HBase and running the analysis::

  $ sbt package
  $ sbt assembly


====================
 Running with Spark
====================

After building, the target jarfile to submit is located at::

  target/scala-2.10/NBIS-assembly-1.0.jar


When submitting, you need to tell Spark to provide HBase in the execution classpath using::

  --driver-class-path $(hbase classpath)

------------
 Components
------------

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

   
--------------------------------
 Local Submission (for testing)
--------------------------------

::

   spark-submit \
     --driver-class-path $(hbase classpath) \
     --class $MAIN_CLASS \
     target/scala-2.10/NBIS-assembly-1.0.jar \
     $MAIN_CLASS_ARGS

-------------------------------------
 Cluster Submission (for production)
-------------------------------------

This is the same as the local submission, but add::

  --master yarn --deploy-mode cluster


============
 References
============


.. [1] This overview section was adapted from the NIST Big Data Public Working Group draft *Possible Big Data Use Cases Implementation using NBDRA* authored by Afzal Godil and Wo Chang
