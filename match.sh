#!/usr/bin/env bash

source /etc/profile


usage() {
    cat <<EOF
Usage: $0 load-images|ridge-detection|subsample|match
EOF
}


load-images() {
    spark-submit \
        --master yarn \
        --deploy-mode cluster \
        --driver-class-path $(hbase classpath) \
        --class LoadData \
        target/scala-2.10/NBIS-assembly-1.0.jar \
        /tmp/nist/NISTSpecialDatabase4GrayScaleImagesofFIGS/sd04/sd04_md5.lst
    touch .load-images
}

ridge-detection() {
    spark-submit \
        --master yarn \
        --deploy-mode cluster \
        --driver-class-path $(hbase classpath) \
        --class RunMindtct \
        target/scala-2.10/NBIS-assembly-1.0.jar
    touch .ridge-detection
}

subsample() {
    spark-submit \
        --master yarn \
        --deploy-mode cluster \
        --driver-class-path $(hbase classpath) \
        --class RunGroup \
        target/scala-2.10/NBIS-assembly-1.0.jar \
        probe 0.001 \
        gallery 0.01
    touch .subsample
}

match() {
    spark-submit \
        --master yarn \
        --deploy-mode cluster \
        --driver-class-path $(hbase classpath) \
        --class RunBOZORTH3 \
        target/scala-2.10/NBIS-assembly-1.0.jar \
        probe gallery
}


_FIXME () {
    local fn=$1
    test -f .$fn && return
    $fn && touch .$fn
}

cd $HOME
pwd
ls

case $1 in
    load-images)
        _FIXME load-images
        ;;
    ridge-detection)
        _FIXME ridge-detection
        ;;
    subsample)
        _FIXME subsample
        ;;
    match)
        _FIXME match
        ;;
    *)
        usage
        exit 1
        ;;        
esac


