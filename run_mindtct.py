from pyspark import SparkContext


sc = SparkContext(appName='MINDTCT')
pngFiles = sc.binaryFiles('hdfs:///nist/NISTSpecialDatabase4GrayScaleImagesofFIGS/sd04/png_txt/figs_0/')\
           .filter(lambda (k, v): k.endswith('.png'))

def mindtct((pngpath, pngdata)):
    from subprocess import Popen
    import tempfile
    import shutil
    import os.path
    import glob

    fd = tempfile.NamedTemporaryFile()
    fd.write(pngdata)
    fd.flush()

    outdir = tempfile.mkdtemp()

    cmd = ['mindtct', fd.name, os.path.join(outdir, 'mindtct')]

    try:
        p = Popen(cmd)
        stdout, stderr = p.communicate()
        returncode = p.returncode

        results = []
        for path in glob.iglob(os.path.join(outdir, 'mindtct.*')):
            pref, ext = os.path.splitext(path)
            with open(path, 'rb') as fd:
                data = fd.read()
            results.append((ext, data))
        
        return pngpath, returncode, stdout, stderr, results

    finally:
        shutil.rmtree(outdir)

    
mindtct_result = pngFiles.map(mindtct).collect()
for pngpath, retval, out, err, results in mindtct_result:
    print pngpath, retval
