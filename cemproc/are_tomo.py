
def runAreTomoProcessing(inStkName,angleDoseFile,tiltAng,volZ,outBin,kV,apix):
    com = 'AreTomo -InMrc %s -OutMrc %s -AngFile %s -Kv %d -PixSize %.3f -OutBin %d -TiltAxis %s -1 -VolZ %d -OutImod 1 -TiltCor 1' % (inStkName,'volume_'+inStkName,angleDoseFile,kV,float(apix),outBin,tiltAng,volZ)
    print ('Running command:')
    print(com)
    os.system(com)
