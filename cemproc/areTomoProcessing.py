import os
import numpy as np
import shutil

from inout import *
def findOutlierInGauss(x):
    stdThreshold = 3.
    removeOutlier = True
    weights = np.ones((len(x)))
    oriSumWeight = np.sum(weights)
    while removeOutlier:
        m = np.sum(x) / np.sum(weights)
        s = (np.sum([(m-x[i])**2 for i in range(len(x)) if weights[i] > 0]) / np.sum(weights))**0.5
        weights = [0 if (np.abs(m - x[ind]) > stdThreshold * s) else 1 for ind in range(len(x))]

        if np.sum(weights) < oriSumWeight:
            oriSumWeight = np.sum(weights)
        else:
            removeOutlier = False

    badPositions = [ind for ind in range(len(weights)) if weights[ind] == 0]
    return badPositions


def removeBadImages(inStk,angleDoseFile,pref,nr,pos):
    badPosList = 'cutViews.txt'
    defaultAngleDoseFile = 'angleDose.dat'
    ang = []
    dose = []
    if os.path.isfile(angleDoseFile):
        with open(angleDoseFile,'r') as aD:
            for line in aD.readlines():
                ls = line.rstrip('\n').split()
                ang.append(ls[0])
                if len(ls) > 1:
                    dose.append(ls[1])
                else:
                    dose.append(0)
        angDosePref = '.'.join(angleDoseFile.split('.')[:-1])
        angDosePos = angleDoseFile.split('.')[-1]
        shutil.move(angleDoseFile, angDosePref + '_ori.' + angDosePos)
    else:
        # -60,60
        minmax = angleDoseFile.split(',')
        angleDoseFile = defaultAngleDoseFile
        assert len(minmax) == 2, sys.exit('angDose argument error, file not found or improper tilt range format, it should be e.g. -60,60')
        try:
            mini = float(minmax[0])
        except:
            sys.exit('angDose argument error, file not found or improper tilt range format, it should be e.g. -60,60 first number is not integer')
        try:
            maxi = float(minmax[1])
        except:
            sys.exit('angDose argument error, file not found or improper tilt range format, it should be e.g. -60,60 second number is not integer')

        step = (maxi - mini) / (float(inStk.zsize) - 1.)

        for i in np.arange(mini,maxi+step,step):
            ang.append(i)
            dose.append(0)

    m = []
    s = []
    for i in range(inStk.zsize):
        m.append(np.mean(inStk.data[i,:,:]))
        s.append(np.std(inStk.data[i,:,:]))
    badFromMeanEval = findOutlierInGauss(m)
    badFromStdEval = findOutlierInGauss(s)

    badJoined = badFromMeanEval + badFromStdEval

    if len(badJoined) > 0:
        badFinal = np.unique(badJoined)
        with open(badPosList,'w') as bP:
            for i in badFinal:
                bP.write('%d\n' % (int(i)+1))
        outStkData = np.zeros((inStk.zsize-len(badFinal),inStk.ysize,inStk.xsize),dtype = np.float)
        c = 0
        angDose = []
        for i in range(inStk.zsize):
            if i in badFinal:
                pass
            else:
                outStkData[c,:,:] = inStk.data[i,:,:]
                angDose.append([ang[i], dose[i]])
                c += 1
        o = Image()
        o.xapix = inStk.xapix
        o.yapix = inStk.xapix
        o.zapix = inStk.xapix
        o.zsize, o.ysize, o.xsize = outStkData.shape
        o.data = outStkData
        shutil.move(pref + str(nr) + pos, pref + str(nr) + '_ori' + pos)
        o.writeMRC(pref + str(nr) + pos)
        with open(angleDoseFile, 'w') as adFile:
            for a in angDose:
                adFile.write("%.3f\t%.3f\n" % (float(a[0]), float(a[1])))
    else:
        with open(angleDoseFile, 'w') as adFile:
            for a in range(len(ang)):
                adFile.write("%.3f\t%.3f\n" % (float(ang[a]), float(dose[a])))

    return angleDoseFile
def runAreTomoProcessing(inStkName,angleDoseFile,tiltAng,volZ,outBin,kV,apix):
    com = 'AreTomo -InMrc %s -OutMrc %s -AngFile %s -Kv %d -PixSize %.3f -OutBin %d -TiltAxis %s -1 -VolZ %d -OutImod 1 -TiltCor 1' % (inStkName,'volume_'+inStkName,angleDoseFile,kV,float(apix),outBin,tiltAng,volZ)
    print ('Running command:')
    print(com)
    os.system(com)
