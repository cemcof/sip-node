import os
import glob
import shutil
import time
from collections import OrderedDict
from inout import *
from auxiliary import module
import areTomoProcessing

import datetime

storagePref = '/storage/brno14-ceitec/shared/cemcof' # general storage path
userType = 'internal' # userType: 'internal' vs. 'external' vs. 'private'
projectName='250121_BumbaL' # project folder name, contains raw data
moviePref = 'mic_'
moviePos = '.mrcs'

#MotionCor2 related options
apix=1.15 # magnified pixel size [A/px]
#!!!frameDose not used any more - does not work properly, shall be done in Relion later
frameDose=0.67 # dose per frame [e/A^2]
kv=200 # acceleration voltage [kV]
tiltAxis = -5.5 # tilt axis rotation (-3.55 for Krios, -5.5 for Arctica
volZ = 1600 # thickness of unbinned tomogram
outBin = 8 # tomogram binning

#ctffind4 related options
cs=2.7 # spherical abberation of the objective lens [mm]
ac=0.07 # amplitude contrast
defL=15000 # minimil defocus used for CTF fitting [A]
defH=45000 # maximal defocus used for CTF fitting [A]
resL=50 # minimal resolution used in CTF fitting [A]
resH=8 # maximal resolution used in CTF fitting [A]
pwrSize = 512 # size of the amplitude spectrum to compute [px]
phasePlate = 0 # 0 | 1
minPhaseShift = 0.2 # minimal phase shift to consider during fit [rad]
maxPhaseShift = 3 # maximum phase shift to consider during fit [rad]

######################### DO NOT CHANGE CODE BELOW ###############################

inDir = os.path.join(storagePref,userType,f"{datetime.datetime.now():DATA_%y}",projectName,'Movies') # path to the directory with frames
# outPath - output folder, defined below
if userType == 'internal':
    outPath = os.path.join(storagePref,userType,f"{datetime.datetime.now():DATA_PROCESSING_%y}")
else:
    outPath = os.path.join(storagePref,userType,f"{datetime.datetime.now():DATA_%y}")

gpu=0 # GPU to use

# path to program binaries
motionCorPath='MotionCor2' # path to motionCor2 binary
ctffind4Path='ctffind' # path to ctffind4 binary
imodPath='' # path to imod bin folder

# processing directory
procPath='/storage/brno14-ceitec/home/emcf/scratch/GPUA' # working directory

# no need to change the script below

try:
  os.stat(procPath)
except:
  os.mkdir(procPath)
os.chdir(procPath)
if os.path.isfile('tomo.run'):
  os.remove('tomo.run')
f=open('tomo.run','w')
f.write('True')
f.close()

def transferToHSM(pathOut,projName,tomoName):
  result=[]
  msg=''
  try:
    os.stat(pathOut + '/' + projName)
  except:
    os.mkdir(pathOut + '/' + projName)

  try:
    shutil.move(tomoName,pathOut + '/' + projName + '/')
    result.append('True')
  except:
    result.append('False')
    msg='ERROR: Data could not be transfered to HSM storage'

  result.append(msg)
  return result

def moveFromScope(inMic,movieFolder):
  result=[]
  msg=''
  while not ((time.time() - os.path.getmtime(inMic)) > 30):
    time.sleep(5)

  try:
    shutil.move(inMic, movieFolder)
    result.append('True')
  except:
    result.append('False')
    msg='ERROR: Could not transfer stack' +  inMic + 'from camera'

  result.append(msg)
  return result

def runMotionCor2(micPref,voltage,apix,preDose,frameDose,gpuId):

  module('load','motionCor2/1.4.0')
  if 'mrc' in moviePos:
    command = motionCorPath + ' -InMrc ' + str(micPref) +  '.mrcs -OutMrc ' + str(micPref) + '.mrc -kV ' + str(voltage) + ' -Iter 3 -Bft 150 -PixSize ' + str(apix) + ' -Gpu ' + str(gpuId)
  else:
    command = motionCorPath + ' -InTiff ' + str(micPref) +  '.tif -OutMrc ' + str(micPref) + '.mrc -kV ' + str(voltage) + ' -Iter 3 -Bft 150 -Gain ../gain.mrc -RotGain 3 -FlipGain 1 -PixSize ' + str(apix) + ' -Gpu ' + str(gpuId)
  os.system(command + ' >> ' + 'motCor.log 2>&1')


  f = open('motCor.log','r')
  lines=f.readlines()
  for line in lines:
    lineSplit=line.split()
    if len(lineSplit) > 0 and lineSplit[0] == 'Stack' and lineSplit[1] == 'size:':
      preDose = preDose + frameDose*(int(lineSplit[4]))
      break
  f.close()
#  os.remove('motCor.log')
  module('unload','motionCor2/1.4.0')
  return preDose


def runCtffind4(micPref,voltage,apix,cs,ac,pwrSize,defMin,defMax,resMin,resMax,phasePlate,minPhaseShift,maxPhaseShift):

  module('load','ctffind4')
  if os.path.exists('ctffind.run'):
    os.remove('ctffind.run')
    f=open('ctffind.run','w')
  else:
    f=open('ctffind.run','w')

  #command - ctffind inMic outPwr apix voltage cs ac amplitudeSpectrumSize minRes maxRes minDef maxDef defStep isAnyKnownAstig slowSearch restrainAstig toleratedAstig additionalPhaseShift minPhase maxPhase phaseStep expertOptions

  if (phasePlate == 0):
    f.write(str(micPref) + '.mrc\n' + micPref + '_pwr.mrc\n' + str(apix) + '\n' + str(voltage) + '\n' + str(cs) + '\n' + str(ac) +  '\n' + str(pwrSize) +  '\n' + str(resMin) +  '\n' + str(resMax) +  '\n' + str(defMin) +  '\n' + str(defMax) + '\n500.0\nno\nyes\nyes\n800.0\nno\nno\n')
  else:
    f.write(str(micPref) + '.mrc\n' + micPref + '_pwr.mrc\n' + str(apix) + '\n' + str(voltage) + '\n' + str(cs) + '\n' + str(ac) +  '\n' + str(pwrSize) +  '\n' + str(resMin) +  '\n' + str(resMax) +  '\n' + str(defMin) +  '\n' + str(defMax) + '\n500.0\nno\nyes\nyes\n800.0\nyes\n' + str(minPhaseShift) + '\n' + str(maxPhaseShift) + '\n0.1\nno\n')
 
  f.close() 
  os.system(ctffind4Path + '< ctffind.run >> ' + 'ctffind4.log 2>&1')
  try:
    shutil.move(micPref+'_DW.mrc',micPref+'.mrc')
  except:
    pass
  module('unload','ctffind4')

def handleCtffindFiles(inFile,count,outFile):
  f=open(inFile,'r')
  names = f.readline().split()
  f.close()

  out=open(outFile,'w')

  cur = 0
  f=open(names[cur][:-3]+'txt','r')
  lines=f.readlines()
  for line in lines:
    lineSplit = line.split()
    if lineSplit[1] == 'Input':
      lineSplit[3]='tomo'+str(count)+'.mrc'
      lineSplit[8]=str(len(names))
    out.write(' '.join(lineSplit)+'\n')
  f.close()

  cur = cur + 1
  while cur < len(names):
    f=open(names[cur][:-3]+'txt','r')
    line=f.readlines()[-1].rstrip('\n').split()
    line[0]=str(cur+1)
    out.write(' '.join(line)+'\n')
    cur = cur + 1
    f.close()

  out.close()
    

def runSingle(inDir,outPath,projName,pref,count,initAng,kV,apix,frameDose,gpu,cs,ac,pwrSize,defMin,defMax,resMin,resMax,phasePlate,minPhaseShift,maxPhaseShift):

  outDir = 'tomo'+str(count)
  if os.path.exists(outPath+'/'+projName+'/'+outDir):
    return

  # this is bad, need to find better solutions for cases where script fails in the middle of processing
  if os.path.exists(outDir):
    return
  else:
    os.mkdir(outDir)
    os.mkdir(outDir+'/Movies')

  os.chdir(outDir)

  run=True
  listMics={}
  preDose=0.0

  while run:

    runSig=open('../tomo.run','r').read().rstrip('\n')
    mic=sorted(glob.glob(inDir+'/'+pref+'*'+moviePos)) #, key=os.path.getmtime)
    while len(mic) == 0:
      time.sleep(15)
      mic=sorted(glob.glob(inDir+'/'+pref+'*'+moviePos)) #, key=os.path.getmtime)
      if not(open('../tomo.run','r').read().rstrip('\n') == 'True'):
        break

    if len(mic) > 0:
      while (time.time() - os.path.getmtime(mic[0]))<15:
        time.sleep(5)

      micName = mic[0].split('/')[-1]
      angle=int(mic[0].split('/')[-1].split('_')[-1].split('.')[0])
    else:
      angle=initAng
    if (len(listMics) > 0) and (angle == initAng):
      listMicsSort = OrderedDict(sorted(listMics.items(), key=lambda x: x[1][0]))
      f=open('listMics.dat','w')
      f_pw=open('listMicsPw.dat','w')
      f2=open('angleDose.dat','w')
      f3=open('angles.tlt', 'w')
      for k,v in listMicsSort.items():
        f.write(str(k)+' ')
        f_pw.write(str(k)[:-4]+'_pwr.mrc ')
        f2.write('%.3f\t%.3f\n' % (v[0],v[1]))
        f3.write('%.3f\n' % v[0])

      f.close()
      f_pw.close()
      f2.close()
      f3.close()
      handleCtffindFiles('listMicsPw.dat',count,'defocus_file.txt')
      # create stack (tomogram)
      module('load','imod')
      os.system(imodPath+'newstack -mode 2 $(less listMics.dat) tomo'+str(count)+'.mrc')
#      os.system(imodPath+'/newstack -mode 2 -meansd 0.0,1.0  $(less listMics.dat) tomo'+str(count)+'.mrc')
      os.system(imodPath+'newstack -mode 2  $(less listMicsPw.dat) powerSpectra'+str(count)+'.mrc')
      module('unload','imod')
      # remove averaged intermediate data
      for i in glob.glob(pref+'*'):
        os.remove(i)
      os.remove('listMics.dat')
      os.remove('listMicsPw.dat')
      ##
      # tomogram generation here
      i = Image()
      i.readMRC('tomo'+str(count)+'.mrc')
      if i.zsize > 1:
        module('load','cuda/11.6.1')
        module('load','areTomo')
        angleDoseFile = 'angleDose.dat'
        angleDoseFile = areTomoProcessing.removeBadImages(i, angleDoseFile, 'tomo', count, '.mrc')
        areTomoProcessing.runAreTomoProcessing('tomo' + str(count) + '.mrc', angleDoseFile, tiltAxis, volZ, outBin, kV, apix)
        module('unload','areTomo')
        module('unload','cuda/11.6.1')
      ##
      os.chdir('../')
      transferToHSM(outPath,projName,outDir)
      run=False
      return
    moveFromScope(mic[0],micName)
#delete when corrent input
#    shutil.move(micName, 'Movies/'+micName)
#    os.system('trimvol Movies/' + micName + ' ' + micName + ' -nx 3838 -ny 3710')
#end delete
    preDose = runMotionCor2(micName[:-len(moviePos)],kV,apix,preDose,frameDose,gpu)
    runCtffind4(micName[:-len(moviePos)],kV,apix,cs,ac,pwrSize,defMin,defMax,resMin,resMax,phasePlate,minPhaseShift,maxPhaseShift)
    listMics[micName[:-len(moviePos)]+'.mrc']=[angle,preDose]
# activate back
    shutil.move(micName, 'Movies/'+micName)
#delete when correct input
#    os.remove(micName)
#end delete

def main():

  count=1
  run=open('tomo.run','r').read().rstrip('\n')
  movies=sorted(glob.glob(inDir + '/' + moviePref + '*' + moviePos)) #, key=os.path.getmtime)
  initAng=int(movies[0].split('/')[-1].split('_')[-1].split('.')[0])

  while run == 'True':
    movies=glob.glob(inDir + '/' + moviePref+ '*' + moviePos)
    
    if len(movies) == 0:
      time.sleep(15)
    else:
      print('... Working on tomogram nr.' + str(count) + ' ...')
      runSingle(inDir,outPath,projectName,moviePref,count,initAng,kv,apix,frameDose,gpu,cs,ac,pwrSize,defL,defH,resL,resH,phasePlate,minPhaseShift,maxPhaseShift)
      count  = count + 1

    run=open('tomo.run','r').read().rstrip('\n')


if __name__ == "__main__":
  main() 
