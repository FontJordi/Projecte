import os
import json

def goBack(times):

    if(times == 0):
        return os.getcwd()

    for i in range(times):
        if i == 0: 
             cwd = os.getcwd()
             path_parent = os.path.dirname(cwd)
        else:
            cwd = path_parent
            path_parent = os.path.dirname(cwd)

    return path_parent

def goForth(path, foldername):

    #cwd = os.getcdw()
    next = os.chdir(f"{path}/{foldername}")
    #print(f"{path}/{foldername}")

    return os.getcwd()


def readTxt(dir, name):

    print(f"Opening file {dir}/{name}")

    with open(f"{dir}/{name}") as f:
        lines = f.readlines()
    return lines

def writeResponse(dir, name, listtwit):

    json_str = json.dumps(listtwit, indent=4)
    print(json_str)
    with open(f"{dir}/{name}", 'w') as f:
        f.write(json_str)


def getPath():
    return os.getcwd()

import glob

def csvListFiles(dir):
 
     return glob.glob(dir + "/*.csv")





#readTxt(goForth(goBack(0),"myfolder"), "tokens.txt")[0:3]
