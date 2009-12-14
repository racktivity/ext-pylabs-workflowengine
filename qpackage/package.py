__author__ = 'aserver'
__tags__ = 'package',

def main(q, i, params, tags):
    qpackage = params["qpackage"]

    filesDir = q.system.fs.joinPaths(qpackage.packageDir, "files")
    q.system.fs.removeDirTree(filesDir)
    q.system.fs.createDir(filesDir)

    q.system.fs.copyDirTree(q.system.fs.joinPaths(q.dirs.baseDir,'var','src','workflowengine','apps'), q.system.fs.joinPaths(filesDir,"generic",'apps'))
    q.system.fs.copyDirTree(q.system.fs.joinPaths(q.dirs.baseDir,'var','src','workflowengine','lib'), q.system.fs.joinPaths(filesDir,"generic",'lib'))

