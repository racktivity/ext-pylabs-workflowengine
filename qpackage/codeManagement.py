__author__ = 'aserver'
__tags__     = "codeManagement",
__priority__ = 1

def match(q, i, params, tags):
    return True

def main(q, i, params, tags):
    qpackage = params['qpackage']
    from pymonkey.clients.hg.HgRecipe import HgRecipe
    recipe = HgRecipe()
    connection = i.hg.connections.findByUrl("http://bitbucket.org/despiegk/pylabs_workflowengine/")
    recipe.addRepository(connection)
    recipe.addSource(connection, q.system.fs.joinPaths('lib'), q.system.fs.joinPaths('var', 'src', 'workflowengine', 'lib'), branch='3.1antony')
    recipe.addSource(connection, q.system.fs.joinPaths('apps'), q.system.fs.joinPaths('var', 'src', 'workflowengine', 'apps'), branch='3.1antony')
    recipe.executeTaskletAction(params['action'])
