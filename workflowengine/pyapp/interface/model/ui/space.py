import pymodel as model

#@doc class that provides properties of a repository
class repository(model.Model):
    #@doc the url of the repository
    url = model.String(thrift_id=1)
    #@doc the username of the repository
    username = model.String(thrift_id=2)
    #@doc the password of the repository
    password = model.String(thrift_id=3)

#@doc class that provides properties of a space
class space(model.RootObjectModel):
    #@doc name of the space
    name = model.String(thrift_id=1)
    #@doc tags related to the space
    tags = model.String(thrift_id=2)
    #@doc repository related to the space
    repository = model.Object(repository, thrift_id=3)
