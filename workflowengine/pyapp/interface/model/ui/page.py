import pymodel as model

#@doc class that provides properties of a page
class page(model.RootObjectModel):
    #@doc name of the page
    name = model.String(thrift_id=1)
    #@doc space of the page
    space = model.GUID(thrift_id=2)
    #@doc category of the page
    category = model.String(thrift_id=3)
    #@doc GUID of the parent page
    parent = model.GUID(thrift_id=4)
    #@doc tags related to the page    
    tags = model.String(thrift_id=5)
    #@doc actual page content
    content = model.String(thrift_id=6)
    #@doc order of the page    
    order = model.Integer(thrift_id=7)
    #@doc title of the page
    title = model.String(thrift_id=8)
