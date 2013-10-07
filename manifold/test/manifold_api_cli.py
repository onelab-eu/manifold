#!/usr/bin/env python
import xmlrpclib

### Your Manifold Server

#srv = xmlrpclib.Server("http://manifold.pl.sophia.inria.fr:7080/", allow_none=True)
srv = xmlrpclib.Server("http://test.myslice.info:7080/", allow_none=True)

### Your Username/Password

auth = {"AuthMethod": "password", "Username":"john", "AuthString": "123456"}


### Your slice HRN

slicename= "ple.inria.plenitos"


### Some useful example of queries



# Get users and resources of the slice
q1 = {
    'object': 'slice',
    'filters': [['slice_hrn', '==', 'ple.inria.f14']], 
    'fields': ['resource.type', 'resource.network_hrn', 'resource.hostname',   
               'user.user_hrn', 'slice_hrn', 'resource.resource_hrn'], 
    'action': 'get'
}


# get all resources (e.g: ListResources)
q3 = {
    'action' : 'get',
    'object' : 'resource',
#    'filters': [["slice_hrn","==", slicename]],
    'fields' : ['urn']
#     'fields' : []
}

# get slice info (e.g: ListResources(slice))
q4 = {
    'action':  'get',
    'object':   'slice',
    'filters':      [['slice_hrn', '=', slicename]],
    'fields':       ['resource.urn']
}


# update slice (e.g: createsliver)
q5 = {
    'action': 'update',
    'object':   'slice',
    'filters':      [['slice_hrn', '=', slicename]],
#    'params':       {'resource': [{'urn': 'urn:publicid:IDN+ple:vrijeple+node+planetlab1.cs.vu.nl'}]}
    'params':       {'resource': [{'urn': 'urn:publicid:IDN+ple:unavarraple+node+planetlab2.tlm.unavarra.es'}]}
}

# create session
q6 = {
    'object': 'local:session', 
    'action': 'create'}
 
# get metadata
q7 = {
    'action':  'get',
    'object':   'local:object',
#    'object':   'local:platform',
}

# get networks
q8 = {
    'object': 'network',
#    'params': {}, 
#    'filters': [],
    'fields': ['network_hrn'],
    'action': 'get'}

# get platforms
q9 = {
    'action':  'get',
    'object':  'local:platform',
    'fields':  ['platform_id','platform','platform_longname','gateway_type']
}


# get account
q10 = {
    'action':  'get',
    'object':  'local:account',
#    'filters': [['']]
    'fields':  ['platform_id', 'user_id', 'auth_type']
}

# create account
q11 = {
    'action':  'create',
    'object':  'local:account',
    'params':  {'platform_id': 3, 'user_id': 7, 'auth_type': 'user' }
}


# update user
q12 = {
    'action':  'update',
    'object':  'local:user',
    'filters': [['email', '==', 'mohamed.larabi@inria.fr']],
#    'params':  {'password': 'pw'}
    'params':  {'config': '{"user_hrn": "ple.inria.mohamed_larabi"}'}
}


# get user
q13 = {
    'action':  'get',
    'object':  'local:user',
    'filters': [['email', '==', 'mohamed.larabi@inria.fr']],
#    'params':  {'password': 'pw'}
}


### Execute the selected query

rs=srv.forward(auth, q1)

### Print returned results

print rs
