#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Annotations enrich a Query pass to a Manifold Gateway
# or to a Manifold Node. For instance, Annotations may
# carry user account information and so on. 
#
# Jordan Augé       <jordan.auge@lip6.fr>
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#
# Copyright (C) 2013 UPMC 

class Annotation(dict):
    def to_dict(self):
        return dict(self)

    #@returns(Annotation)
    def __ior__(self, annotation):
        # Update would erase existing keys...
        #self.update(annotation)
        for key, value in annotation.items():
            if key in self:
                continue
            self[key] = value
        return self

