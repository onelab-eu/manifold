#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Model class. All the Model* classes should inherits Model.
#
# Jordan Auge       <jordan.auge@lip6.fr>
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

from sqlalchemy.ext.declarative                 import declarative_base
from manifold.gateways.sqlalchemy.models.base   import Base

Model = declarative_base(cls = Base)

