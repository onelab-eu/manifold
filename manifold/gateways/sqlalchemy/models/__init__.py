#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Base class.
#
# Jordan Auge       <jordan.auge@lip6.fr>
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

from sqlalchemy.ext.declarative import declarative_base
from ..models.base              import Base

Base = declarative_base(cls = Base)

