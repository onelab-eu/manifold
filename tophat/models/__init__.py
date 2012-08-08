
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base, declared_attr

engine = create_engine('sqlite:///:memory:', echo=True)

# OLD from tophat.conf import settings
# OLD from sqlalchemy.pool import StaticPool
# OLD metadata.bind = create_engine("sqlite:///%s?check_same_thread=False" %
# OLD        settings.DATABASE_PATH, echo=False, poolclass=StaticPool)

class Base(object):
    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()

    __mapper_args__= {'always_refresh': True}

    #id =  Column(Integer, primary_key=True)

from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base(cls=Base)

# Models
from tophat.models.platform import Platform
from tophat.models.field import Field

Base.metadata.create_all(engine) 
