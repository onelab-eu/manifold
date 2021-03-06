from sqlalchemy                 import create_engine
from sqlalchemy.orm             import sessionmaker
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from manifold.util.log          import Log

# exceptions.Exception: Error during local request: (ProgrammingError) SQLite objects created in a thread can only be used in that same thread.The object was created in thread id 139885302400768 and this is thread id 139885208180480 'SELECT platform.platform AS platform_platform \nFROM platform' [immutabledict({})]
#engine = create_engine('sqlite:///:memory:?check_same_thread=False', echo=False)
#engine = create_engine('sqlite:////var/myslice/db.sqlite?check_same_thread=False', echo=False)

#url = 'sqlite:////var/myslice/db.sqlite?check_same_thread=False'
from manifold.util.storage import DBStorage
url = DBStorage.get_url()

engine = create_engine(url, echo=False, pool_recycle=3600)

# OLD from tophat.conf import settings
# OLD from sqlalchemy.pool import StaticPool
# OLD metadata.bind = create_engine("sqlite:///%s?check_same_thread=False" %
# OLD        settings.DATABASE_PATH, echo=False, poolclass=StaticPool)

from manifold.models.base       import Base

Base    = declarative_base(cls = Base)
SQLAlchemySession = sessionmaker(bind = engine)
db = SQLAlchemySession()

# Models
from manifold.models.platform       import Platform
from manifold.models.user           import User
from manifold.models.account        import Account
from manifold.models.session        import Session
#from manifold.models.linked_account import LinkedAccount
from manifold.models.policy         import Policy

# This is required to create tables
Base.metadata.create_all(engine)

#from manifold.models.field import Field


# Create a session

#Session = sessionmaker()
#Session.configure(bind=engine)

