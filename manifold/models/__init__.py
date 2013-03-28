
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy.orm import sessionmaker

# exceptions.Exception: Error during local request: (ProgrammingError) SQLite objects created in a thread can only be used in that same thread.The object was created in thread id 139885302400768 and this is thread id 139885208180480 'SELECT platform.platform AS platform_platform \nFROM platform' [immutabledict({})]
#engine = create_engine('sqlite:///:memory:?check_same_thread=False', echo=False)
engine = create_engine('sqlite:////var/myslice/db.sqlite?check_same_thread=False', echo=False)

# OLD from tophat.conf import settings
# OLD from sqlalchemy.pool import StaticPool
# OLD metadata.bind = create_engine("sqlite:///%s?check_same_thread=False" %
# OLD        settings.DATABASE_PATH, echo=False, poolclass=StaticPool)


class Base(object):
    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()

    # By default, we do not filter the content of the table according to the
    # authenticated user
    user_filter = False

    __mapper_args__= {'always_refresh': True}

    #id =  Column(Integer, primary_key=True)

    #def to_dict(self):
    #    return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    @staticmethod
    def process_filters(filters):
        return filters

    @staticmethod
    def process_params(params, filters, users):
        return params

Base = declarative_base(cls=Base)

Session = sessionmaker(bind=engine)
db = Session()

# Models
from manifold.models.platform   import Platform
from manifold.models.user       import User
from manifold.models.account    import Account
from manifold.models.session    import Session

Base.metadata.create_all(engine)

#from manifold.models.field import Field


# Create a session

#Session = sessionmaker()
#Session.configure(bind=engine)

