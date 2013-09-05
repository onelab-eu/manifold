from sqlalchemy.ext.declarative import declared_attr, declarative_base

class Base(object):
    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()

    # By default, we do not filter the content of the table according to the
    # authenticated user
    restrict_to_self = False

    __mapper_args__= {'always_refresh': True}

    #id =  Column(Integer, primary_key=True)

    #def to_dict(self):
    #    return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    @classmethod
    def process_filters(cls, filters):
        return filters

    @classmethod
    def process_params(cls, params, filters, user):
        return params

Base = declarative_base(cls = Base)
