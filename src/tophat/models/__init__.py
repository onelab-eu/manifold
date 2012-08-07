from elixir import setup_all, create_all, metadata, Entity, session, drop_all
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from tophat.conf import settings

from tophat.models.platform import Platform
from tophat.models.field import Field

metadata.bind = create_engine("sqlite:///%s?check_same_thread=False" %
        settings.DATABASE_PATH, echo=False, poolclass=StaticPool)

setup_all()
create_all()
