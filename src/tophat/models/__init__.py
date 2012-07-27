from elixir import setup_all, create_all, metadata, Entity, session, drop_all
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from tophat.conf import settings
#from tophat.models.node import Node
#from tophat.models.stream import Stream
#from tophat.models.ticket import Ticket
#from tophat.models.event import Event

metadata.bind = create_engine("sqlite:///%s?check_same_thread=False" %
        settings.DATABASE_PATH, echo=False, poolclass=StaticPool)

setup_all()
create_all()
