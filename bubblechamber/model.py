from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, Boolean
import sqlalchemy as sa


from datetime import datetime

Base = declarative_base()


class ApiKey(Base):
    __tablename__ = 'apikeys'

    key = Column(String(64), primary_key=True)
    email = Column(String(128))
    last_updated = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    def __repr__(self):
        return "<ApiKey(key='%s',)>" % (
                            self.key)

class Container(Base):
    __tablename__ = 'containers'

    container = Column(String(64), primary_key=True)
    created_at = Column(DateTime, default=datetime.now)
    last_updated = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    cpu_resa = Column(Integer, default=0)
    mem_resa = Column(Integer, default=0)

    def __repr__(self):
        return "<Container(container='%s',)>" % (
                            self.container)


class Process(Base):
    __tablename__ = 'processes'

    container = Column(String(64), primary_key=True)
    process_id = Column(Integer, primary_key=True)
    name = Column(String(100))
    exe = Column(String(100))
    arguments = Column(String(256))
    parent_id = Column(Integer)
    is_root = Column(Boolean, default=False)
    last_updated = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    def __repr__(self):
        return "<File(container='%s', name='%s', process='%d',)>" % (
                            self.container, self.name, self.process_id)

class File(Base):
    __tablename__ = 'files'

    container = Column(String(64), primary_key=True)
    process_id = Column(Integer, primary_key=True)
    name = Column(String(256), primary_key=True)
    io_in = Column(Integer, default=0, nullable=False, server_default='0')
    io_out = Column(Integer, default=0, nullable=False, server_default='0')
    io_total = Column(Integer, default=0, nullable=False, server_default='0')
    last_updated = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    def __repr__(self):
        return "<File(container='%s', name='%s', process='%d', io_total='%s')>" % (
                            self.container, self.name, self.process_id, str(self.io_total))
