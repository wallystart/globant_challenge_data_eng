from database import Base
from sqlalchemy import Column, String, Boolean, Integer


class Departments(Base):
    __table_args__ = {"schema": "dbo"}
    __tablename__ = 'departments'
    id = Column(Integer, primary_key = True, autoincrement = True)
    name = Column(String)


class HiredEmployees(Base):
    __table_args__ = {"schema": "dbo"}
    __tablename__ = 'hired_employees'
    id = Column(Integer, primary_key = True, autoincrement = True)
    name = Column(String)


class Jobs(Base):
    __table_args__ = {"schema": "dbo"}
    __tablename__ = 'jobs'
    id = Column(Integer, primary_key = True, autoincrement = True)
    name = Column(String)