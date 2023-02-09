from database import Base
from sqlalchemy import Column, String, Boolean, Integer


class Departments(Base):
    __table_args__ = {"schema": "dbo"}
    __tablename__ = 'departments'
    id = Column(Integer, primary_key = True, autoincrement = True)
    department = Column(String)


class HiredEmployees(Base):
    __table_args__ = {"schema": "dbo"}
    __tablename__ = 'hired_employees'
    id = Column(Integer, primary_key = True, autoincrement = True)
    name = Column(String)
    datetime = Column(String)
    department_id = Column(Integer)
    job_id = Column(Integer)


class Jobs(Base):
    __table_args__ = {"schema": "dbo"}
    __tablename__ = 'jobs'
    id = Column(Integer, primary_key = True, autoincrement = True)
    job = Column(String)