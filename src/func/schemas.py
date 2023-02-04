from pydantic import BaseModel, validator
from typing import Optional, List

"""
Contains all schemas alias domain models of the application.
For domain modelling, the library pydantic is used.
Pydantic allows to create versatile domain models and ensures data integrity and much more.
"""


class DepartmentBase(BaseModel):
    """
    Department base schema
    """
    name: str

    class Config:
        fields = {
            "name": {"description": "Department name"}
        }
        

class DepartmentCreate(DepartmentBase):
    """
    Department create schema
    """
    name: str


class DepartmentPartialUpdate(DepartmentBase):
    """
    Department update schema
    """
    ...


class Department(DepartmentBase):
    """
    Department schema, database representation
    """
    id: int

    class Config:
        fields = {
            "id": {"description": "Unique ID of the department "},
        }
