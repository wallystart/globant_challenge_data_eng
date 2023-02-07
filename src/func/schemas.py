from pydantic import BaseModel, validator
from typing import Optional, List

"""
Contains all schemas alias domain models of the application.
For domain modelling, the library pydantic is used.
Pydantic allows to create versatile domain models and ensures data integrity and much more.
"""


class Department(BaseModel):
    """
    Department schema
    """
    name: str

    class Config:
        orm_mode = True

        fields = {
            "name": {"description": "Department name"}
        }

class DepartmentWithIdentifier(Department):
    """
    GenericIdentifier schema
    """
    id: int

    class Config:
        orm_mode = True

        fields = {
            "id": {"description": "GenericIdentifier value"}
        }


class HiredEmployee(BaseModel):
    """
    HiredEmployees schema
    """
    name: str

    class Config:
        orm_mode = True

        fields = {
            "name": {"description": "HiredEmployees name"}
        }

class HiredEmployeeWithIdentifier(HiredEmployee):
    """
    GenericIdentifier schema
    """
    id: int

    class Config:
        orm_mode = True

        fields = {
            "id": {"description": "GenericIdentifier value"}
        }


class Job(BaseModel):
    """
    Job schema
    """
    name: str

    class Config:
        orm_mode = True

        fields = {
            "name": {"description": "Job name"}
        }

class JobWithIdentifier(Job):
    """
    Job schema
    """
    id: int

    class Config:
        orm_mode = True

        fields = {
            "id": {"description": "Job value"}
        }
