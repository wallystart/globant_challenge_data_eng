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
    department: Optional[str] = None

    class Config:
        orm_mode = True

        fields = {
            "department": {"description": "Department name"}
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
    name: Optional[str] = None
    datetime: Optional[str] = None
    department_id: Optional[int] = None
    job_id: Optional[int] = None

    class Config:
        orm_mode = True

        fields = {
            "name": {"description": "HiredEmployees name"},
            "datetime": {"description": "Hire datetime in ISO format"},
            "department_id": {"description": "Id of department"},
            "job_id": {"description": "Id of Job"}
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
    job: Optional[str] = None

    class Config:
        orm_mode = True

        fields = {
            "job": {"description": "Job name"}
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
