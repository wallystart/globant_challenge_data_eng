from abc import ABC, abstractmethod
from typing import Optional, List

import schemas


class DatabaseManagerBase(ABC):
    """
    Example implementation of a database manager.
    In a productive application, SQLAlchemy or another ORM framework could be used here (depending on the database used). 
    This is a very simplified database manager for demonstration purposes.
    """

    @abstractmethod
    def add_departments(self, department: List[schemas.DepartmentCreate]) -> List[schemas.Department]:
        """
        Adds a department to the database
        Args:
            department (schemas.DepartmentCreate): Department to be added

        Returns:
            schemas.Department: Inserted department
        """
        ...

    @abstractmethod
    def get_departments(self) -> Optional[List[schemas.Department]]:
        """
        Returns all departments from the database
        Returns:
            Optional[List[schemas.Department]]: List of departments
        """
        ...

    @abstractmethod
    def get_department(self, id: int) -> Optional[schemas.Department]:
        """
        Returns a specific department by id
        Args:
            id (int): Id of the department

        Returns:
            Optional[schemas.Department]: Returns the specified department
        """
        ...

    @abstractmethod
    def update_department(self, department_id: int, department: schemas.DepartmentPartialUpdate) -> schemas.Department:
        """
        Updates a department
        Args:
            department_id (int): Department ID of the department to be updated
            department (schemas.Department): Department to update

        Returns:
            schemas.Department: Updated department
        """
        ...

    @abstractmethod
    def delete_department(self, department_id: int) -> None:
        """
        Deletes a department by id
        Args:
            id (int): Id of the to be deleted department
        """
        ...


class FakeDataBaseManager(DatabaseManagerBase):

    def __init__(self) -> None:
        super().__init__()

        self._departments = [
            schemas.Department(
                id=1,
                name="Product Management"
            ),
            schemas.Department(
                id=2,
                name="Sales"
            )
        ]

    def add_departments(self, departments: List[schemas.DepartmentCreate]) -> List[schemas.Department]:
        # Normally, this step would be handled by the database
        idx = max([p.id for p in self._departments]) + 1

        departments_insert = []
        for department in departments:
            department_insert = schemas.Department(id=idx, **department.dict())
            self._departments.append(department_insert)
            departments_insert.append(department_insert)
            idx += 1

        return departments_insert

    def get_departments(self) -> Optional[List[schemas.Department]]:
        return self._departments

    def get_department(self, id: int) -> Optional[schemas.Department]:
        return next(iter([p for p in self._departments if p.id == id]), None)

    def update_department(self, department_id: int, department: schemas.DepartmentPartialUpdate) -> schemas.Department:
        for idx, p in enumerate(self._departments):
            if p.id == department_id:
                db_department = self._departments[idx]
                update_data = department.dict(exclude_unset=True)
                updated_department = db_department.copy(update=update_data)
                self._departments[idx] = updated_department
                return updated_department
        return None
    
    def delete_department(self, department_id: int) -> None:
        for p in self._departments:
            if p.id == department_id:
                department_del = p
                break
        
        if department_del:
            self._departments.remove(department_del)
