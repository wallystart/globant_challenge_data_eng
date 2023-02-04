import logging
from fastapi import APIRouter, Depends
from typing import Optional, List

from orm import DatabaseManagerBase
from dependencies import get_db
from utilities.exceptions import EntityNotFoundException, ApiException
import schemas

router = APIRouter(
    prefix="/departments",
    tags=["departments"]
)


@router.post("/", response_model=List[schemas.Department], summary="Creates departments")
async def add_departments(departments_create: List[schemas.DepartmentCreate], db: DatabaseManagerBase = Depends(get_db)):
    """
    Create a department:

    - **department_name**: Nmae of the department
    """
    logging.debug("Departments: Add departments")

    if len(departments_create) == 0:
        raise ApiException(status_code=400, code="Invalid request",
                           description="Please specify at least one row!")
    elif len(departments_create) > 1000:
        raise ApiException(status_code=400, code="Invalid request",
                           description="Please specify up to 1000 rows!")

    departments = db.add_departments(departments_create)
    return departments


@router.get(
    "/",
    response_model=Optional[List[schemas.Department]],
    summary="Retrieves all departments",
    description="Retrieves all available departments from the API")
async def read_departments(db: DatabaseManagerBase = Depends(get_db)):
    logging.debug("Department: Fetch departments")
    departments = db.get_departments()
    return departments


@router.get(
    "/{departments_id}",
    response_model=Optional[schemas.Department],
    summary="Retrieve a department by ID",
    description="Retrieves a specific department by ID, if no department matches the filter criteria a 404 error is returned")
async def read_department(department_id: int, db: DatabaseManagerBase = Depends(get_db)):
    logging.debug("Department: Fetch department by id")
    department = db.get_department(department_id)
    if not department:
        raise EntityNotFoundException(code="Unable to retrieve department",
                                      description=f"Department with the id {department_id} does not exist")
    return department


@router.patch("/{department_id}", response_model=schemas.Department, summary="Patches a product")
async def update_department(department_id: int, department_update: schemas.DepartmentPartialUpdate, db: DatabaseManagerBase = Depends(get_db)):
    """ 
    Patches a department, this endpoint allows to update single or multiple values of a department

    - **department_name**: Name of the department
    """
    logging.debug("Department: Update department")

    if len(department_update.dict(exclude_unset=True).keys()) == 0:
        raise ApiException(status_code=400, code="Invalid request",
                           description="Please specify at least one property!")

    department = db.update_department(department_id, department_update)
    if not department:
        raise EntityNotFoundException(
            code="Unable to update department", description=f"Department with the id {department_id} does not exist")
    return department


@router.delete("/{department_id}", summary="Deletes a department", description="Deletes a department permanently by ID")
async def department_product(department_id: int, db: DatabaseManagerBase = Depends(get_db)):
    logging.debug("Product: Delete department")
    db.delete_department(department_id)
