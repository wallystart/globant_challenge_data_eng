import logging
from fastapi import APIRouter, Depends
from typing import Optional, List

from dependencies import get_db
from sqlalchemy.exc import IntegrityError
from utilities.exceptions import EntityNotFoundException, ApiException
import schemas
import crud

router = APIRouter(
    prefix="/departments",
    tags=["departments"]
)


@router.post("/", response_model=List[schemas.Department], summary="Insert departments")
async def add_departments(departments_insert: List[schemas.Department], db = Depends(get_db)):
    """
    Create departments:

    - **name**: Nmae of the department
    """
    logging.debug("Departments: Add departments")

    if len(departments_insert) == 0:
        raise ApiException(status_code=400, code="Invalid request",
                           description="Please specify at least one row!")
    elif len(departments_insert) > 1000:
        raise ApiException(status_code=400, code="Invalid request",
                           description="Please specify up to 1000 rows!")

    departments = crud.add_departments(db, departments_insert)
    return departments


@router.get(
    "/",
    response_model=Optional[List[schemas.DepartmentWithIdentifier]],
    summary="Retrieves all departments",
    description="Retrieves all available departments from the API")
async def read_departments(db = Depends(get_db)):
    logging.debug("Department: Fetch departments")
    departments = crud.get_departments(db)
    return departments


@router.get(
    "/{departments_id}",
    response_model=Optional[schemas.DepartmentWithIdentifier],
    summary="Retrieve a department by ID",
    description="Retrieves a specific department by ID, if no department matches the filter criteria a 404 error is returned")
async def read_department(department_id: int, db = Depends(get_db)):
    logging.debug("Department: Fetch department by id")
    department = crud.get_departments(db, department_id)
    if not department:
        raise EntityNotFoundException(code="Unable to retrieve department",
                                      description=f"Department with the id {department_id} does not exist")
    return department


@router.patch("/{department_id}", response_model=schemas.DepartmentWithIdentifier, summary="Patches a product")
async def update_department(department_id: int, department_update: schemas.Department, db = Depends(get_db)):
    """ 
    Patches a department, this endpoint allows to update single or multiple values of a department

    - **department_name**: Name of the department
    """
    logging.debug("Department: Update department")

    if len(department_update.dict(exclude_unset=True).keys()) == 0:
        raise ApiException(status_code=400, code="Invalid request",
                           description="Please specify at least one property!")

    department = crud.update_department(db, department_id, department_update)
    if not department:
        raise EntityNotFoundException(
            code="Unable to update department", description=f"Department with the id {department_id} does not exist")
    return department


@router.delete("/{department_id}", summary="Deletes a department", description="Deletes a department permanently by ID")
async def department_product(department_id: int, db = Depends(get_db)):
    logging.debug("Product: Delete department")
    try:
        department = crud.delete_department(db, department_id)
    except IntegrityError as err:
        raise EntityNotFoundException(code="Unable to delete department, integrity error", 
                                      description=str(err))
    if not department:
        raise EntityNotFoundException(code="Unable to delete department", 
                                      description=f"Department with the id {department_id} does not exist")
