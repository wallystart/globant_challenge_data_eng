import logging
from fastapi import APIRouter, Depends
from typing import Optional, List

from orm import DatabaseManagerBase
from dependencies import get_db
from utilities.exceptions import EntityNotFoundException, ApiException
import schemas

router = APIRouter(
    prefix="/hired_employees",
    tags=["hired_employees"]
)


@router.post("/", response_model=List[schemas.HiredEmployee], summary="Creates hired_employees")
async def add_hired_employees(hired_employees_create: List[schemas.HiredEmployeesCreate], db: DatabaseManagerBase = Depends(get_db)):
    """
    Create hired_employees:

    - **department_name**: Nmae of the department
    """
    logging.debug("hired_employees: Add hired_employees")

    if len(hired_employees_create) == 0:
        raise ApiException(status_code=400, code="Invalid request",
                           description="Please specify at least one row!")
    elif len(hired_employees_create) > 1000:
        raise ApiException(status_code=400, code="Invalid request",
                           description="Please specify up to 1000 rows!")

    hired_employees = db.add_hired_employees_create(hired_employees_create)
    return hired_employees


@router.get(
    "/",
    response_model=Optional[List[schemas.HiredEmployee]],
    summary="Retrieves all hired_employees",
    description="Retrieves all available hired_employees from the API")
async def read_hired_employees(db: DatabaseManagerBase = Depends(get_db)):
    logging.debug("hired_employees: Fetch hired_employees")
    hired_employees = db.get_hired_employees()
    return hired_employees


@router.get(
    "/{hired_employee_id}",
    response_model=Optional[schemas.HiredEmployee],
    summary="Retrieve a hired_employees by ID",
    description="Retrieves a specific hired_employee by ID, if no hired_employee matches the filter criteria a 404 error is returned")
async def read_hired_employee(hired_employee_id: int, db: DatabaseManagerBase = Depends(get_db)):
    logging.debug("hired_employees: Fetch hired_employee by id")
    hired_employee = db.get_hired_employee(hired_employee_id)
    if not hired_employee:
        raise EntityNotFoundException(code="Unable to retrieve hired_employee",
                                      description=f"hired_employee with the id {hired_employee_id} does not exist")
    return hired_employee


@router.patch("/{hired_employee_id}", response_model=schemas.HiredEmployee, summary="Patches a HiredEmployee")
async def update_hired_employee(hired_employee_id: int, hired_employee_update: schemas.HiredEmployeePartialUpdate, db: DatabaseManagerBase = Depends(get_db)):
    """ 
    Patches a hired_employee, this endpoint allows to update single or multiple values of a hired_employee

    - **department_name**: Name of the department
    """
    logging.debug("hired_employee: Update hired_employee")

    if len(hired_employee_update.dict(exclude_unset=True).keys()) == 0:
        raise ApiException(status_code=400, code="Invalid request",
                           description="Please specify at least one property!")

    hired_employee = db.update_hired_employee(hired_employee_id, hired_employee_update)
    if not hired_employee:
        raise EntityNotFoundException(
            code="Unable to update hired_employee", description=f"hired_employee with the id {hired_employee_id} does not exist")
    return hired_employee


@router.delete("/{hired_employee_id}", summary="Deletes a hired_employee", description="Deletes a hired_employee permanently by ID")
async def hired_employee_product(hired_employee_id: int, db: DatabaseManagerBase = Depends(get_db)):
    logging.debug("Product: Delete department")
    db.delete_hired_employee(hired_employee_id)
