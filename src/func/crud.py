from typing import Optional, List, Union
from sqlalchemy.orm import Session
import schemas
import models


def add_departments(db: Session, departments: List[schemas.Department]) -> List[schemas.Department]:
    # TODO: Automatically return generated ids, keep testing with bulk_save_mappings()
    # _departments = [models.Departments(**department.dict()) for department in departments]
    # db.bulk_insert_mappings(models.Departments, _departments)
    # db.commit()
    # return [{"id": department.id} for department in _departments]
    #
    _departments = [models.Departments(**department.dict()) for department in departments]
    db.bulk_save_objects(_departments)
    db.commit()
    return _departments

def get_departments(db: Session, id: int = None) -> Union[Optional[List[schemas.Department]], Optional[schemas.Department]]:
    result = db.query(models.Departments).filter(models.Departments.id == id).first() if id else db.query(models.Departments).all()
    return result

def update_department(db: Session, id: int, department: schemas.Department) -> Optional[schemas.Department]:
    _department = get_departments(db, id)
    if _department:
        for key, value in department.dict().items():
            setattr(_department, key, value)
            db.commit()
        db.add(_department)
        db.commit()
        return _department
    return None

def delete_department(db: Session, id: int):
    _department = get_departments(db, id)
    if _department:
        db.delete(_department)
        db.commit()
        return _department

def add_hired_employees(db: Session, hired_employees: List[schemas.HiredEmployee]) -> List[schemas.HiredEmployee]:
    # TODO: Automatically return generated ids, keep testing with bulk_save_mappings()
    _hired_employees = [models.HiredEmployees(**hired_employee.dict()) for hired_employee in hired_employees]
    db.bulk_save_objects(_hired_employees)
    db.commit()
    return _hired_employees

def get_hired_employees(db: Session, id: int = None) -> Union[Optional[List[schemas.HiredEmployee]], Optional[schemas.HiredEmployee]]:
    result = db.query(models.HiredEmployees).filter(models.HiredEmployees.id == id).first() if id else db.query(models.HiredEmployees).all()
    return result

def update_hired_employee(db: Session, id: int, hired_employee: schemas.HiredEmployee) -> Optional[schemas.HiredEmployeeWithIdentifier]:
    _hired_employee = get_hired_employees(db, id)
    if _hired_employee:
        for key, value in hired_employee.dict().items():
            setattr(_hired_employee, key, value)
            db.commit()
        db.add(_hired_employee)
        db.commit()
        return _hired_employee
    return None

def delete_hired_employee(db: Session, id: int):
    _hired_employee = get_hired_employees(db, id)
    if _hired_employee:
        db.delete(_hired_employee)
        db.commit()
        return _hired_employee

def add_jobs(db: Session, jobs: List[schemas.Job]) -> List[schemas.Job]:
    # TODO: Automatically return generated ids, keep testing with bulk_save_mappings()
    _jobs = [models.Jobs(**job.dict()) for job in jobs]
    db.bulk_save_objects(_jobs)
    db.commit()
    return _jobs

def get_jobs(db: Session, id: int = None) -> Union[Optional[List[schemas.Job]], Optional[schemas.Job]]:
    result = db.query(models.Jobs).filter(models.Jobs.id == id).first() if id else db.query(models.Jobs).all()
    return result

def update_job(db: Session, id: int, job: schemas.HiredEmployee) -> Optional[schemas.HiredEmployee]:
    _job = get_jobs(db, id)
    if _job:
        for key, value in job.dict().items():
            setattr(_job, key, value)
            db.commit()
        db.add(_job)
        db.commit()
        return _job
    return None

def delete_job(db: Session, id: int):
    _hired_employee = get_jobs(db, id)
    if _hired_employee:
        db.delete(_hired_employee)
        db.commit()
        return _hired_employee