import logging
from fastapi import APIRouter, Depends
from typing import Optional, List

from dependencies import get_db
from sqlalchemy.exc import IntegrityError
from utilities.exceptions import EntityNotFoundException, ApiException
import schemas
import crud

router = APIRouter(
    prefix="/jobs",
    tags=["jobs"]
)


@router.post("/", response_model=List[schemas.Job], summary="Creates jobs")
async def add_jobs(jobs_create: List[schemas.Job], db = Depends(get_db)):
    """
    Create jobs:

    - **name**: Nmae of the job
    """
    logging.debug("Jobs: Add jobs")

    if len(jobs_create) == 0:
        raise ApiException(status_code=400, code="Invalid request",
                           description="Please specify at least one row!")
    elif len(jobs_create) > 1000:
        raise ApiException(status_code=400, code="Invalid request",
                           description="Please specify up to 1000 rows!")

    jobs = crud.add_jobs(db, jobs_create)
    return jobs


@router.get(
    "/",
    response_model=Optional[List[schemas.JobWithIdentifier]],
    summary="Retrieves all jobs",
    description="Retrieves all available jobs from the API")
async def read_jobs(db = Depends(get_db)):
    logging.debug("jobs: Fetch jobs")
    jobs = crud.get_jobs(db)
    return jobs


@router.get(
    "/{job_id}",
    response_model=Optional[schemas.JobWithIdentifier],
    summary="Retrieve a job by ID",
    description="Retrieves a specific job by ID, if no job matches the filter criteria a 404 error is returned")
async def read_job(job_id: int, db = Depends(get_db)):
    logging.debug("jobs: Fetch job by id")
    job = crud.get_jobs(db, job_id)
    if not job:
        raise EntityNotFoundException(code="Unable to retrieve job",
                                      description=f"Job with the id {job_id} does not exist")
    return job


@router.patch("/{job_id}", response_model=schemas.JobWithIdentifier, summary="Patches a job")
async def update_job(job_id: int, job_update: schemas.Job, db = Depends(get_db)):
    """ 
    Patches a job, this endpoint allows to update single or multiple values of a job

    - **department_name**: Name of the department
    """
    logging.debug("jobs: Update job")

    if len(job_update.dict(exclude_unset=True).keys()) == 0:
        raise ApiException(status_code=400, code="Invalid request",
                           description="Please specify at least one property!")

    job = crud.update_job(db, job_id, job_update)
    if not job:
        raise EntityNotFoundException(
            code="Unable to update job", description=f"job with the id {job_id} does not exist")
    return job


@router.delete("/{job_id}", summary="Deletes a job", description="Deletes a job permanently by ID")
async def job_product(job_id: int, db = Depends(get_db)):
    logging.debug("Jobs: Delete job")
    try:
        job = crud.delete_job(db, job_id)
    except IntegrityError as err:
        raise EntityNotFoundException(code="Unable to delete job, integrity error", 
                                      description=str(err))
    if not job:
        raise EntityNotFoundException(code="Unable to delete job", 
                                      description=f"Job with the id {job_id} does not exist")
