-- dbo schema definition

CREATE SCHEMA IF NOT EXISTS dbo;

-- dbo.departments definition

CREATE TABLE IF NOT EXISTS dbo."departments" (
	"id" int8 NOT NULL GENERATED ALWAYS AS IDENTITY,
	"department" varchar null,
	CONSTRAINT departments_pk PRIMARY KEY (id)
);

-- dbo.jobs definition

CREATE TABLE IF NOT EXISTS dbo.jobs (
	id int8 NOT NULL GENERATED ALWAYS AS IDENTITY,
	"job" varchar null,
	CONSTRAINT jobs_pk PRIMARY KEY (id)
);

-- dbo.hired_employees definition

CREATE TABLE dbo.hired_employees (
	id int8 NOT NULL GENERATED ALWAYS AS IDENTITY,
	"name" varchar NULL,
	"datetime" varchar NULL,
	department_id int8 NULL,
	job_id int8 NULL,
	CONSTRAINT hired_employees_pk PRIMARY KEY (id),
	CONSTRAINT fk_hired_employees_departments FOREIGN KEY(department_id) REFERENCES dbo.departments(id),
	CONSTRAINT fk_hired_employees_jobs FOREIGN KEY(job_id) REFERENCES dbo.jobs(id)
);