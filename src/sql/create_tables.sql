-- dbo schema definition

CREATE SCHEMA IF NOT EXISTS dbo AUTHORIZATION globant_super_admin;

-- dbo.departments definition

CREATE TABLE IF NOT EXISTS dbo.departments (
	id int8 NOT NULL GENERATED ALWAYS AS IDENTITY,
	"name" varchar NULL
);

-- dbo.hired_employees definition

CREATE TABLE IF NOT EXISTS dbo.hired_employees (
	id int8 NOT NULL GENERATED ALWAYS AS IDENTITY,
	"name" varchar NULL
);

-- dbo.jobs definition

CREATE TABLE IF NOT EXISTS dbo.jobs (
	id int8 NOT NULL GENERATED ALWAYS AS IDENTITY,
	"name" varchar NULL
);