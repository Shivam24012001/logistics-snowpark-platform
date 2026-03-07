-- LOGISTICS PLATFORM - INFRA SETUP


--1 Warehouse 
create or replace warehouse ingest_wh
warehouse_size=SMALL
auto_suspend=60
auto_resume=True
initially_suspended=True;

create or replace warehouse transform_wh
warehouse_size=small
auto_suspend=60
auto_resume=True
initially_suspended=True;

--Database

create or replace database logistics_db;

create or replace schema logistics_db.bronze;
create or replace schema logistics_db.silver;

create or replace schema logistics_db.gold;
create or replace schema logistics_db.audit;

use warehouse ingest_wh;
use database logistics_db;

use schema logistics_db.bronze;




