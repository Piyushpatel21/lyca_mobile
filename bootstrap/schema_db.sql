/* Script has been tested ready for review  */
--#1 to create Redshift schemas-------------------------------------------------

create schema if not exists stg authorization <redshiftadminname>;
create schema if not exists dm authorization <redshiftadminname>;


--#2 to create Redshift databases-----------------------------------------------

create databse if not exists hemanthdb; 
