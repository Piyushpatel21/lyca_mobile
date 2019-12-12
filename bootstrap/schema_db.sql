/*----------------------------------------------------------------
	Topic	:	Schema and Database creation scripts
	Author	: 	Hemanth Pasumarthi
	Domain	:	Redshift
	Version		Description			Date			Change By
	---------------------------------------------------------
	v 0.1		Initial Draft		12/12/2019		Hemanth Pasumarthi
------------------------------------------------------------------*/

/* Script has been tested ready for review  */
--#1 to create Redshift schemas-------------------------------------------------

create schema if not exists stg authorization <redshiftadminname>;
create schema if not exists dm authorization <redshiftadminname>;


--#2 to create Redshift databases-----------------------------------------------

create databse if not exists hemanthdb; 
