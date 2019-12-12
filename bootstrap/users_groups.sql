/*----------------------------------------------------------------
	Topic	:	Users and Groups creation scripts
	Author	: 	Hemanth Pasumarthi
	Domain	:	Redshift
	Version		Description			Date			Change By
	---------------------------------------------------------
	v 0.1		Initial Draft		12/12/2019		Hemanth Pasumarthi
------------------------------------------------------------------*/

/* Script has been tested ready for review  */
--#1 to create Redshift local groups for Cloudwick and Lycamobile teams---------

create group cloudwickgroup;
create group lycagroup;


--#2 to create Redshift local users with passwords------------------------------

create user <username> password '<password>' in group cloudwickgroup;


--#3 to Grant permission to the schemas ----------------------------------------

grant all on schema stg to group cloudwickgroup;
grant all on schema dm to group cloudwickgroup;

--#4 to Grant permission to the all databases ----------------------------------

grant all on DATABASE ukdev to group cloudwickgroup;

--#5 to Grant super user permission to make a user -----------------------------

alter user <username> createuser;