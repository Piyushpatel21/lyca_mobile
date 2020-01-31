--drop table uk_rrbs_dm.dim_date;

create table uk_rrbs_dm.dim_date
(
DateNum	int,
"Date"	date ,
YearMonthNum	int,
Calendar_Quarter	varchar(20),
MonthNum	smallint,
"MonthName"	varchar(20),
MonthShortName	varchar(10),
WeekNum	smallint,
DayNumOfYear	smallint,
DayNumOfMonth	smallint,
DayNumOfWeek	smallint,
"DayName"	varchar(10),
DayShortName	varchar(10),
"Quarter"	smallint ,
YearQuarterNum	int,
DayNumOfQuarter	smallint
)
diststyle all
sortkey (DateNum);

insert into uk_rrbs_dm.dim_date
(
DateNum			,
"Date"			,
YearMonthNum	,
Calendar_Quarter,
MonthNum		,
"MonthName"		,
MonthShortName	,
WeekNum			,
DayNumOfYear	,
DayNumOfMonth	,
DayNumOfWeek	,
"DayName"		,
DayShortName	,
"Quarter"		,
YearQuarterNum	,
DayNumOfQuarter	
)
select
datenum::int,
to_date("Date",'mm/dd/yyyy'),
YearMonthNum	::int,
Calendar_Quarter	::varchar(20),
MonthNum	::smallint,
"MonthName"	::varchar(20),
MonthShortName	::varchar(10),
WeekNum	::smallint,
DayNumOfYear	::smallint,
DayNumOfMonth	::smallint,
DayNumOfWeek	::smallint,
"DayName"	::varchar(10),
DayShortName	::varchar(10),
"Quarter"	::smallint ,
YearQuarterNum	::int,
DayNumOfQuarter	::smallint
from 
uk_rrbs_stg.stg_date
;