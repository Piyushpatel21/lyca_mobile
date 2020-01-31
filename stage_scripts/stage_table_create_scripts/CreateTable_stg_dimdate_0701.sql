--DROP TABLE uk_rrbs_stg.stg_date;
create table uk_rrbs_stg.stg_date
(
DateNum						varchar(100),
"Date"						varchar(100),
YearMonthNum				varchar(100),
Calendar_Quarter			varchar(100),
MonthNum					varchar(100),
"MonthName"					varchar(100),
MonthShortName				varchar(100),
WeekNum						varchar(100),
DayNumOfYear				varchar(100),
DayNumOfMonth				varchar(100),
DayNumOfWeek				varchar(100),
"DayName"					varchar(100),
DayShortName				varchar(100),
"Quarter"					varchar(100),
YearQuarterNum				varchar(100),
DayNumOfQuarter				varchar(100)
)
diststyle even
sortkey (DateNum);


