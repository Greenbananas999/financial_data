Select B.date, B.value as "Corn", GAS.value as "Gas"
from fred_series_observations B
join (select * from fred_series_observations where series_id = "APU000074714") GAS
on GAS.date = B.date
where B.series_id = "WPS012202"
and B.value is not NULL
and GAS.value is not NULL
;

Select * from fred_series
;