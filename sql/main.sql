Select fs1.title as title_1,
       fs2.title as title_2,
       fsc.Month_Shift,
       fsc.correlation
from fred_series_correlation fsc

left join fred_series fs1
on fs1.id = fsc.Series_1
left join fred_series fs2
on fs2.id = fsc.Series_2

ORDER BY fsc.correlation DESC