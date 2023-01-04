(select
measurement_source_id,hit_date,total_imps,
lead(total_imps,1) over (partition by measurement_source_id order by hit_date desc) as previous_imps,
lead(total_imps,2) over (partition by measurement_source_id order by hit_date desc) as previous_yr_imps,
round((((total_imps - previous_imps)/ previous_imps)*100),2) as DOD_percent,
round((((total_imps - previous_yr_imps)/ previous_yr_imps)*100),2) as YOY
from
(
  ( select
  CASE
  WHEN measurement_source_id = 19 THEN 'Spotify'
  ELSE 'UNKNOWN'
  END as measurement_source_id,
  hit_date,
  sum(imps) as total_imps
  FROM cds_prod.analytics.AGG_PARTNER_MEASURED_VIEWABILITY
  where measurement_source_id = 19 and hit_date in ($max_date_1,dateadd(year, -1, $max_date_1),dateadd(day, -1, $max_date_1))
  group by hit_date,measurement_source_id
  order by hit_date desc
) )limit 1)
union
(select
measurement_source_id,hit_date,total_imps,
lead(total_imps,1) over (partition by measurement_source_id order by hit_date desc) as previous_imps,
lead(total_imps,2) over (partition by measurement_source_id order by hit_date desc) as previous_yr_imps,
round((((total_imps - previous_imps)/ previous_imps)*100),2) as DOD_percent,
round((((total_imps - previous_yr_imps)/ previous_yr_imps)*100),2) as YOY
from
(
  ( select
  CASE
  WHEN measurement_source_id = 6 THEN 'Twitter'
  ELSE 'UNKNOWN'
  END as measurement_source_id,
  hit_date,
  sum(imps) as total_imps
  FROM cds_prod.analytics.AGG_PARTNER_MEASURED_VIEWABILITY
  where measurement_source_id = 6 and hit_date in ($max_date_2,dateadd(year, -1, $max_date_2),dateadd(day, -1, $max_date_2))
  group by hit_date,measurement_source_id
  order by hit_date desc
) )limit 1)
union
(select
measurement_source_id,hit_date,total_imps,
lead(total_imps,1) over (partition by measurement_source_id order by hit_date desc) as previous_imps,
lead(total_imps,2) over (partition by measurement_source_id order by hit_date desc) as previous_yr_imps,
round((((total_imps - previous_imps)/ previous_imps)*100),2) as DOD_percent,
round((((total_imps - previous_yr_imps)/ previous_yr_imps)*100),2) as YOY
from
(
  ( select
  CASE
  WHEN measurement_source_id = 11 THEN 'Pinterest'
  ELSE 'UNKNOWN'
  END as measurement_source_id,
  hit_date,
  sum(imps) as total_imps
  FROM cds_prod.analytics.AGG_PARTNER_MEASURED_VIEWABILITY
  where measurement_source_id = 11 and hit_date in ($max_date_1,dateadd(year, -1, $max_date_1),dateadd(day, -1, $max_date_1))
  group by hit_date,measurement_source_id
  order by hit_date desc
) )limit 1)
union
(select
measurement_source_id,hit_date,total_imps,
lead(total_imps,1) over (partition by measurement_source_id order by hit_date desc) as previous_imps,
lead(total_imps,2) over (partition by measurement_source_id order by hit_date desc) as previous_yr_imps,
round((((total_imps - previous_imps)/ previous_imps)*100),2) as DOD_percent,
round((((total_imps - previous_yr_imps)/ previous_yr_imps)*100),2) as YOY
from
(
  ( select
  CASE
  WHEN measurement_source_id = 10 THEN 'Snapchat'
  ELSE 'UNKNOWN'
  END as measurement_source_id,
  hit_date,
  sum(imps) as total_imps
  FROM cds_prod.analytics.AGG_PARTNER_MEASURED_VIEWABILITY
  where measurement_source_id = 10 and hit_date in ($max_date_1,dateadd(year, -1, $max_date_1),dateadd(day, -1, $max_date_1))
  group by hit_date,measurement_source_id
  order by hit_date desc
) )limit 1)
union
(select
measurement_source_id,hit_date,total_imps,
lead(total_imps,1) over (partition by measurement_source_id order by hit_date desc) as previous_imps,
lead(total_imps,2) over (partition by measurement_source_id order by hit_date desc) as previous_yr_imps,
round((((total_imps - previous_imps)/ previous_imps)*100),2) as DOD_percent,
round((((total_imps - previous_yr_imps)/ previous_yr_imps)*100),2) as YOY
from
(
  ( select
  CASE
  WHEN measurement_source_id = 16 THEN 'Linkedin'
  ELSE 'UNKNOWN'
  END as measurement_source_id,
  hit_date,
  sum(imps) as total_imps
  FROM cds_prod.analytics.AGG_PARTNER_MEASURED_VIEWABILITY
  where measurement_source_id = 16 and hit_date in ($max_date_1,dateadd(year, -1, $max_date_1),dateadd(day, -1, $max_date_1))
  group by hit_date,measurement_source_id
  order by hit_date desc
) )limit 1)
union
(select
measurement_source_id,hit_date,total_imps,
lead(total_imps,1) over (partition by measurement_source_id order by hit_date desc) as previous_imps,
lead(total_imps,2) over (partition by measurement_source_id order by hit_date desc) as previous_yr_imps,
round((((total_imps - previous_imps)/ previous_imps)*100),2) as DOD_percent,
round((((total_imps - previous_yr_imps)/ previous_yr_imps)*100),2) as YOY
from
(
  ( select
  CASE
  WHEN measurement_source_id = 12 THEN 'Youtube - Google Ads'
  ELSE 'UNKNOWN'
  END as measurement_source_id,
  hit_date,
  sum(imps) as total_imps
  FROM cds_prod.analytics.AGG_PARTNER_MEASURED_VIEWABILITY
  where measurement_source_id = 12 and hit_date in ($max_date_1,dateadd(year, -1, $max_date_1),dateadd(day, -1, $max_date_1))
  group by hit_date,measurement_source_id
  order by hit_date desc
))limit 1)
union
(select
measurement_source_id,hit_date,total_imps,
lead(total_imps,1) over (partition by measurement_source_id order by hit_date desc) as previous_imps,
lead(total_imps,2) over (partition by measurement_source_id order by hit_date desc) as previous_yr_imps,
round((((total_imps - previous_imps)/ previous_imps)*100),2) as DOD_percent,
round((((total_imps - previous_yr_imps)/ previous_yr_imps)*100),2) as YOY
from
(
  ( select
  CASE
  WHEN measurement_source_id = 13 THEN 'Youtube - DV 360'
  ELSE 'UNKNOWN'
  END as measurement_source_id,
  hit_date,
  sum(imps) as total_imps
  FROM cds_prod.analytics.AGG_PARTNER_MEASURED_VIEWABILITY
  where measurement_source_id = 13 and hit_date in ($max_date_1,dateadd(year, -1, $max_date_1),dateadd(day, -1, $max_date_1))
  group by hit_date,measurement_source_id
  order by hit_date desc
 
)) limit 1)
union
(select
measurement_source_id,hit_date,total_imps,
lead(total_imps,1) over (partition by measurement_source_id order by hit_date desc) as previous_imps,
lead(total_imps,2) over (partition by measurement_source_id order by hit_date desc) as previous_yr_imps,
round((((total_imps - previous_imps)/ previous_imps)*100),2) as DOD_percent,
round((((total_imps - previous_yr_imps)/ previous_yr_imps)*100),2) as YOY
from
(
  ( select
  CASE
  WHEN measurement_source_id = 14 THEN 'Youtube - Reserve'
  ELSE 'UNKNOWN'
  END as measurement_source_id,
  hit_date,
  sum(imps) as total_imps
  FROM cds_prod.analytics.AGG_PARTNER_MEASURED_VIEWABILITY
  where measurement_source_id = 14 and hit_date in ($max_date_1,dateadd(year, -1, $max_date_1),dateadd(day, -1, $max_date_1))
  group by hit_date,measurement_source_id
  order by hit_date desc
) )limit 1)
union
(select
measurement_source_id,hit_date,total_imps,
lead(total_imps,1) over (partition by measurement_source_id order by hit_date desc) as previous_imps,
lead(total_imps,2) over (partition by measurement_source_id order by hit_date desc) as previous_yr_imps,
round((((total_imps - previous_imps)/ previous_imps)*100),2) as DOD_percent,
round((((total_imps - previous_yr_imps)/ previous_yr_imps)*100),2) as YOY
from
(
  ( select
  CASE
  WHEN measurement_source_id = 15 THEN 'Youtube - Partner Sold'
  ELSE 'UNKNOWN'
  END as measurement_source_id,
  hit_date,
  sum(imps) as total_imps
  FROM cds_prod.analytics.AGG_PARTNER_MEASURED_VIEWABILITY
  where measurement_source_id = 15 and hit_date in ($max_date_1,dateadd(year, -1, $max_date_1),dateadd(day, -1, $max_date_1))
  group by hit_date,measurement_source_id
  order by hit_date desc
) )limit 1)
union
(select
measurement_source_id,hit_date,total_imps,
lead(total_imps,1) over (partition by measurement_source_id order by hit_date desc) as previous_imps,
lead(total_imps,2) over (partition by measurement_source_id order by hit_date desc) as previous_yr_imps,
round((((total_imps - previous_imps)/ previous_imps)*100),2) as DOD_percent,
round((((total_imps - previous_yr_imps)/ previous_yr_imps)*100),2) as YOY
from
(
  ( select
  CASE
  WHEN measurement_source_id = 4 THEN 'Facebook'
  ELSE 'UNKNOWN'
  END as measurement_source_id,
  hit_date,
  sum(imps) as total_imps
  FROM cds_prod.analytics.AGG_FACEBOOK_VIEWABILITY
  where measurement_source_id = 4 and hit_date in ($max_date_1,dateadd(year, -1, $max_date_1),dateadd(day, -1, $max_date_1))
  group by hit_date,measurement_source_id
  order by hit_date desc
) )limit 1)
union
(select
measurement_source_id,hit_date,total_imps,
lead(total_imps,1) over (partition by measurement_source_id order by hit_date desc) as previous_imps,
lead(total_imps,2) over (partition by measurement_source_id order by hit_date desc) as previous_yr_imps,
round((((total_imps - previous_imps)/ previous_imps)*100),2) as DOD_percent,
round((((total_imps - previous_yr_imps)/ previous_yr_imps)*100),2) as YOY
from
(
  ( select
  CASE
  WHEN measurement_source_id = 5 THEN 'Yahoo'
  ELSE 'UNKNOWN'
  END as measurement_source_id,
  hit_date,
  sum(imps) as total_imps
  FROM cds_prod.analytics.AGG_PARTNER_MEASURED_VIEWABILITY
  where measurement_source_id = 5 and hit_date in ($max_date_1,dateadd(year, -1, $max_date_1),dateadd(day, -1, $max_date_1))
  group by hit_date,measurement_source_id
  order by hit_date desc
) )limit 1);