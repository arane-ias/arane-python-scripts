select src, utcdate, imps_count , previous_day_count,
round((CAST((imps_count -previous_day_count) as double ) / previous_day_count *100),2) 
as DOD_percent 
from 
(
    ( select src, utcdate, imps_count, lead(imps_count,1 ) over (partition by src order by utcdate desc ) as previous_day_count 
    from 
    (
        select 'snapchat' as src, utcdate , count(distinct impressionid) as imps_count 
        from snapchat  where type = 'impression' and utcdate in ('2022-12-12','2022-12-11') group by utcdate) 
        order by utcdate desc limit 1
    ) 
    
    UNION ALL 
    
    ( select src, utcdate, imps_count, lead(imps_count,1 ) over (partition by src order by utcdate desc ) as previous_day_count 
    from 
    (
        select 'pinterest' as src, utcdate , count(distinct impressionid) as imps_count from pinterest  
        where type = 'impression' and utcdate in ('2022-12-12','2022-12-11') group by utcdate) order by utcdate desc limit 1
    ) 
    
    UNION ALL 
        
    ( select src, utcdate, imps_count, lead(imps_count,1 ) over (partition by src order by utcdate desc ) as previous_day_count 
    from 
    (
        select 'linkedin' as src, utcdate , count(distinct impressionid) as imps_count from linkedin  
        where type = 'impression' and utcdate in ('2022-12-12','2022-12-11') group by utcdate) order by utcdate desc limit 1
    ) 
    
    UNION ALL 
    ( select src, utcdate, imps_count, lead(imps_count,1 ) over (partition by src order by utcdate desc ) as previous_day_count 
    from 
    (
        select 'spotify' as src, utcdate , count(distinct impressionid) as imps_count from spotify  where type = 'impression' 
        and utcdate in ('2022-12-12','2022-12-11') group by utcdate) 
        order by utcdate desc limit 1
    ) 
    
    UNION ALL 
    ( select src, utcdate, imps_count, lead(imps_count,1 ) over (partition by src order by utcdate desc ) as previous_day_count 
    from 
    (
        select 'yahoo' as src, utcdate , count(distinct impressionid) as imps_count from yahoo  where type = 'impression' 
        and utcdate in ('2022-12-12','2022-12-11') group by utcdate) 
        order by utcdate desc limit 1
    )

    UNION ALL 
    ( select src,utcdate,imps_count,previous_day_count, round((CAST((imps_count -previous_day_count) as double ) / previous_day_count *100),2) as DOD_percent
    from 
    (
        select src,utcdate,imps_count,lead(imps_count,1 ) over (order by utcdate desc) as previous_day_count
        from 
        (
            select 'facebook' as src, count(*) as imps_count,utcdate from "partner_raw"."facebook_parquet"
            where utcdate in ('2022-12-11','2022-12-10') and facebook_event_type in ('fb_init', 'ig_init', 'an_init') 
            and not ( facebook_event_type in ('fb_init', 'ig_init') 
                and (page_type in (72, 73, 74) or (page_type in (72, 73, 74, 70, 46, 37) and ad_type in ('DISPLAY')))
            )
            group by 2
            order by 2 desc 
        )
    ) limit 1 )
)