select src, utcdate, imps_count , previous_day_count,
round((CAST((imps_count -previous_day_count) as double ) / previous_day_count *100),2) 
as DOD_percent 
from 
(
    (   
        select 'Snapchat' as src, ? as utcdate, imps_count, previous_day_count
        from
        (
            (
                select count(distinct impressionid) as imps_count from snapchat
                where 
                (
                    type = 'impression' and utcdate in (?) and sourceid = 10 and
                    utchour not in ('00','01','02','03','04')
                )
                or
                (
                    type = 'impression' and  utcdate in (?) and sourceid = 10 and
                    utchour in ('00','01','02','03','04')
                )
            )
            CROSS JOIN
            (
                select count(distinct impressionid) as previous_day_count from snapchat
                where 
                (
                    type = 'impression' and utcdate in (?) and sourceid = 10 and
                    utchour not in ('00','01','02','03','04')
                )
                or
                (
                    type = 'impression' and utcdate in (?) and sourceid = 10 and
                    utchour in ('00','01','02','03','04')
                )
            )
        )
    ) 
    
    UNION ALL 
    
    ( 
        select 'Pinterest' as src, ? as utcdate, imps_count, previous_day_count
        from
        (
            (
                select count(distinct "original.impressionid") as imps_count from general_events_v1
                where 
                (
                    "original.type"='impression' and utcdate in (?) and sourceid = 11 
                    and utchour not in ('00','01','02','03','04')
                )
                or
                ( 
                    "original.type"='impression' and  utcdate in (?) and sourceid = 11 
                    and utchour in ('00','01','02','03','04')
                )  
            )
            CROSS JOIN
            (
                select count(distinct "original.impressionid") as previous_day_count from general_events_v1
                where  
                (
                    "original.type"='impression' and utcdate in (?) and sourceid = 11 
                    and utchour not in ('00','01','02','03','04')
                )
                or
                (
                    "original.type"='impression' and utcdate in (?) and sourceid = 11 
                    and utchour in ('00','01','02','03','04')
                )   
            )
        )
    ) 
    
    UNION ALL 
        
    ( 
        select 'Linkedin' as src, ? as utcdate, imps_count, previous_day_count
        from
        (
            (
                select count(distinct "original.impressionid") as imps_count from general_events_v1
                where 
                (
                    "original.type"='impression' and utcdate in (?) and sourceid = 16
                    and utchour not in ('00','01','02','03','04')
                )
                or
                (
                    "original.type"='impression' and  utcdate in (?) and sourceid = 16 
                    and utchour in ('00','01','02','03','04')
                ) 
            )
            CROSS JOIN
            (
                select count(distinct "original.impressionid") as previous_day_count from general_events_v1
                where  
                (
                    "original.type"='impression' and utcdate in (?) and sourceid = 16 
                    and utchour not in ('00','01','02','03','04')
                )
                or
                or
                (
                    "original.type"='impression' and utcdate in (?) and sourceid = 16 
                    and utchour in ('00','01','02','03','04')
                )   
            )
        )
    ) 
    
    UNION ALL 
    ( 
        select 'Spotify' as src, ? as utcdate, imps_count, previous_day_count
        from
        (
            (
                select count(distinct "original.impressionid") as imps_count from general_events_v2
                where 
                (
                    "original.type"='impression' and utcdate in (?) and sourceid = 19
                    and utchour not in('00','01','02','03','04')
                )
                or
                (
                    "original.type"='impression' and  utcdate in (?) and sourceid = 19
                    and utchour in ('00','01','02','03','04')
                )
            )
            CROSS JOIN
            (
                select count(distinct "original.impressionid") as previous_day_count from general_events_v2
                where  
                (
                    "original.type"='impression' and utcdate in (?) and sourceid = 19
                    and utchour not in ('00','01','02','03','04')
                )
                or
                (
                    "original.type"='impression' and utcdate in (?) and sourceid = 19
                    and utchour in ('00','01','02','03','04')
                )
            )
        )
    ) 
    
    UNION ALL 
    ( 
        select 'Yahoo' as src, ? as utcdate, imps_count, previous_day_count
        from
        (
            (
                select count(distinct impressionid) as imps_count from yahoo
                where 
                (
                    type = 'impression' and utcdate in (?) and sourceid = 5
                    and utchour not in ('00','01','02','03','04')
                )
                or
                (
                    type = 'impression' and  utcdate in (?) and sourceid = 5
                    and utchour in ('00','01','02','03','04')
                )
            )
            CROSS JOIN
            (
                select count(distinct impressionid) as previous_day_count from yahoo
                where 
                (
                    type = 'impression' and utcdate in (?) and sourceid = 5
                    and utchour not in ('00','01','02','03','04')
                )
                or
                (
                    type = 'impression' and utcdate in (?) and sourceid = 5
                    and utchour in ('00','01','02','03','04')
                )
            )
        )
    )

    UNION ALL 
    ( 
        select 'Facebook' as src, ? as utcdate, imps_count, previous_day_count
        from
        (
            (
                select count(*) as imps_count from facebook_parquet
                where  
                (
                    facebook_event_type in ('fb_init', 'ig_init', 'an_init') 
                    and not ( 
                            facebook_event_type in ('fb_init', 'ig_init') 
                        and 
                        (   
                            page_type in (72, 73, 74) 
                            or (page_type in (72, 73, 74, 70, 46, 37) and ad_type in ('DISPLAY'))
                        )
                    )
                )
                and
                (
                    (
                        utcdate in (?)
                        and utchour not in ('00','01','02','03','04')
                    )
                    or
                    (
                        utcdate in (?) 
                        and utchour in ('00','01','02','03','04')
                    )
                )
            )
            CROSS JOIN
            (
                select count(*) as previous_day_count from facebook_parquet
                where  
                (
                    facebook_event_type in ('fb_init', 'ig_init', 'an_init') 
                    and not ( 
                            facebook_event_type in ('fb_init', 'ig_init') 
                        and 
                        (      
                            page_type in (72, 73, 74) 
                            or (page_type in (72, 73, 74, 70, 46, 37) and ad_type in ('DISPLAY'))
                        )
                    )
                )
                and 
                (
                    (
                        utcdate in (?)
                        and utchour not in ('00','01','02','03','04')
                    )
                    or
                    (
                        utcdate in (?) 
                        and utchour in ('00','01','02','03','04')
                    )
                )
            )
        )
    )
)