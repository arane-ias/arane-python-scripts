
-- can we cremove grouo by utcdate in below sub-queries

-- snapchat query

select 'Snapchat' as src, '20230105' as utcdate, imps_count, previous_day_count,
round((CAST((imps_count - previous_day_count) as double ) / previous_day_count *100),2) 
as DOD_percent
from
(
    (
        select sum(imps_count) as imps_count
        from
        (
            select count(distinct impressionid) as imps_count from snapchat
            where type = 'impression' and utcdate in ('20230105') and sourceid = 10 and
            utchour
            in ('05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23')
            group by utcdate

            union

            select count(distinct impressionid) as imps_count from snapchat
            where type = 'impression' and  utcdate in ('20230106') and sourceid = 10 and
            utchour
            in ('00','01','02','03','04')
            group by utcdate
        )
    )
    CROSS JOIN
    (
        select sum(imps_count) as previous_day_count
        from
        (
            select count(distinct impressionid) as imps_count from snapchat
            where type = 'impression' and utcdate in ('20230104') and sourceid = 10 and
            utchour
            in ('05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23')
            group by utcdate

            union

            select count(distinct impressionid) as imps_count from snapchat
            where type = 'impression' and utcdate in ('20230105') and sourceid = 10 and
            utchour
            in ('00','01','02','03','04')
            group by utcdate
        )
    )
)

-------------------------------------

-- Pinterest

select 'Pinterest' as src, '20230105' as utcdate, imps_count, previous_day_count,
round((CAST((imps_count - previous_day_count) as double ) / previous_day_count *100),2) 
as DOD_percent
from
(
    (
        select sum(imps_count) as imps_count
        from
        (
            select count(distinct 'original.impressionid') as imps_count from general_events_v1
            where "original.type"='impression' and utcdate in ('20230105') and sourceid = 11 
            and utchour
            in ('05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23')
            group by utcdate

            union

            select count(distinct 'original.impressionid') as imps_count from general_events_v1
            where "original.type"='impression' and  utcdate in ('20230106') and sourceid = 11 
            and utchour
            in ('00','01','02','03','04')
            group by utcdate
        )
    )
    CROSS JOIN
    (
        select sum(imps_count) as previous_day_count
        from
        (
            select count(distinct 'original.impressionid') as imps_count from general_events_v1
            where  "original.type"='impression' and utcdate in ('20230104') and sourceid = 11 
            and utchour
            in ('05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23')
            group by utcdate

            union

            select count(distinct 'original.impressionid') as imps_count from general_events_v1
            where  "original.type"='impression' and utcdate in ('20230105') and sourceid = 11 
            and utchour
            in ('00','01','02','03','04')
            group by utcdate
        )
    )
)

-------------------------------------

-- LinkedIn

select 'Linkedin' as src, '20230105' as utcdate, imps_count, previous_day_count,
round((CAST((imps_count - previous_day_count) as double ) / previous_day_count *100),2) 
as DOD_percent
from
(
    (
        select sum(imps_count) as imps_count
        from
        (
            select count(distinct 'original.impressionid') as imps_count from general_events_v1
            where "original.type"='impression' and utcdate in ('20230105') and sourceid = 16
            and utchour
            in ('05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23')
            group by utcdate

            union

            select count(distinct 'original.impressionid') as imps_count from general_events_v1
            where "original.type"='impression' and  utcdate in ('20230106') and sourceid = 16 
            and utchour
            in ('00','01','02','03','04')
            group by utcdate
        )
    )
    CROSS JOIN
    (
        select sum(imps_count) as previous_day_count
        from
        (
            select count(distinct 'original.impressionid') as imps_count from general_events_v1
            where  "original.type"='impression' and utcdate in ('20230104') and sourceid = 16 
            and utchour
            in ('05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23')
            group by utcdate

            union

            select count(distinct 'original.impressionid') as imps_count from general_events_v1
            where  "original.type"='impression' and utcdate in ('20230105') and sourceid = 16 
            and utchour
            in ('00','01','02','03','04')
            group by utcdate
        )
    )
)

-------------------------------------

-- Spotify

select 'Spotify' as src, '20230105' as utcdate, imps_count, previous_day_count,
round((CAST((imps_count - previous_day_count) as double ) / previous_day_count *100),2) 
as DOD_percent
from
(
    (
        select sum(imps_count) as imps_count
        from
        (
            select count(distinct 'original.impressionid') as imps_count from general_events_v2
            where "original.type"='impression' and utcdate in ('20230105') and sourceid = 19
            and utchour
            in ('05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23')
            group by utcdate

            union

            select count(distinct 'original.impressionid') as imps_count from general_events_v2
            where "original.type"='impression' and  utcdate in ('20230106') and sourceid = 19
            and utchour
            in ('00','01','02','03','04')
            group by utcdate
        )
    )
    CROSS JOIN
    (
        select sum(imps_count) as previous_day_count
        from
        (
            select count(distinct 'original.impressionid') as imps_count from general_events_v2
            where  "original.type"='impression' and utcdate in ('20230104') and sourceid = 19
            and utchour
            in ('05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23')
            group by utcdate

            union

            select count(distinct 'original.impressionid') as imps_count from general_events_v2
            where  "original.type"='impression' and utcdate in ('20230105') and sourceid = 19
            and utchour
            in ('00','01','02','03','04')
            group by utcdate
        )
    )
)

-------------------------------------

-- Yahoo

select 'Yahoo' as src, '20230105' as utcdate, imps_count, previous_day_count,
round((CAST((imps_count - previous_day_count) as double ) / previous_day_count *100),2) 
as DOD_percent
from
(
    (
        select sum(imps_count) as imps_count
        from
        (
            select count(distinct impressionid) as imps_count from yahoo
            where type = 'impression' and utcdate in ('20230105') and sourceid = 5
            and utchour
            in ('05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23')
            group by utcdate

            union

            select count(distinct impressionid) as imps_count from yahoo
            where type = 'impression' and  utcdate in ('20230106') and sourceid = 5
            and utchour
            in ('00','01','02','03','04')
            group by utcdate
        )
    )
    CROSS JOIN
    (
        select sum(imps_count) as previous_day_count
        from
        (
            select count(distinct impressionid) as imps_count from yahoo
            where type = 'impression' and utcdate in ('20230104') and sourceid = 5
            and utchour
            in ('05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23')
            group by utcdate

            union

            select count(distinct impressionid) as imps_count from yahoo
            where type = 'impression' and utcdate in ('20230105') and sourceid = 5
            and utchour
            in ('00','01','02','03','04')
            group by utcdate
        )
    )
)

-------------------------------------

-- Facebook

select 'Facebook' as src, '20230104' as utcdate, imps_count, previous_day_count,
round((CAST((imps_count - previous_day_count) as double ) / previous_day_count *100),2) 
as DOD_percent
from
(
    (
        select sum(imps_count) as imps_count
        from
        (
            select count(*) as imps_count from facebook_parquet
            where  facebook_event_type in ('fb_init', 'ig_init', 'an_init') 
            and not ( 
                facebook_event_type in ('fb_init', 'ig_init') 
                and 
                (   
                    page_type in (72, 73, 74) 
                    or (page_type in (72, 73, 74, 70, 46, 37) and ad_type in ('DISPLAY'))
                )
            )
            and utcdate in ('20230104')
            and utchour
            in ('05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23')
            group by utcdate

            union

            select count(*) as imps_count from facebook_parquet
            where  facebook_event_type in ('fb_init', 'ig_init', 'an_init')
            and not ( 
                facebook_event_type in ('fb_init', 'ig_init') 
                and 
                (   
                    page_type in (72, 73, 74) 
                    or (page_type in (72, 73, 74, 70, 46, 37) and ad_type in ('DISPLAY'))
                )
            )
            and utcdate in ('20230105') 
            and utchour
            in ('00','01','02','03','04')
            group by utcdate
        )
    )
    CROSS JOIN
    (
        select sum(imps_count) as previous_day_count
        from
        (
            select count(*) as imps_count from facebook_parquet
            where  facebook_event_type in ('fb_init', 'ig_init', 'an_init') 
            and not ( 
                facebook_event_type in ('fb_init', 'ig_init') 
                and 
                (   
                    page_type in (72, 73, 74) 
                    or (page_type in (72, 73, 74, 70, 46, 37) and ad_type in ('DISPLAY'))
                )
            )
            and utcdate in ('20230103')
            and utchour
            in ('05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23')
            group by utcdate

            union

            select count(*) as imps_count from facebook_parquet
            where  facebook_event_type in ('fb_init', 'ig_init', 'an_init')
            and not ( 
                facebook_event_type in ('fb_init', 'ig_init') 
                and 
                (   
                    page_type in (72, 73, 74) 
                    or (page_type in (72, 73, 74, 70, 46, 37) and ad_type in ('DISPLAY'))
                )
            )
            and utcdate in ('20230104') 
            and utchour
            in ('00','01','02','03','04')
            group by utcdate
        )
    )
)

