
class Queries:
    def __init__(self) -> None:
        self.snapchat = """select 'Snapchat' as src, ? as utcdate, imps_count, previous_day_count, 
                            round((CAST((imps_count - previous_day_count) as double ) / previous_day_count *100),2) as DOD_percent
                            from
                            (
                                (
                                    select sum(imps_count) as imps_count
                                    from
                                    (
                                        select utcdate , count(distinct impressionid) as imps_count from snapchat
                                        where utcdate in (?) and sourceid = 10 and
                                        utchour
                                        in ('05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23')
                                        group by utcdate

                                        union

                                        select utcdate, count(distinct impressionid) as imps_count from snapchat
                                        where type = 'impression' and  utcdate in (?) and sourceid = 10 and
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
                                        select utcdate, count(distinct impressionid) as imps_count from snapchat
                                        where utcdate in (?) and sourceid = 10 and
                                        utchour
                                        in ('05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23')
                                        group by utcdate

                                        union

                                        select utcdate, count(distinct impressionid) as imps_count from snapchat
                                        where type = 'impression' and utcdate in (?) and sourceid = 10 and
                                        utchour
                                        in ('00','01','02','03','04')
                                        group by utcdate
                                    )
                                )
                            )"""
        self.spotify = """select 'Spotify' as src, ? as utcdate, imps_count, previous_day_count,
                            round((CAST((imps_count - previous_day_count) as double ) / previous_day_count *100),2) 
                            as DOD_percent
                            from
                            (
                                (
                                    select sum(imps_count) as imps_count
                                    from
                                    (
                                        select utcdate , count(distinct 'original.impressionid') as imps_count from general_events_v2
                                        where "original.type"='impression' and utcdate in (?) and sourceid = 19
                                        and utchour
                                        in ('05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23')
                                        group by utcdate

                                        union

                                        select utcdate, count(distinct 'original.impressionid') as imps_count from general_events_v2
                                        where "original.type"='impression' and  utcdate in (?) and sourceid = 19
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
                                        select utcdate, count(distinct 'original.impressionid') as imps_count from general_events_v2
                                        where  "original.type"='impression' and utcdate in (?) and sourceid = 19
                                        and utchour
                                        in ('05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23')
                                        group by utcdate

                                        union

                                        select utcdate, count(distinct 'original.impressionid') as imps_count from general_events_v2
                                        where  "original.type"='impression' and utcdate in (?) and sourceid = 19
                                        and utchour
                                        in ('00','01','02','03','04')
                                        group by utcdate
                                    )
                                )
                            )"""
        self.pinterest = """select 'Pinterest' as src, ? as utcdate, imps_count, previous_day_count,
                            round((CAST((imps_count - previous_day_count) as double ) / previous_day_count *100),2) 
                            as DOD_percent
                            from
                            (
                                (
                                    select sum(imps_count) as imps_count
                                    from
                                    (
                                        select utcdate , count(distinct 'original.impressionid') as imps_count from general_events_v1
                                        where "original.type"='impression' and utcdate in (?) and sourceid = 11 
                                        and utchour
                                        in ('05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23')
                                        group by utcdate

                                        union

                                        select utcdate, count(distinct 'original.impressionid') as imps_count from general_events_v1
                                        where "original.type"='impression' and  utcdate in (?) and sourceid = 11 
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
                                        select utcdate, count(distinct 'original.impressionid') as imps_count from general_events_v1
                                        where  "original.type"='impression' and utcdate in (?) and sourceid = 11 
                                        and utchour
                                        in ('05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23')
                                        group by utcdate

                                        union

                                        select utcdate, count(distinct 'original.impressionid') as imps_count from general_events_v1
                                        where  "original.type"='impression' and utcdate in (?) and sourceid = 11 
                                        and utchour
                                        in ('00','01','02','03','04')
                                        group by utcdate
                                    )
                                )
                            )"""
        self.linkedin = """select 'Linkedin' as src, ? as utcdate, imps_count, previous_day_count,
                            round((CAST((imps_count - previous_day_count) as double ) / previous_day_count *100),2) 
                            as DOD_percent
                            from
                            (
                                (
                                    select sum(imps_count) as imps_count
                                    from
                                    (
                                        select utcdate , count(distinct 'original.impressionid') as imps_count from general_events_v1
                                        where "original.type"='impression' and utcdate in (?) and sourceid = 16
                                        and utchour
                                        in ('05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23')
                                        group by utcdate

                                        union

                                        select utcdate, count(distinct 'original.impressionid') as imps_count from general_events_v1
                                        where "original.type"='impression' and  utcdate in (?) and sourceid = 16 
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
                                        select utcdate, count(distinct 'original.impressionid') as imps_count from general_events_v1
                                        where  "original.type"='impression' and utcdate in (?) and sourceid = 16 
                                        and utchour
                                        in ('05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23')
                                        group by utcdate

                                        union

                                        select utcdate, count(distinct 'original.impressionid') as imps_count from general_events_v1
                                        where  "original.type"='impression' and utcdate in (?) and sourceid = 16 
                                        and utchour
                                        in ('00','01','02','03','04')
                                        group by utcdate
                                    )
                                )
                            )"""
        self.yahoo = """select 'Yahoo' as src, ? as utcdate, imps_count, previous_day_count,
                        round((CAST((imps_count - previous_day_count) as double ) / previous_day_count *100),2) 
                        as DOD_percent
                        from
                        (
                            (
                                select sum(imps_count) as imps_count
                                from
                                (
                                    select utcdate , count(distinct impressionid) as imps_count from yahoo
                                    where type = 'impression' and utcdate in (?) and sourceid = 5
                                    and utchour
                                    in ('05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23')
                                    group by utcdate

                                    union

                                    select utcdate, count(distinct impressionid) as imps_count from yahoo
                                    where type = 'impression' and  utcdate in (?) and sourceid = 5
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
                                    select utcdate, count(distinct impressionid) as imps_count from yahoo
                                    where type = 'impression' and utcdate in (?) and sourceid = 5
                                    and utchour
                                    in ('05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23')
                                    group by utcdate

                                    union

                                    select utcdate, count(distinct impressionid) as imps_count from yahoo
                                    where type = 'impression' and utcdate in (?) and sourceid = 5
                                    and utchour
                                    in ('00','01','02','03','04')
                                    group by utcdate
                                )
                            )
                        )"""
        self.fb = """select 'Facebook' as src, ? as utcdate, imps_count, previous_day_count,
                    round((CAST((imps_count - previous_day_count) as double ) / previous_day_count *100),2) 
                    as DOD_percent
                    from
                    (
                        (
                            select sum(imps_count) as imps_count
                            from
                            (
                                select utcdate, count(*) as imps_count from facebook_parquet
                                where  facebook_event_type in ('fb_init', 'ig_init', 'an_init') 
                                and not ( 
                                    facebook_event_type in ('fb_init', 'ig_init') 
                                    and 
                                    (   
                                        page_type in (72, 73, 74) 
                                        or (page_type in (72, 73, 74, 70, 46, 37) and ad_type in ('DISPLAY'))
                                    )
                                )
                                and utcdate in (?)
                                and utchour
                                in ('05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23')
                                group by utcdate

                                union

                                select utcdate, count(*) as imps_count from facebook_parquet
                                where  facebook_event_type in ('fb_init', 'ig_init', 'an_init')
                                and not ( 
                                    facebook_event_type in ('fb_init', 'ig_init') 
                                    and 
                                    (   
                                        page_type in (72, 73, 74) 
                                        or (page_type in (72, 73, 74, 70, 46, 37) and ad_type in ('DISPLAY'))
                                    )
                                )
                                and utcdate in (?) 
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
                                select utcdate, count(*) as imps_count from facebook_parquet
                                where  facebook_event_type in ('fb_init', 'ig_init', 'an_init') 
                                and not ( 
                                    facebook_event_type in ('fb_init', 'ig_init') 
                                    and 
                                    (   
                                        page_type in (72, 73, 74) 
                                        or (page_type in (72, 73, 74, 70, 46, 37) and ad_type in ('DISPLAY'))
                                    )
                                )
                                and utcdate in (?)
                                and utchour
                                in ('05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23')
                                group by utcdate

                                union

                                select utcdate, count(*) as imps_count from facebook_parquet
                                where  facebook_event_type in ('fb_init', 'ig_init', 'an_init')
                                and not ( 
                                    facebook_event_type in ('fb_init', 'ig_init') 
                                    and 
                                    (   
                                        page_type in (72, 73, 74) 
                                        or (page_type in (72, 73, 74, 70, 46, 37) and ad_type in ('DISPLAY'))
                                    )
                                )
                                and utcdate in (?) 
                                and utchour
                                in ('00','01','02','03','04')
                                group by utcdate
                            )
                        )
                    )"""
                    