alltime_count_view = """ 
create table alltime_count (poly_id, pv, openwater, wet, total) as
    (select * from (select poly_id, count(fc_pv) as pv from data where fc_pv > 0 group by poly_id) as a 
        full join (select poly_id, count(wofs_water) as openwater from data where wofs_water > 0 group by poly_id) as b using (poly_id)
        full join (select poly_id, count(tci_w) as wet from data where tci_w+wofs_water > 0 group by poly_id) as c using (poly_id)
        full join (select poly_id, count(datetime) as total from data group by poly_id) as d using (poly_id)
    )
    """

first_observe_view = """
create table first_observe (poly_id, pv, openwater, wet) as
    (select * from (select poly_id, min(datetime) as pv from data where fc_pv > 0 group by poly_id ) as a
        full join (select poly_id, min(datetime) as openwater from data where wofs_water > 0 group by poly_id) as b using (poly_id)
        full join (select poly_id, min(datetime) as wet from data where tci_w+wofs_water > 0 group by poly_id) as c using (poly_id)
    )
    """

year_metric_view = """
create or replace view year_metrics (poly_id, year, wet_min, wet_max, wet_mean, water_min, water_max, water_mean,
pv_min, pv_max, pv_mean) as
    (select poly_id, extract(year from datetime) as year, min(tci_w + wofs_water), max(tci_w + wofs_water),
        avg(tci_w+wofs_water), min(wofs_water), max(wofs_water), avg(wofs_water),
            min(fc_pv), max(fc_pv), avg(fc_pv)
        from data group by year, poly_id 
        order by year asc
    )
    """

event_metrics_time_table = """
create table event_metrics_time (event_id bigserial primary key,
    poly_id int, end_time timestamp,
    start_time timestamp, duration interval,
    unique(poly_id, end_time, start_time)
)
"""

first_event_metrics = """
insert into event_metrics_time (poly_id, end_time, start_time, duration)
(select *, f.end-f.start + interval '1D' as duration from
    (select data.poly_id, max(data.datetime) as end, a.start from data
        natural join (select data.poly_id, min(data.datetime) as end, b.start from data
            natural join (select poly_id, min(datetime) as start from data where (tci_w+wofs_water)> 0.01 group by poly_id) as b 
            where data.datetime > start and (tci_w+wofs_water) <= 0.01 group by poly_id, b.start) as a 
        where data.datetime < a.end group by data.poly_id, a.start) as f) 
on conflict do nothing
"""

update_event_metrics = """
insert into event_metrics_time (poly_id, end_time, start_time, duration)
(select *, b.end-b.start + interval '1D' as duration from
(select data.poly_id, max(data.datetime) as end, a.start from data
    natural join (select data.poly_id, min(datetime) as end, ns.start from data 
        natural join (select data.poly_id, min(data.datetime) as start from data,
        (select poly_id, max(end_time) as end_time from event_metrics_time group by poly_id) as ev
        where (tci_w+wofs_water) > 0.01 and data.datetime > ev.end_time and data.poly_id = ev.poly_id
        group by data.poly_id) as ns
        where data.datetime > ns.start and (tci_w+wofs_water) <= 0.01
        group by data.poly_id, ns.start) as a
        where data.datetime < a.end group by data.poly_id, a.start) as b)
on conflict do nothing
"""

last_event_metrics = """
insert into event_metrics_time (poly_id, end_time, start_time, duration)
(select *, b.end-b.start + interval '1D' as duration from (select poly_id, max(datetime) as end, a.start from data
        natural join (select data.poly_id, min(data.datetime) as start from data,
                    (select poly_id, max(end_time) as end_time from event_metrics_time group by poly_id) as ev
                    where (tci_w+wofs_water) > 0.01 and data.datetime > ev.end_time and data.poly_id = ev.poly_id
                    group by data.poly_id) as a where (tci_w+wofs_water) > 0.01
                    group by data.poly_id,a.poly_id, a.start) as b)
on conflict do nothing
"""

incomplete_event_table= """
create table incomplete_event (poly_id, end_time, start_time, duration) as
(select *, b.end-b.start + interval '1D' as duration from (select poly_id, max(datetime) as end, a.start from data
        natural join (select data.poly_id, min(data.datetime) as start from data,
                    (select poly_id, max(end_time) as end_time from event_metrics_time group by poly_id) as ev
                    where (tci_w+wofs_water) > 0.01 and data.datetime > ev.end_time and data.poly_id = ev.poly_id
                    group by data.poly_id) as a where (tci_w+wofs_water) > 0.01
                    group by data.poly_id,a.poly_id, a.start) as b
        )
"""

event_metrics_view = """
create materialized view event_metrics as
(
select ev.*, max(data.tci_w+data.wofs_water) as max, avg(data.tci_w+data.wofs_water) as mean,
    max(data.tci_w+data.wofs_water)*max(ST_area(polygons.geometry))/10000 as area from data, polygons, event_metrics_time as ev
    where polygons.poly_id = ev.poly_id and data.poly_id = polygons.poly_id and data.datetime >= ev.start_time and data.datetime <= ev.end_time
    group by ev.event_id
    union all
select ev.*, max(data.tci_w+data.wofs_water) as max, avg(data.tci_w+data.wofs_water) as mean,
    max(data.tci_w+data.wofs_water)*max(ST_area(polygons.geometry))/10000 as area from data, polygons, incomplete_event as ev
    where polygons.poly_id = ev.poly_id and data.poly_id = polygons.poly_id and data.datetime >= ev.start_time and data.datetime <= ev.end_time
    group by ev.event_id

)
"""
