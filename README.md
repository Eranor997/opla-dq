# opla-dq

![image](https://github.com/user-attachments/assets/89431f77-bf73-4149-9635-ed8f58d1b7e0)


create database dq;
drop table dq.dq_shkDefect_verdict;
create table dq.dq_shkDefect_verdict
    engine = ReplacingMergeTree() order by dt_date as
select toDate(dt) dt_date
     , count()                                            rows_qty
     , uniq(shk_id)                                  shk_id_uniq
     , uniq(chrt_id)                                        chrt_id_uniq
     , uniq(seller_id)                                 seller_id_uniq
     , uniq(state_id)                           state_id_uniq
     , countIf(shk_id < 1)                                 shk_id_0_qty
     , countIf(chrt_id < 1)                           chrt_id_0_qty
     , countIf(seller_id < 1)                     seller_id_0_qty
from default.shkDefect_verdict
where dt >= today() - interval 10 day
group by dt_date;
