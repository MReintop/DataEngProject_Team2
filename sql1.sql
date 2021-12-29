select * from memes limit 100

select count(*) from (
select distinct origin from memes ) o -- 4920

select * from memes m 
where m.about_text is null 

select * from memes m 
where time_added > "2021-01-01-00-00-00"

select * from memes m2 where parent in (
select url from memes m 
where title = 'Copypasta' )

select * from memes m 
where url='https://knowyourmeme.com/memes/memes'

select distinct * from memes m3 where parent in (
select distinct url from memes m2 where parent in (
select distinct url from memes m 
where title = 'Copypasta' ))


select parent as url, count(distinct id) from memes m2 
where parent in (
	select url from memes m )
group by 1 order by 2