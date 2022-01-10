

/* 
 *  A N A L Y S I S 
 * 
 * */

----------------------------------------------------------------
----------------------------------------------------------------
--
-- PARENTS
--
-- How many distinct parents?
select count(distinct(parent)) from meme m; -- 1377

-- How many memes have parent?
select count(distinct(url)) from meme m
where parent != ''; -- 5310

-- Are all the parents saved as memes?
select count(distinct(par)) from (
	select distinct p.parent as par from meme p
	left join meme m 
	on m.url = p.parent 
	where m.url is null
) a; -- 942 -- these are not memes -- there are other categories

-- Do we find these parents from the table link?
select count(distinct(url)) from link l 
where url in(
	select distinct par from (
		select distinct p.parent as par from meme p
		left join meme m 
		on m.url = p.parent 
		where m.url is null
	) a
); -- 940; only 2 were missing

-- Which parents are missing from database at all?
select distinct par from (
	select distinct p.parent as par from meme p
	left join meme m 
	on m.url = p.parent 
	where m.url is null
) a
left join link l 
on l.url=a.par
where l.url is null;

/*
'' <- one is empty parent
https://knowyourmeme.com/memes/42 <- there is actually only one and it is not found in internet
*/

select * from meme m 
where url like '%https://knowyourmeme.com/memes/42%';

/* There are 2 potential memes:
https://knowyourmeme.com/memes/420dankbillyclinton
https://knowyourmeme.com/memes/420-blaze-it
*/

-- Memes which are parents, but have no parents
select * from meme m 
where url in (select distinct parent from meme m2 where parent != '')
and m.parent = ''; -- 124


-- Count parents, childred, grandchildren, ...
-- grandgrandgrandgrandgrandchildren
select * from meme m8 
where m8.parent in (
	-- grandgrandgrandgrandchildren
	select distinct m7.url from meme m7 
	where parent in (
		-- grandgrandgrandchildren
		select distinct m6.url from meme m6 
		where m6.parent in (
			-- grandgrandchildren
			select distinct m5.url from meme m5 
			where m5.parent in (
				-- grandchildren
				select distinct m4.url from meme m4 
				where m4.parent in (
					-- children
					select distinct m.url from meme m 
					where parent in (
						-- memes which are parents, but have no parent
						select distinct m2.url from meme m2
						where url in (
							select distinct parent from meme m3 where parent != '' -- all parents
						)
						and m2.parent = '' -- 124
					) -- 418
				) -- 120
			) -- 185
		) -- 32
	) -- 6
) -- 0


-- Relatives ordered by the youngest generations existing
select * from (
	select distinct
		m1.title as parent0, 
		m2.title as parent1,
		m3.title as parent2,
		m4.title as parent3,
		m5.title as parent4,
		m6.title as parent5,
		m7.title as parent6
	from meme m1 
	left join meme m2 on m2.parent = m1.url
	left join meme m3 on m3.parent = m2.url
	left join meme m4 on m4.parent = m3.url
	left join meme m5 on m5.parent = m4.url
	left join meme m6 on m6.parent = m5.url
	left join meme m7 on m7.parent = m6.url
	where m1.url in (select distinct m.parent from meme m where m.parent != '')
	and m1.parent = ''
) relatives
order by 7,6,5,4,3,2,1 desc;

-- where parent4 is not null; -- etc
-- parent5 is not null; -- 6 urls on 6th generation
-- parent6 is not null; -- there is no 7th generation


----------------------------------------------------------------
----------------------------------------------------------------
--
-- TAGS
--

-- top 50 popular tag + tags which is like them
select distinct ptt.pop as pop_tag, ptt.tag as tag from (
	-- select memes and tags and
	select mt2.*,poptag.tag as pop from meme_tag mt2 
	-- join the popular tags with tags which are like them
	left join (
		-- select top 50 tags as list
		select tag from (
			-- find most popular tags
			select tag,count(distinct(meme_url)) from meme_tag mt 
			group by 1 order by 2 desc limit 50
		) t 
	) poptag
	on mt2.tag like concat('%',poptag.tag,'%')
) ptt
where pop is not null
order by 2;


-- find the most popular tags
select pop,count(distinct(meme_url)) from (
	-- select memes and tags and
	select mt2.*,poptag.tag as pop from meme_tag mt2 
	-- join the popular tags with tags which are like them
	left join (
		-- select top x tags as list
		select tag from (
			-- find most popular tags
			select tag,count(distinct(meme_url)) from meme_tag mt 
			group by 1 order by 2 desc limit 50
		) t 
	) poptag
	on mt2.tag like concat('%',poptag.tag,'%')
	where poptag.tag is not null
) ptt
group by 1 order by 2 desc;




----------------------------------------------------------------
----------------------------------------------------------------
--
-- REFERENCES AND ORIGINS
--

-- Popular references 
select mr.reference_name,count(distinct(m.url)) from meme m 
join meme_reference mr 
on mr.meme_url = m.url 
group by 1 order by 2 desc;

-- Popular origins
select origin,count(distinct(m.url)) from meme m 
group by 1 order by 2 desc;

-- meme year and origin
select meme_year, origin,count(distinct(m.url)) from meme m 
group by 1,2 order by 3 desc;

-- time added year and origin counts
select extract(year from time_added), origin,count(distinct(m.url)) from meme m 
group by 1,2 order by 3 desc;

-- combinations of popular origins and references 
select m.origin,mr.reference_name,count(distinct(m.url)) from meme m 
join meme_reference mr 
on mr.meme_url = m.url
group by 1,2 order by 3 desc limit 20;

-- origins and types 
select origin,mt.type, count(distinct(m.url)) from meme m 
left join meme_type mt 
on mt.meme_url = m.url 
group by 1,2 order by 3 desc;

-- types and counts
select mt.type,count(distinct(meme_url)) from meme_type mt 
group by 1 order by 2 desc; -- 34 types altogether



--- LINK and MEME

select meme_url,section,count(distinct(link)) from meme_link ml 
group by 1,2 order by 3 desc;

select * from meme_link ml 
where meme_url = 'https://knowyourmeme.com/memes/pepe-the-frog';

select section,count(distinct(link)) from meme_link ml 
where link in (select url from meme m)
group by 1 order by 2 desc;

select 
	section,
	case 
		when m.url is null then 'N' 
		when m.url is not null then 'Y' 
	end as is_meme,
	count(distinct(link)) 
from meme_link ml 
left join meme m 
on m.url = ml.link 
group by 1,2 order by 3 desc;

