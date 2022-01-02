-- 100 samples from main table
select * from meme limit 100;

/* VALIDATE MEME*/
-- count rows
select count(*) from meme m; -- 12621

-- count URLs
select count(distinct(url)) from meme m; -- 12621

-- count titles
select count(distinct(title)) from meme m; -- 12621
select * from meme m 
where title = 'nan'; -- 0

-- count images
select count(distinct(image)) from meme m; -- 12621
select * from meme m 
where image = 'nan'; -- 0

-- social_media_description
-- most frequent length is 156, count 1521
select length(social_media_description),count(*) from meme m 
group by 1 order by 2 desc;
-- the longest length is 3328, count 1
select length(social_media_description),count(*) from meme m 
group by 1 order by 1 desc;
-- the shortest length is 1, count 19
select length(social_media_description),count(*) from meme m 
group by 1 order by 1;
-- examples
select * from meme m 
where length(social_media_description)=1;

-- status
select count(distinct(status)) from meme m; -- 4
select status, count(*) from meme m 
group by 1 order by 2 desc; -- unlisted, count 2

-- origin
select count(distinct(origin)) from meme m; -- 4920
select origin,count(distinct(url)) from meme m 
group by 1 order by 2 desc; -- many memes can have same origin -- most popular is 'Unknown'
select url,count(distinct(origin)) from meme m 
group by 1 order by 2 desc; -- all memes have only one origin

-- meme_year
select * from meme m 
where meme_year is null; -- 0 rows
select count(distinct(meme_year)) from meme m; -- 145
-- the most popular year is 2011, count 1853
select meme_year,count(*) from meme m 
group by 1 order by 2 desc;
-- years from 1070 till 2916 -- these are years related to topics of memes
select meme_year,count(*) from meme m 
group by 1 order by 1 desc;

-- count unique keywords, group keywords and count
select count(distinct(keywords)) from meme m; -- 5635
select keywords, count(distinct(url)) from meme m
group by 1 order by 2 desc; -- 6978 no values, 9 double, others unique

-- parent 
select count(*) from meme m 
where parent = 'nan'; -- 7311
-- count parents
select count(distinct(parent)) from meme m; -- 1377

-- time_updated
select count(*) from meme m 
where time_updated is null; -- 0

-- time_added
select count(*) from meme m 
where time_added is null; -- 0


/*VALIDATE MEME_TAG*/
select * from meme_tag mt limit 100;

select * from meme_tag mt 
where tag is null; -- 0
select * from meme_tag mt 
where tag = 'nan'; -- 2 !!!

/*
https://knowyourmeme.com/memes/spicy-memes
https://knowyourmeme.com/memes/photorape
*/

select * from meme m 
where url in(
'https://knowyourmeme.com/memes/spicy-memes',
'https://knowyourmeme.com/memes/photorape');

/*VALIDATE MEME_TYPE*/
select * from meme_type mt 
where type is null; -- 0
select * from meme_type mt 
where type = 'nan'; -- 0
select type,count(distinct(meme_url)) from meme_type mt 
group by 1 order by 2 desc; 
-- 34 different types
-- the most popular is character, count 332
-- the less popular is lip-dub, count 1

/*VALIDATE MEME_TEXT*/
select * from meme_text mt limit 20;
select count(distinct(meme_url)) from meme_text mt 
where about_text = 'nan'; -- 5041
select count(distinct(meme_url)) from meme_text mt 
where origin_text = 'nan'; -- 5524
select count(distinct(meme_url)) from meme_text mt 
where spread_text = 'nan'; -- 6143
select count(distinct(meme_url)) from meme_text mt 
where not_examples_text = 'nan'; -- 12219
select count(distinct(meme_url)) from meme_text mt 
where search_intr_text = 'nan'; -- 11439
select count(distinct(meme_url)) from meme_text mt 
where external_ref_text = 'nan'; -- 6422

-- if no text, then no links for ABOUT
select * from (
	select meme_url from meme_text mt 
	where about_text = 'nan'
) txt
left join meme_about_link mal 
on mal.meme_url = txt.meme_url
where mal.meme_url is not null; -- 0

-- ORIGIN
select * from (
	select meme_url from meme_text mt 
	where origin_text = 'nan'
) txt
left join meme_origin_link mal 
on mal.meme_url = txt.meme_url
where mal.meme_url is not null; -- 1

-- SPREAD
select * from (
	select meme_url from meme_text mt 
	where spread_text = 'nan'
) txt
left join meme_spread_link mal 
on mal.meme_url = txt.meme_url
where mal.meme_url is not null; -- 0

-- NOTABLE EXAMPLES
select * from (
	select meme_url from meme_text mt 
	where not_examples_text = 'nan'
) txt
left join meme_notex_link mal 
on mal.meme_url = txt.meme_url
where mal.meme_url is not null; -- 12

-- SEARCH INTEREST
select * from (
	select meme_url from meme_text mt 
	where search_intr_text = 'nan'
) txt
left join meme_searchint_link mal 
on mal.meme_url = txt.meme_url
where mal.meme_url is not null; -- 0

-- EXTERNAL REF
select * from (
	select meme_url from meme_text mt 
	where external_ref_text = 'nan'
) txt
left join meme_extref_link mal 
on mal.meme_url = txt.meme_url
where mal.meme_url is not null; -- 0


/*VALIDATE MEME_IMAGE*/
select * from meme_image mi limit 20;

select * from meme_image mi 
where link = 'nan'; -- 0

select * from meme_image mi 
where link is null; -- 0


/*VALIDATE MEME_REFERENCE*/
select * from meme_reference mr limit 200;

select * from meme_reference mr 
where reference_link = 'nan'; -- 0
select * from meme_reference mr 
where reference_link is null; -- 0
select * from meme_reference mr 
where reference_name = 'nan'; -- 0
select * from meme_reference mr 
where reference_name is null; -- 0

-- most popular references
select reference_name,count(distinct(meme_url)) from meme_reference mr 
group by 1 order by 2 desc;
/*
Wikipedia	1784
Urban Dictionary	1428
Encyclopedia Dramatica	838
Reddit	608
Twitter	521
Facebook	409
Meme Generator	369
Fandom	19
Dictionary.com	13
IMDb	12
*/


/* 
 * M A K E  A N A L Y S I S 
 * 
 * */


-- PARENTS
-- There are 1377 parents
select count(distinct(parent)) from meme m; -- 1377

-- are all the parents saved as memes?
select count(distinct(par)) from (
	select distinct p.parent as par from meme p
	left join meme m 
	on m.url = p.parent 
	where m.url is null
) a; -- 942 -- these are not memes -- there are other categories

-- memes which are parents, but have no parents
select * from meme m 
where url in (select distinct parent from meme m2 where parent != 'nan')
and m.parent = 'nan';


select par,count(distinct(url)) from (
	-- memes which have these parents
	select
		m.url,
		case when m.parent != 'nan' then 'parent'
		when m.parent = 'nan' then 'not' end as par
	from meme m 
	where parent in (
		select distinct m2.url from meme m2
		where url in (select distinct parent from meme m3 where parent != 'nan')
		and m2.parent = 'nan'
	)
) m group by 1 order by 2 desc; -- all 418 are parents


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
							select distinct parent from meme m3 where parent != 'nan'
						)
						and m2.parent = 'nan' -- 124
					) -- 418
				) -- 120
			) -- 185
		) -- 32
	) -- 6
) -- 0



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


-- find the most ppopular tags
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
) ptt
group by 1 order by 2 desc;



-- count all links
select count(*) from (
	select 'about' as typ, mal.* from meme_about_link mal 
	union all
	select 'origin' as typ, mol.* from meme_origin_link mol 
	union all
	select 'spread' as typ, msl.* from meme_spread_link msl 
	union all
	select 'notex' as typ, mnl.* from meme_notex_link mnl 
	union all
	select 'searchint' as typ, msl2.* from meme_searchint_link msl2 
	union all
	select 'extref' as typ, mel.* from meme_extref_link mel 
) unlinks; -- 106376

select count(*) from (
	select distinct meme_url,link from (
		select 'about' as typ, mal.* from meme_about_link mal 
		union all
		select 'origin' as typ, mol.* from meme_origin_link mol 
		union all
		select 'spread' as typ, msl.* from meme_spread_link msl 
		union all
		select 'notex' as typ, mnl.* from meme_notex_link mnl 
		union all
		select 'searchint' as typ, msl2.* from meme_searchint_link msl2 
		union all
		select 'extref' as typ, mel.* from meme_extref_link mel 
	) unlinks
) onlyurlandlinks; -- 105543

select count(*) from (
	select distinct link_name,link from (
		select 'about' as typ, mal.* from meme_about_link mal 
		union all
		select 'origin' as typ, mol.* from meme_origin_link mol 
		union all
		select 'spread' as typ, msl.* from meme_spread_link msl 
		union all
		select 'notex' as typ, mnl.* from meme_notex_link mnl 
		union all
		select 'searchint' as typ, msl2.* from meme_searchint_link msl2 
		union all
		select 'extref' as typ, mel.* from meme_extref_link mel 
	) unlinks
) onlylinksandnames; -- 68816

select count(*) from (
	select distinct link from (
		select 'about' as typ, mal.* from meme_about_link mal 
		union all
		select 'origin' as typ, mol.* from meme_origin_link mol 
		union all
		select 'spread' as typ, msl.* from meme_spread_link msl 
		union all
		select 'notex' as typ, mnl.* from meme_notex_link mnl 
		union all
		select 'searchint' as typ, msl2.* from meme_searchint_link msl2 
		union all
		select 'extref' as typ, mel.* from meme_extref_link mel 
	) unlinks
) onlylinks; -- 64534

select count(*) from ( 
	select link,count(distinct(typ)) from (
		select 'about' as typ, mal.* from meme_about_link mal 
		union all
		select 'origin' as typ, mol.* from meme_origin_link mol 
		union all
		select 'spread' as typ, msl.* from meme_spread_link msl 
		union all
		select 'notex' as typ, mnl.* from meme_notex_link mnl 
		union all
		select 'searchint' as typ, msl2.* from meme_searchint_link msl2 
		union all
		select 'extref' as typ, mel.* from meme_extref_link mel 
	) unlinks
	group by 1
	having count(distinct(typ))>1
) a; -- 2114 repeating links


select link,count(distinct(meme_url)) from (
	select 'about' as typ, mal.* from meme_about_link mal 
	union all
	select 'origin' as typ, mol.* from meme_origin_link mol 
	union all
	select 'spread' as typ, msl.* from meme_spread_link msl 
	union all
	select 'notex' as typ, mnl.* from meme_notex_link mnl 
	union all
	select 'searchint' as typ, msl2.* from meme_searchint_link msl2 
	union all
	select 'extref' as typ, mel.* from meme_extref_link mel 
) unlinks
group by 1 order by 2 desc;

/*
https://knowyourmeme.com/memes/sites/youtube	2188
https://knowyourmeme.com/memes/sites/reddit	1665
https://knowyourmeme.com/memes/sites/tumblr	1529
https://knowyourmeme.com/memes/cultures/the-internet	1452
https://knowyourmeme.com/memes/sites/twitter	1392
https://knowyourmeme.com/memes/sites/facebook	1299
https://knowyourmeme.com/memes/sites/4chan	1110
https://knowyourmeme.com/memes/image-macros	1019
*/

select * from (
	select 'about' as typ, mal.* from meme_about_link mal 
	union all
	select 'origin' as typ, mol.* from meme_origin_link mol 
	union all
	select 'spread' as typ, msl.* from meme_spread_link msl 
	union all
	select 'notex' as typ, mnl.* from meme_notex_link mnl 
	union all
	select 'searchint' as typ, msl2.* from meme_searchint_link msl2 
	union all
	select 'extref' as typ, mel.* from meme_extref_link mel 
) unlinks limit 50;

select * from meme m limit 30;

