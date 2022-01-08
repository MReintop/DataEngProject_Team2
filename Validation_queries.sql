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
where title is null; -- 0

-- count images
select count(distinct(image)) from meme m; -- 12621
select * from meme m 
where image is null; -- 0

-- social_media_description
-- most frequent length is 156, count 1521
select length(social_media_description),count(*) from meme m 
group by 1 order by 2 desc;
-- the longest length is 3328, count 1
select length(social_media_description),count(*) from meme m 
group by 1 order by 1 desc;
-- the shortest length is 0, count 16
select length(social_media_description),count(*) from meme m 
group by 1 order by 1;
-- examples
select * from meme m 
where length(social_media_description)=0;

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
where parent is null; -- 7311
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
where meme_url is null; -- 0


/*VALIDATE MEME_TYPE*/
select * from meme_type mt 
where type is null; -- 0
select * from meme_type mt 
where type is null; -- 0
select type,count(distinct(meme_url)) from meme_type mt 
group by 1 order by 2 desc; 
-- 34 different types
-- the most popular is character, count 332
-- the less popular is lip-dub, count 1


/*VALIDATE MEME_TEXT*/
select * from meme_text mt limit 20;
select count(distinct(meme_url)) from meme_text mt 
where about_text = ''; -- 5041
select count(distinct(meme_url)) from meme_text mt 
where origin_text = ''; -- 5524
select count(distinct(meme_url)) from meme_text mt 
where spread_text = ''; -- 6143
select count(distinct(meme_url)) from meme_text mt 
where not_examples_text = ''; -- 12219
select count(distinct(meme_url)) from meme_text mt 
where search_intr_text = ''; -- 11439
select count(distinct(meme_url)) from meme_text mt 
where external_ref_text = ''; -- 6422

/*VALIDATE MEME_LINK*/
select * from meme_link limit 20;
select * from meme_link ml 
where meme_url is null;
select * from meme_link ml 
where meme_url = '';
select * from meme_link ml 
where link is null;
select * from meme_link ml 
where link = '';
select * from meme_link ml 
where link_title is null;
select * from meme_link ml 
where link_title = '';
select section, count(*) from meme_link ml 
group by 1 order by 2 desc;
select section, count(distinct(meme_url)) from meme_link ml 
group by 1 order by 2 desc;
select section, count(distinct(link)) from meme_link ml 
group by 1 order by 2 desc;

-- if no text, then no links for ABOUT
select * from (
	select meme_url from meme_text mt 
	where about_text = ''
) txt
left join meme_link ml 
on ml.meme_url = txt.meme_url
and ml.section = 'about'
where ml.meme_url is not null; -- 0

-- ORIGIN
select * from (
	select meme_url from meme_text mt 
	where origin_text = ''
) txt
left join meme_link ml 
on ml.meme_url = txt.meme_url
and ml.section = 'Origin'
where ml.meme_url is not null; -- 1

-- SPREAD
select * from (
	select meme_url from meme_text mt 
	where spread_text = ''
) txt
left join meme_link ml 
on ml.meme_url = txt.meme_url
and ml.section = 'Spread'
where ml.meme_url is not null; -- 0

-- NOTABLE EXAMPLES
select * from (
	select meme_url from meme_text mt 
	where not_examples_text = ''
) txt
left join meme_link ml 
on ml.meme_url = txt.meme_url
and ml.section = 'Notable Examples'
where ml.meme_url is not null; -- 12

-- SEARCH INTEREST
select * from (
	select meme_url from meme_text mt 
	where search_intr_text = ''
) txt
left join meme_link ml 
on ml.meme_url = txt.meme_url
and ml.section = 'Search Interest'
where ml.meme_url is not null; -- 0

-- EXTERNAL REF
select * from (
	select meme_url from meme_text mt 
	where external_ref_text = ''
) txt
left join meme_link ml 
on ml.meme_url = txt.meme_url
and ml.section = 'External Reference'
where ml.meme_url is not null; -- 0


/*VALIDATE MEME_IMAGE*/
select * from meme_image mi limit 20;

select * from meme_image mi 
where image_link = ''; -- 0

select * from meme_image mi 
where image_link is null; -- 0


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

/*VALIDATE LINK*/
select * from link l limit 100;

select category,count(url) from link l 
group by 1 order by 2 desc;

select * from (
	select * from link l 
	where l.category ='Meme'
) m2
left join meme m 
on m2.url = m.url 
where m.url is null;

select count(distinct(url)) from link l 
where category = 'Meme';

/*
Other	60826
Meme	12654
Subculture	1233
Person	1174
Event	1116
Site	380
Culture	190
*/