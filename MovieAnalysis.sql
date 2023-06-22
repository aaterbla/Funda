/* This file contains queries used for the analyses of the movie streaming dataset
 */
-- Tables created through transforming original csv files in CreateDatabase.sql
select * from funda.a_movies_all_genres;
select * from funda.movies;
select * from funda.a_tags;
select * from funda.a_ratings;

-- checking rating years	
select * from funda.a_ratings order by year_rate desc; -- ratings from 1995-2018
-- checking if users rated 1 movie more than once -- NO
select userid user_r, movieid mov_r, count(rating) num from funda.a_ratings group by 1,2 order by num desc 

-- Count of movies in each genre
select a.genre, count(distinct(a.movieid)) ct_mov from funda.a_movies_all_genres a group by 1 order by 2 desc;

-- Count of movies tagged over time
select year_tag, count(distinct(movieid_t)) 
from funda.a_tags group by 1;

-- Create table with rating stats that include movie metadata
drop table if exists funda.rating_stats;
create table funda.rating_stats as (
select 
 a.movieid
, b.title
, c.yor_clean
, avg(a.rating) as av_rating
, max(a.rating) as max_rating
, min(a.rating) as min_rating
, sum(a.rating) as sum_rating
, median(a.rating) as med_rating 
, count(distinct(a.userid)) as ct_users -- amount of users that rated movie
from funda.a_ratings a
left join funda.movies b on a.movieid = b.movieid
left join (select movieid, yor_clean from funda.a_movies_all_genres) c on a.movieid = c.movieid 
group by 1,2,3
);

-- Movies with the most amount of ratings
select * from funda.rating_stats rs 
order by rs.ct_users desc limit 20;
-- Movies with highest ave rating
select * from funda.rating_stats rs 
where ct_users >= 10000 
order by rs.av_rating desc 
limit 20;

-- std of ratings 
select 
 a.movieid
, b.title
, c.yor_clean
,stddev(a.rating) as std_rating
from funda.a_ratings a
left join funda.movies b on a.movieid = b.movieid
left join (select movieid, yor_clean from funda.a_movies_all_genres) c on a.movieid = c.movieid 
group by 1,2,3;

-- Create function to calculate Median, because no such function in Postgresql
create or replace function final_median(anyarray) returns float8 as
$$
declare
	cnt integer;
begin
	cnt := (select count(*) from unnest($1) val
	where val is not null);
return (select avg(tmp.val)::float8 from(select val from unnest($1) val 
	where val is not null 
	order by 1
	limit 2 - mod(cnt, 2)
	offset ceil(cnt/2.0) - 1) as tmp ); end
$$ language plpgsql;
create aggregate median(anyelement) (
sfunc=array_append,
stype=anyarray,
finalfunc=final_median,
initcond='{}');

-- Investigating "no genres listed" category for insights
select movieid_t, tag, year_tag, c.yor_clean 
from funda.a_tags a
left join (select * from funda.a_movies_all_genres where genre = '(no genres listed)') b
	on a.movieid_t = b.movieid
left join (select distinct(movieid),yor_clean from funda.a_movies_all_genres ) c
	on a.movieid_t = c.movieid 
where tag = '007' or tag like 'Bond' 
order by c.yor_clean;

-- How many genres are tagged per movie
select distinct(movieid), count(genre) from funda.a_movies_all_genres  group by 1 order by 2 desc; -- up to 10

-- Create table for highly rated movies (more than 4 median rating)
drop table if exists funda.best_movies;
create table funda.best_movies as(
select * from funda.rating_stats rs 
left join (select movieid movieid_gen, title title_gen, genre from funda.a_movies_all_genres) t on rs.movieid = t.movieid_gen
where rs.med_rating >= 4
);
-- Rank the top movies by genre from high to low -- "No genres" and "Children" are top 2
select genre, avg(med_rating) avg_med_rating, sum(sum_rating) sum_ratings 
from funda.best_movies 
group by 1
order by 2 desc;

-- Average number of ratings per user to measure engagement over time
select year_rate, count(movieid) ct_mov, count(distinct(userid)) ct_user, count(distinct(movieid))/count(distinct(userid))::float ave_num_rating_p_cust from funda.a_ratings group by 1

-- Customer engagement with genres
select  a.year_rate, b.genre, count(a.movieid) ct_mov, count(distinct(a.user_r)) ct_user, count(a.movieid)/count(distinct(a.user_r))::float ave_num_rating_p_cust 
from funda.a_ratings a
left join funda.a_movies_all_genres b 
on a.mov_r = b.movieid
--where a.yor_clean = 2005

-- Customer engagement with specific genre
select a.genre, b.year_r, count(a.movieid) ct_mov, count(distinct(b.user_r)) ct_user, count(a.movieid)/count(distinct(b.user_r))::float ave_num_rating_p_cust
from funda.a_movies_all_genres a
left join (select userid user_r, movieid mov_r, year_rate year_r from funda.a_ratings) b
on a.movieid = b.mov_r
where genre = 'Documentary' and year_r is not null
group by 1,2;

-- Customer engagement over time
select year_rate, count(movieid) ct_mov, count(distinct(userid)) ct_user, count(movieid)/count(distinct(userid)) ave_num_rating_p_cust from funda.a_ratings group by 1;
-- Normalising customer engagement for growth in movie content
select year_rate, count(movieid) ct_mov, count(distinct(userid)) ct_user, (count(movieid)/count(distinct(userid)))/(count(distinct(movieid)))::float ave_num_rating_p_cust_norm from funda.a_ratings group by 1;

-- Investigating that many users rated in 1996 before digital streaming
select year_rate, 
count(movieid) ct_mov, 
count(distinct(userid)) ct_user, 
count(movieid)/count(distinct(userid)) ave_num_rating_p_cust 
from funda.a_ratings 
where year_rate = 1996
group by 1;
-- Are the same users who rated in 1996 still active in 2018? -- NO
select ar.userid, ar2.userid, ar.movieid, ar.year_rate, ar2.year_rate
from funda.a_ratings ar 
inner join (select userid, movieid, year_rate  from funda.a_ratings ar where year_rate = 2018) ar2 on ar.userid = ar2.userid
where ar.year_rate = 1996; 
-- count of movies, users in 1996
select ar.year_rate, count(distinct(ar.movieid)) ct_mov, count(distinct(ar.userid)) ct_user from funda.a_ratings ar where ar.year_rate = 1996 group by 1; 
-- count of movies, users in 2018
select ar2.year_rate, count(distinct(ar2.movieid)) ct_mov, count(distinct(ar2.userid)) ct_user from funda.a_ratings ar2 where ar2.year_rate = 2018 group by 1;

-- Are the same users who rated in 2013 still active 2014-2018 to analyse burn rate (churn)
select ar.year_rate recent5yrs, count(distinct(ar.userid)) ct_users, count(distinct(ar.movieid)) ct_movies
from funda.a_ratings ar 
inner join (select distinct(ar.userid), ar.movieid, ar.year_rate
		from funda.a_ratings ar 
		where ar.year_rate = 2013) ar2 
		on ar.userid = ar2.userid
where ar.year_rate in (2013,2014,2015,2016,2017,2018)
group by 1; 



