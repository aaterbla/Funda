/* 
 * This file contains code for creating usable tables from the original csv source files for Movie Streaming analyses
 */

-- Create new database for movie analysis
create schema funda;
drop table if exists funda.ratings;
create table funda.movies
( movieId	integer
, title 	varchar(200)
, genres	varchar(80)
, PRIMARY KEY (movieId));

create table funda.ratings
( userId	integer
, movieId	integer
, rating	float
, time_rate integer);

create table funda.tags
( userId	integer
, movieId	integer
, tag		varchar(300)
, time_tag 	integer);

-- Check all the data is imported
select count(rating) from funda.ratings;
select count(tag) from funda.tags;--good
select count(movieid) from funda.movies; --good
select count(distinct(movieid)) from funda.movies; --no duplicates

-- 1 -- Transform the ratings table --
drop table if exists funda.a_ratings;
create table funda.a_ratings as (select
a.userId	
, a.movieId	
, a.rating
, date_part('year',time_rate) as year_rate -- because data so large, grouping by year for now, can make months for more granularity
from (select userId	, movieId, rating, to_timestamp("timestamp") as time_rate from funda.ratings) a); --convert UTC to timestamp
-- Basic checks
select max(rating) from funda.a_ratings; --good
select min(rating) from funda.a_ratings; --good
	
-- 2 -- Transform the tags table --
drop table if exists funda.a_tags;
create table funda.a_tags as (select
a.userid userid_t 
, a.movieid movieid_t
, a.tag 
, date_part('year',time_tag) as year_tag -- because data so large, grouping by year for now, can make months for more granularity
from (select userid, movieid, tag, to_timestamp("timestamp") as time_tag from funda.tags) a); --convert UTC to timestamp

-- 3 -- Transform the movies table to focus on Genres --
-- Function to convert string to integer:
CREATE OR REPLACE FUNCTION isnumeric(text) RETURNS BOOLEAN AS $$
DECLARE x NUMERIC;
BEGIN
    x = $1::NUMERIC;
    RETURN TRUE;
EXCEPTION WHEN others THEN
    RETURN FALSE;
END;
$$
STRICT
LANGUAGE plpgsql IMMUTABLE;
---------
/*
Given info: 18 genres total. 
Need to create one-to-many relationship when doing analysis on genres, therefore unnest.
After unnesting, the YOR needs to be converted from string to int to represent years.
*/
drop table tmp_movie;
create temp table tmp_movie as (
select movieId
, title
, left(right(title,5),4) as YOR --extract release year from the back of the string
, unnest(string_to_array(genres, '|')) as genre 
from funda.movies);
-- Check YOR data:
select *
from tmp_movie order by yor desc; 
/* Following 4 movies' YOR not accurately extracted:
 * movieid	yor
175335	2017
172451	2013
107434	2009
98063	1983
 */
update tmp_movie set yor= '2017' where movieid = 175335;
update tmp_movie set yor = '2013' where movieid = 172451;
update tmp_movie set yor = '2009' where movieid = 107434;
update tmp_movie set yor = '1983' where movieid = 98063;
-- I probably could create a special function or subrstring regexp to do this more eloquently, but assessed this as my fastest option.
-- Chose not to split because there are many parentheses used throughout the title field
-- Final step to create movies with all genres table:
drop table if exists funda.a_movies_all_genres;
create table funda.a_movies_all_genres as (
select movieId
, title
, case when isnumeric(yor) = true then yor::int else null end as yor_clean -- convert to int
, genre
from tmp_movie
);
-- Do some basic checks
select * from funda.a_movies_all_genres order by yor_clean desc; 
select count(distinct(movieid)) from funda.a_movies_all_genres; -- 58098 unique movies



