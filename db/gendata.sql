select country,  count(country) from profile where country<>"" group by "country" order by count(country) desc;

select city, count(city) from profile where country<>"" group by "city" order by count(city) desc;
