select country, count(country) from profile where country<>"" group by "country" order by count(country);
