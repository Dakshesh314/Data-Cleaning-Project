-- DATA CLEANING

select *
from layoffs
;

-- 1. Remove duplicates
-- 2. Standardize the data 
-- 3. Null Values or Blank Values
-- 4. Remove any columns

CREATE TABLE layoffs_staging
LIKE layoffs;
 
 SELECT * 
 FROM layoffs_staging ;
 
 INSERT layoffs_staging 
 SELECT *
 FROM layoffs ;

 SELECT * 
 FROM layoffs ;
 
SELECT * 
FROM layoffs_staging ;

Select *, 
Row_number() over (partition by company, location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) row_num
from layoffs_staging ;

with duplicate_cte as 
(
Select *, 
Row_number() over (partition by company, location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) row_num
from layoffs_staging
) 
SELECT * 
FROM duplicate_cte
where row_num > 1;

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select *
from layoffs_staging2;

insert into layoffs_staging2
Select *, 
Row_number() over (partition by company, location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) row_num
from layoffs_staging ;

select *
from layoffs_staging2
where row_num > 1;

delete 
from layoffs_staging2
where row_num > 1;

select *
from layoffs_staging2
;

-- standardizing data 

select company
from layoffs_staging2;

select distinct(company), trim(company)
from layoffs_staging2;

update layoffs_staging2
set company = trim(company);


select *
from layoffs_staging2;

select distinct industry
from layoffs_staging2
order by industry asc;

select *
from layoffs_staging2
where industry like 'Crypto%';

update layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%'; 
 
select distinct location
from layoffs_staging2
order by 1;

select distinct country
from layoffs_staging2
order by 1;


select *
from layoffs_staging2
where country like 'United States%'
order by country asc;

update layoffs_staging2
set country = 'United States'
where country like 'United States%';

select `date`, str_to_date(`date`, '%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y')
where `date`;

alter table layoffs_staging2
modify column `date` DATE;

select *
from layoffs_staging2
where company = 'Airbnb'; 

select * 
from layoffs_staging2 
where industry is null or industry = "";

update layoffs_staging2
set industry = null
where industry = '';


select t1.company, t2.company, t1.location, t2.location, t1.industry,t2.industry
from layoffs_staging2 as t1
join layoffs_staging2 as t2 
	on t1.company = t2.company
	and  t1.location = t2.location
where t1.industry is null 
;

update layoffs_staging2 t1 
join layoffs_staging2 t2
on t1.company = t2.company
	and  t1.location = t2.location
set t1.industry = t2.industry
where t1.industry is null 
and t2.industry is not null;


select *
from layoffs_staging2
where total_laid_off is null and percentage_laid_off is null;


delete 
from layoffs_staging2
where total_laid_off is null and percentage_laid_off is null;


select * 
from layoffs_staging2
;

alter table layoffs_staging2
drop column row_num;
























































