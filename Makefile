
felony_convictions.csv : all.jl
	cat $< | python scripts/felony_convictions.py > $@

all.jl : 2000_year.jl 2001_year.jl 2002_year.jl 2003_year.jl 2004_year.jl \
         2005_year.jl 2006_year.jl 2007_year.jl 2008_year.jl 2009_year.jl \
         2010_year.jl 2011_year.jl 2012_year.jl 2013_year.jl 2014_year.jl \
         2015_year.jl 2016_year.jl 2017_year.jl 2018_year.jl 2019_year.jl \
         2020_year.jl 2021_year.jl 2022_year.jl	2023_year.jl 2024_year.jl
	cat $^ > $@

%_year.jl :
	scrapy crawl criminal -a year=$* -O $@
