**Setting up your Environment:**
1. Install php and Mysql on your machine
2. In your project web root directory, clone the repo

`git clone https://github.com/dgroene/GSA-Analytics-Merge.git`



**Usage:**

1. DB Credentials are configured in **config.php**
2. Program data is available in **data/programs.csv**
3. Database and tables are cretaed in **install.php**
4. Program data is imported to mysql using **import_analytics.php**
5. Program data with analytics is exported from mysql to csv file in **export_analytics.php**
6. Run **program_analytics.php** to read and merge program data and 30 day view counts and export them to **data/program_analytics.csv**

