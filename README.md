# Weather Data Project Setup for Fashion trend
Data Engineering project 
Our data science team is developing a model to interpret and predict the impacts of weather on fashion purchases.  In addition to all of the ecom and customer data we already have, they will require climate and weather data to properly train their models.

They have specifically requested the following data as a minimum starting point to test their models, but are open to suggestions for other data that is discovered along the way to help train their models:
•	Zip code level daily weather statistics for the Boston Metro Area:
•	Min temp
•	Max temp
•	Precipitation type
•	Precipitation amount 
•	Climate norms for the Boston Metro Area:
•	Average min temp
•	Average max temp
•	Average precipitation type
•	Average precipitation amount

The data science team also requires the ability to easily join climate data with order and customer data, and would prefer a single source for all climate data that can easily be joined to customer and order data (see the example customer_order schema to help with your design)

Their models will be trained on a minimum of 1 year of historical data broken into fall/winter and spring/summer seasons.

Upon moving to production, the model will be trained weekly and predict daily, which means your initial design should be flexible and consider the future data movement needs of the production pipeline.

Your Mission
•	Develop a robust data pipeline to programmatically collect and land as much of the necessary weather & climate data into a snowflake database as possible
•	The NCDC is a good starting point for data: https://www.ncdc.noaa.gov/cdo-web/ 
•	Create a test Snowflake account and spin up a database, warehouse, stage, and table(s) to land all of the data you are collecting
•	You can find all the details and documentation here: https://docs.snowflake.com/en/user-guide/admin-trial-account.html
•	We recommend sizing your warehouse as XS with a single core to keep credit burn under control so you don’t burn through all your free credits
•	There is a handy python snowflake connector: https://pypi.org/project/snowflake-connector-python/ 
•	Integrate the necessary climate data as per the requirements of the data science team and create a schema and table(s) to allow for the most efficient joining of data with the customer_order schema 

•	Coding requirements
•	Python and SQL are required
•	Code must contain appropriate logging to facilitate troubleshooting
•	All code should be written so that it can be orchestrated via a typical production schedule manager
•	Code should adhere to the pep8 python and ansi sql standards as much as possible 
