# Customer_Call_Duration-Redis-
This program is designed to process customer call information using python, redis and postgressSQL. It is a pipeline that consists of several functions
The extract function extacts data from the source and stores it in a cloud instance redis cache
The transform function collects data from the redis cache converts the dat into a dataframe, carries out data cleaning and transformation process.
The load function loads the transformed data into postgres sql where it can be accessed from.
