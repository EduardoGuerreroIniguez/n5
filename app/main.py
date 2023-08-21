from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import pandas as pd


################# Create a Spark session ################
spark = SparkSession.builder.appName("Load_Files").getOrCreate()

################## Class Pipeline #####################
class Pipeline:
    '''
    This class permit Extract, Transform and Load data give a source and transform it and load for specific analyze
    '''
    ROOT_DATA_PATH = '../data/'
    RAW_DATA_PATH_SPARK = '../data/raw/spark/'
    RAW_DATA_PATH_PANDAS = '../data/raw/pandas'
    SPARK_TO_PARQUET_PATH = '/challenge/parquets/'
    PANDAS_TO_PARQUET_PATH = '/challenge/pandas/parquets/'
    SILVER_ZONE = '../data/silver/spark/'
    
    def __init__(self, file_name, root_data_path = ROOT_DATA_PATH, file_format='csv',type_load='spark'):
        self.root_data_path = root_data_path
        self.file_name = file_name
        self.type_load = type_load
        self.file_format=file_format

    def extract(self, default_schema=StructType([]),save=False):
        '''
        This function take data from sources and load data in a raw zone. In the challenge ask me to read data in spark and pandas dataframe format. This explanation
        is confused but I try to read data in both forms.
        '''
        if self.type_load == 'spark':
            if default_schema != StructType([]): 
                df = spark.read.format(self.file_format).option("header", "true").schema(default_schema).load(self.root_data_path+self.file_name+'.'+self.file_format)
            else:
                df = spark.read.format(self.file_format).option("header", "true").load(self.root_data_path+self.file_name+'.'+self.file_format)    
            if save:
                print('>>>>>Save raw data')
                self.save_data(df,self.RAW_DATA_PATH_SPARK,self.type_load)
            return df
        elif self.type_load == 'pandas':
            pandas_df = pd.read_csv(self.root_data_path+self.file_name+'.'+self.file_format)
            spark_df = spark.createDataFrame(pandas_df)
            if save:
                self.save_data(spark_df,self.RAW_DATA_PATH_PANDAS,self.type_load)
            return spark_df
    
    def transform(self, file_name,schema=StructType([]), save=False):
        '''
        This function take data from raw zone and apply schema and save the result in silver zone if the save parameter is set True
        '''
        df=spark.read.parquet(self.RAW_DATA_PATH_SPARK+file_name,schema=schema)
        if schema != StructType([]) and save:
            df.write.parquet(self.SILVER_ZONE+file_name,mode='overwrite')

        return df
    
    def save_data(self, df,path, flag = 'spark'):
        if flag == 'spark':
            df.write.mode("overwrite").parquet(path+self.file_name)
        else:
            df.write.mode("overwrite").parquet(self.PANDAS_TO_PARQUET_PATH+self.file_name)


########## Schema Sections ##############################
day_wise_schema = StructType([
                                StructField("date", StringType(), nullable=False),
                                StructField("confirmed", IntegerType(), nullable=True),
                                StructField("deaths", IntegerType(), nullable=True),
                                StructField("recovered", IntegerType(), nullable=True),
                                StructField("active", IntegerType(), nullable=True),
                                StructField("new_cases", IntegerType(), nullable=True),
                                StructField("new_deaths", IntegerType(), nullable=True),
                                StructField("new_recovered", IntegerType(), nullable=True),
                                StructField("deaths/100 cases", FloatType(), nullable=True),
                                StructField("recovered/100 cases", FloatType(), nullable=True),
                                StructField("deaths/100 recovers", FloatType(), nullable=True),
                                StructField("no. of countries", IntegerType(), nullable=True)
                            ])  

covid_19_clean_schema = StructType([
                                StructField("Province/State",StringType(), nullable=False),
                                StructField("Country/Region",StringType(), nullable=False),
                                StructField("Latitude of the location",FloatType(), nullable=False),
                                StructField("Longitude of the location",FloatType(), nullable=False),
                                StructField("Country/Region",StringType(), nullable=False),
                                StructField("Date",StringType(), nullable=False),
                                StructField("Confirmed",IntegerType(), nullable=False),
                                StructField("Deaths",IntegerType(), nullable=False),
                                StructField("Recovered",IntegerType(), nullable=False),
                                StructField("Active",IntegerType(), nullable=False),
                                StructField("WHO Region",StringType(), nullable=False)
                                ])

country_wise_latest_schema = StructType([
                                StructField("Country/Region",StringType(), nullable=False),
                                StructField("Confirmed",IntegerType(), nullable=False),
                                StructField("Deaths",IntegerType(), nullable=False),
                                StructField("Recovered",IntegerType(), nullable=False),
                                StructField("Active",IntegerType(), nullable=False),
                                StructField("new_cases", IntegerType(), nullable=True),
                                StructField("new_deaths", IntegerType(), nullable=True),
                                StructField("new_recovered", IntegerType(), nullable=True),
                                StructField("deaths/100 cases", FloatType(), nullable=True),
                                StructField("recovered/100 cases", FloatType(), nullable=True),
                                StructField("deaths/100 recovers", FloatType(), nullable=True),
                                StructField("Confirmed last week",IntegerType(), nullable=False),
                                StructField("1 week change",IntegerType(), nullable=False),
                                StructField("1 week % increase", FloatType(), nullable=False),
                                StructField("WHO Region",StringType(), nullable=False)
                                        ])

full_grouped_schema = StructType([
                                StructField("date", StringType(), nullable=False),
                                StructField("Country/Region",StringType(), nullable=False),
                                StructField("Confirmed",IntegerType(), nullable=False),
                                StructField("Deaths",IntegerType(), nullable=False),
                                StructField("Recovered",IntegerType(), nullable=False),
                                StructField("Active",IntegerType(), nullable=False),
                                StructField("new_cases", IntegerType(), nullable=True),
                                StructField("new_deaths", IntegerType(), nullable=True),
                                StructField("new_recovered", IntegerType(), nullable=True),
                                StructField("WHO Region",StringType(), nullable=False)
                                ])

usa_country_wise_schema = StructType([
                                StructField("UID",IntegerType(), nullable=False), 
                                StructField("ISO2", StringType(), nullable=False),
                                StructField("ISO3", StringType(), nullable=False),
                                StructField("Code3", IntegerType(), nullable=True),
                                StructField("FIPS", FloatType(), nullable=False),
                                StructField("Admin2",StringType(), nullable=False),
                                StructField("Province/State",StringType(), nullable=False),
                                StructField("Country/Region",StringType(), nullable=False),
                                StructField("Lat",FloatType(), nullable=False),
                                StructField("Lon",FloatType(), nullable=False),
                                StructField("Combined_key",StringType(), nullable=False),
                                StructField("date", StringType(), nullable=False),
                                StructField("Confirmed",IntegerType(), nullable=False),
                                StructField("Deaths",IntegerType(), nullable=False)
])

worldometer_data_schema = StructType([StructField("Country/Region",StringType(), nullable=False),
                                      StructField("Continent",StringType(), nullable=False),
                                      StructField("Population",IntegerType(), nullable=False),
                                      StructField("Total_Case",IntegerType(), nullable=False),
                                      StructField("New_Cases",IntegerType(), nullable=False),
                                      StructField("Total_Deaths",IntegerType(), nullable=False),
                                      StructField("New_Deaths",IntegerType(), nullable=False),
                                      StructField("Total_Recovered",IntegerType(), nullable=False),
                                      StructField("New_Recovered",IntegerType(), nullable=False),
                                      StructField("ActiveCases",IntegerType(), nullable=False),
                                      StructField("Serious/Critical",IntegerType(), nullable=False),
                                      StructField("Tot_Cases/1M",FloatType(), nullable=False),
                                      StructField("Deaths_Cases/1M",FloatType(), nullable=False),
                                      StructField("Total_Test",IntegerType(), nullable=False),
                                      StructField("Test/1M",FloatType(), nullable=False),
                                      StructField("WHO Region",StringType(), nullable=False)
                                      ])


################# Load source data and put it in raw zone using spark ################

day_wise = Pipeline(file_name='day_wise')
df_day_wise = day_wise.extract()

covid_19_clean = Pipeline(file_name='covid_19_clean_complete')
df_covid_19_clean = covid_19_clean.extract()

country_wise_latest = Pipeline(file_name='country_wise_latest')
df_country_wise_latest = country_wise_latest.extract()


full_grouped = Pipeline(file_name='full_grouped')
df_full_grouped = full_grouped.extract()


usa_country_wise = Pipeline(file_name='usa_county_wise')
df_usa_country_wise = usa_country_wise.extract()


worldometer_data = Pipeline(file_name='worldometer_data')
df_worldometer_data = worldometer_data.extract()

################# transform and load data in silver zone using spark ################

day_wise = day_wise.transform(file_name='day_wise', schema = day_wise_schema, save =True)

covid_19_clean=covid_19_clean.transform(file_name = 'covid_19_clean_complete', schema = covid_19_clean_schema, save=True)

country_wise_latest=country_wise_latest.transform(file_name = 'country_wise_latest', schema = country_wise_latest_schema, save=True)

full_grouped=full_grouped.transform(file_name = 'full_grouped', schema = full_grouped_schema, save=True)

usa_country_wise=usa_country_wise.transform(file_name = 'usa_county_wise', schema = usa_country_wise_schema, save=True)

worldometer_data=worldometer_data.transform(file_name = 'worldometer_data', schema = worldometer_data_schema, save=True)
#worldometer_data.show()

############## Show Load Data (In this part Spark working in parallel) ####################

#df_day_wise.show()
#df_covid_19_clean.show()
#df_country_wise_latest.show()
#df_full_grouped.show()
#df_usa_country_wise.show()
#df_worldometer_data.show()

############# Load data using pandas #########################

# day_wise = Pipeline(file_name='day_wise', type_load='pandas')
# spark_df_day_wise = day_wise.extract()
# # spark_df_day_wise.show()

# covid_19_clean = Pipeline(file_name='covid_19_clean_complete', type_load='pandas')
# spark_df_covid_19_clean = covid_19_clean.extract()
# #dspark_f_covid_19_clean.show()

# country_wise_latest = Pipeline(file_name='country_wise_latest', type_load='pandas')
# spark_df_country_wise_latest = country_wise_latest.extract()
# #spark_df_country_wise_latest.show()

# full_grouped = Pipeline(file_name='full_grouped', type_load='pandas')
# spark_df_full_grouped = full_grouped.extract()
# #spark_df_full_grouped.show()

# usa_country_wise = Pipeline(file_name='usa_county_wise', type_load='pandas')
# spark_df_usa_country_wise = usa_country_wise.extract()
# #spark_df_usa_country_wise.show()

# worldometer_data = Pipeline(file_name='worldometer_data', type_load='pandas')
# spark_df_worldometer_data = worldometer_data.extract()
#spark_df_worldometer_data.show()


# Stop the Spark session when you're done
spark.stop()