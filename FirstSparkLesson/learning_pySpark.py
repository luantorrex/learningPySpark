#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pyspark
from pyspark.sql import SparkSession


# In[2]:


spark = SparkSession.builder.appName("ReadwriteVal").getOrCreate()
spark


# In[3]:


# Get the number of cores
cores = spark._jsc.sc().getExecutorMemoryStatus().keySet().size()
cores


# In[4]:


# Read and show csv file
path = "Datasets/ReadWriteValidate/"

students = spark.read.csv(path+'students.csv',inferSchema=True,header=True)
students.limit(4).toPandas()


# In[5]:


# Read and show parquet file

parquet = spark.read.parquet(path+'users1.parquet')
parquet.limit(4).toPandas()


# In[6]:


# Read all parquet files that starts with 'users'

partitioned = spark.read.parquet(path+'users*')
partitioned.limit(4).toPandas()


# In[7]:


# Read and show parquet file 'users1' and 'users2'

users1_2 = spark.read.option('bathPath', path).parquet(path+'users1.parquet', path+'users2.parquet')
users1_2.limit(4).toPandas()


# ## Validating Data

# In[8]:


students.printSchema()


# In[9]:


students.columns


# In[10]:


students.describe()


# In[11]:


students.schema['math score'].dataType


# In[12]:


students.select('math score', 'reading score').summary('count', 'max', 'min').show()


# ## How to specify data types

# In[13]:


from pyspark.sql.types import *


# In[14]:


data_schema = [StructField('name', StringType(), True),
               StructField('email', StringType(), True),
               StructField('city', StringType(), True),
               StructField('mac', StringType(), True),
               StructField('timestamp', DateType(), True),
               StructField('creditcard', StringType(), True)]


# In[15]:


final_struc = StructType(fields = data_schema)


# In[16]:


people = spark.read.json(path+'people.json', schema=final_struc)


# In[17]:


people.limit(4).toPandas()


# In[18]:


people.printSchema()


# ## Writing in Data

# In[19]:


students.write.mode('overwrite').csv('write_test.csv')


# In[20]:


from py4j.java_gateway import java_import
java_import(spark._jvm, 'org.apache.hadoop.fs.Path')

fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
file = fs.globStatus(spark._jvm.Path('write_test.csv/part*'))[0].getPath().getName()
fs.rename(spark._jvm.Path('write_test.csv/' + file), spark._jvm.Path('write_test2.csv'))
fs.delete(spark._jvm.Path('write_test.csv'), True)


# In[21]:


users1_2.write.mode('overwrite').parquet('parquet/')


# In[22]:


# Partitioning

users1_2.write.mode('overwrite').partitionBy('gender').parquet('part_parquet/')


# In[23]:


values = [('Pear',10),('Orange',13),('Peach',5)]
df = spark.createDataFrame(values,['fruit','quant'])


# In[24]:


df.show()


# In[ ]:




