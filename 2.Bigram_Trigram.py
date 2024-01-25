#################################################################################################
#############  USE THIS FOR Bigram and Trigram - using Lemmatization (not stemming) #############
####### In this code duplicates are being removed because of function array_except() ############
#################################################################################################

from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import udf, col, lower, regexp_replace
from pyspark.sql.types import StringType, ArrayType
from pyspark.sql.functions import trim
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions
from pyspark.sql import HiveContext
from pyspark.sql.window import Window
from pyspark.sql.functions import col
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import *
from pyspark.sql.functions import desc
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.functions import lit
from pyspark.ml.feature import Tokenizer, RegexTokenizer
from pyspark.ml.feature import StopWordsRemover
from pyspark.ml.feature import NGram
import nltk
from nltk.stem.snowball import SnowballStemmer
from nltk.stem import PorterStemmer
from nltk.stem.wordnet import WordNetLemmatizer

cc_df1=spark.sql("""select event_dt, chat_conversation_id, visitor_id, mtn, customer_chat_text
                    from
                    (select distinct chat_conversation_id, visitor_id, mtn, customer_chat_text, sentiment_segment, 
                    ....
                    group by event_dt, chat_conversation_id, visitor_id, mtn, customer_chat_text
                    """)

cc_df2 = cc_df1.withColumn("customer_chat_text", functions.regexp_replace(cc_df1["customer_chat_text"], "[^\w]|\s+|_", " ")).withColumn("customer_chat_text", concat_ws(" ",col("customer_chat_text")))

...

bogusWordList = spark.sql("Select * from ##TableName##")

df_clean = df_clean.withColumn("cust_chat_text", F.split("cust_chat_text", " "))

....

df_lookup_var = bogusWordList_lookup.groupBy("col1").agg(F.collect_set("column1").alias("column1")).collect()[0][1]

x = ",".join(df_lookup_var)

df_clean = df_clean.withColumn("filter_col", F.lit(x))

....

tokenizer = RegexTokenizer(inputCol="customer_chat_text", outputCol="wordTokens")

wordTokens_df = tokenizer.transform(df_spark)

remover = StopWordsRemover(inputCol="wordTokens", outputCol="wordTokens_no_stopwords")

....

lmtzr = WordNetLemmatizer()

lemma_udf = udf(lambda tokens: [lmtzr.lemmatize(token) for token in tokens], ArrayType(StringType()))

....

df_lemma = df_lemma.drop('wordTokens_no_stopwords')

filter_length_udf = udf(lambda row: [x for x in row if len(x) >= 3], ArrayType(StringType()))

df_final_words = df_lemma.withColumn('words', filter_length_udf(col('lemmatized')))

ngram = NGram(n=2, inputCol="words", outputCol="ngrams")

ngramDF = ngram.transform(df_final_words)

....

....

df_exp.orderBy('event_dt','chat_conversation_id','visitor_id','mtn',).show(20, truncate=False)
