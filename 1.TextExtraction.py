# Lowercase the text.
cc_df2 = cc_df1.withColumn("customer_chat_text", functions.regexp_replace(cc_df1["customer_chat_text"], "[^\w]|\s+|_", " ")).withColumn("customer_chat_text", concat_ws(" ",col("customer_chat_text")))
cc_df3 = (reduce(lambda memo_df, col_name: memo_df.withColumn(col_name, lower(col(col_name))),cc_df2.columns,cc_df2))

cc_df3.orderBy('event_dt').show()
cc_df3.printSchema()
type(cc_df3)


from pyspark.sql.functions import explode

# Clean text
df_clean = cc_df3.select('event_dt', (lower(regexp_replace('customer_chat_text', "[^a-zA-Z\\s]", "")).alias('cust_chat_text')))

# Convert to Pandas
df_clean_pdf = df_clean.toPandas()

bogusWordList = spark.sql("Select * from ##TableName## ")
bogusWordList_pdf = bogusWordList.toPandas()

df1 = pd.DataFrame(df_clean_pdf, columns = ['event_dt', 'cust_chat_text'])
df2 = pd.DataFrame(bogusWordList_pdf, columns = ['column1'])


import re

def remove_bogus_word(x, df2):
    replace=re.compile(r'\b(' + ('|'.join(df2['column1'])) + r')\b')
    return x.str.replace(replace, '').str.replace(re.compile('\s{2,}'), ' ')
df = df1.apply(lambda x: remove_bogus_word(x, df2))

#Convert pandas dataframe to spark dataframe

# Remove whitespaces

# Tokenize
tokenizer = RegexTokenizer(inputCol="cust_chat_text", outputCol="wordTokens") 


# Remove stopwords
remover = StopWordsRemover(inputCol="wordTokens", outputCol="wordTokens_no_stopwords")
wordTokenNoStopw = remover.transform(wordTokens_df)
    
# Stem words
stemmer = SnowballStemmer(language='english')
stemmer_udf = udf(lambda tokens: [stemmer.stem(token) for token in tokens], ArrayType(StringType()))


# Filter words > 3
filter_length_udf = udf(lambda row: [x for x in row if len(x) >= 3], ArrayType(StringType()))
df_final_words = df_stemmed.withColumn('words', filter_length_udf(col('words_stemmed')))
#df_final_words.show(10)

