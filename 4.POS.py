#################################################################################################
############################################  For POS ###########################################
####### In this code duplicates are being removed because of function array_except() ############
#################################################################################################

def pos_tag(tokens, tagset=None):
    tagger = PerceptronTagger()
    return _pos_tag(tokens, tagset, tagger)

POS_udf = udf(lambda tokens: [token for token in nltk.pos_tag(tokens)], ArrayType(ArrayType(StringType())))

...

POS_word_grammer = (POS_Gramm_1.withColumn("ar", split(col("POS_Gramm1"), ","))
                    .select('event_dt','chat_conversation_id','visitor_id','mtn', 
                            col("ar")[0].alias("cust_words"), slice(col("ar"), 2,1).alias("Grammer"))
                   )
word_grammer = POS_word_grammer.select('event_dt','chat_conversation_id','visitor_id','mtn','cust_words', explode(col('Grammer')).alias('grammer'))
(word_grammer
 .select('event_dt','chat_conversation_id','visitor_id','mtn','cust_words','grammer')
 .orderBy('event_dt','chat_conversation_id','visitor_id','mtn')
 .show(truncate=False)
)
