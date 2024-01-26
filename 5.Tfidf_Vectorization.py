# ---- TfidfVectorization ---- #

from sklearn.feature_extraction.text import TfidfVectorizer 

sampleData1 = textData[:10000]

text_list = sampleData1["cleaned_text"].tolist()

# settings that you use for count vectorizer will go here
tfidf_vect = TfidfVectorizer(analyzer=clean_text1, stop_words='english')

# just send in all your docs here
X_tfidf = tfidf_vect.fit_transform(sampleData1['cleaned_text'])