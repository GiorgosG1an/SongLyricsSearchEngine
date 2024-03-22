"""
File: preprocess.py
Authors: Giannopoulos Georgios, Ioannis Giannopoulos
Purpose: Preprocessing the csv files in order to be indexed
License: This script is licensed under Apache License version 2.0.

Project: Information Retrieval and Mining Lesson
University: University of Peloponnese
Department: Informatics and Telecommunications

Description:
This project is developed as part of the Information Retrieval and Mining lesson at the University of Peloponnese, Department of Informatics and Telecommunications. 

"""
import pandas as pd
import time
from nltk.stem import SnowballStemmer
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
import nltk
nltk.download('stopwords')
nltk.download('punkt')


def lowercase_normalize_columns(dataframe, columns): #function to lowercase
    for col in columns:
        if col in dataframe.columns:
            dataframe[col] = dataframe[col].str.lower()
    return dataframe


# Function to perform stemming on lyrics column
def stem_lyrics(dataframe):
    start_time = time.time()
    stop_words = set(stopwords.words('english'))
    stemmer = SnowballStemmer('english')
    stemmed_lyrics = []
        
    for lyrics in dataframe['lyrics']:
        tokens = word_tokenize(str(lyrics))  # Tokenize the lyrics
        filtered_tokens = [token for token in tokens if token.lower() not in stop_words]  # Remove stopwords
        stemmed_tokens = [stemmer.stem(token) for token in filtered_tokens]  # Stem each token
        stemmed_text = ' '.join(stemmed_tokens)  # Join stemmed tokens
        stemmed_lyrics.append(stemmed_text)
        
    dataframe['stemmed_lyrics'] = stemmed_lyrics
        
    end_time = time.time()
    execution_time = end_time - start_time
    return dataframe, execution_time

