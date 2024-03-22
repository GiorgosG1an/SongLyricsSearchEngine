"""
File: add.py
Authors: Giannopoulos Georgios, Ioannis Giannopoulos
Purpose: Add new data in the index by input or from a csv
License: This script is licensed under Apache License v2.0.

Project: FALSE search engine 
Course: Information Retrieval and Mining 
University: University of Peloponnese
Department: Informatics and Telecommunications
Date : December 22, 2023

Description:
This project is developed as part of the Information Retrieval and Mining course at the University of Peloponnese, Department of Informatics and Telecommunications. 

"""

import lucene
import os, sys, time
import pandas as pd
from pathlib import Path
from java.nio.file import Paths
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.document import Document, Field, FieldType, TextField
from org.apache.lucene.index import (IndexOptions, IndexWriter,
                                     IndexWriterConfig, DirectoryReader)
from org.apache.lucene.store import MMapDirectory, SimpleFSDirectory, FSDirectory
from org.apache.lucene.search import (IndexSearcher, TermQuery, MatchAllDocsQuery,
                                     PhraseQuery, BooleanQuery, BooleanClause)
from org.apache.lucene.queryparser.classic import QueryParser
from java.nio.file import Paths
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import SnowballStemmer
from preprocessing import preprocess
#lucene.initVM()

# Function to check if the required columns are present in the CSV file
def check_columns(csv_file):
    required_columns = ['song_name', 'singer_name', 'link', 'lyrics']
    
    # Read the first row (header) of the CSV file
    header = pd.read_csv(csv_file, nrows=0).columns.tolist()
    
    # Check if all required columns are present in the CSV file
    return all(column in header for column in required_columns)

# Function to preprocess the data
def preprocess_data(text):
    # Lowercase normalization
    text = text.lower()
    
    # Tokenization
    tokens = word_tokenize(text)
    
    # Removing stopwords
    stop_words = set(stopwords.words('english'))
    filtered_tokens = [word for word in tokens if word not in stop_words]
    
    # Snowball stemming
    stemmer = SnowballStemmer('english')
    stemmed_tokens = [stemmer.stem(token) for token in filtered_tokens]
    
    # Join stemmed tokens
    stemmed_text = ' '.join(stemmed_tokens)
    
    return stemmed_text

# Function to add document to the index when user selects to input the data 
def add_document_to_index(singer_name, song_name, link, lyrics):
    try:
        existing_fsDir = MMapDirectory(Paths.get('data/index/main_index'))
        # Initialize IndexWriterConfig with the analyzer
        writerConfig = IndexWriterConfig(StandardAnalyzer())
        # Initialize IndexWriter with the existing index and config
        index_writer = IndexWriter(existing_fsDir, writerConfig)
        new_doc_df = pd.DataFrame({
            'Unnamed: 0_x': [None],
            'song_id': [None],
            'singer_name': [singer_name],
            'song_name': [song_name],
            'Unnamed: 0_y': [None],
            'link': [link],
            'lyrics': [lyrics],
            'stemmed_lyrics': [lyrics]
        })

        # Preprocess the data
        new_doc_df['stemmed_lyrics'] = new_doc_df['lyrics'].apply(preprocess_data)
        # Save the preprocessed data to a new CSV file
        new_doc_df.to_csv('data/add_doc.csv', index=False)
        # Read existing CSV
        existing_data = pd.read_csv('data/preprocessed_data/merged_songs_lyrics_stemmed.csv')

        # Append new data to existing data
        updated_data = pd.concat([existing_data, new_doc_df], ignore_index=True)

        # Save the updated data to the CSV file
        updated_data.to_csv('data/preprocessed_data/merged_songs_lyrics_stemmed.csv', index=False)

        print("Data added to the CSV file successfully.")
        # Define field types in Lucene for indexing
        #The columns of the stemmed are: Index(['Unnamed: 0_x', 'song_id', 'singer_name', 'song_name', 'Unnamed: 0_y','link', 'lyrics', 'stemmed_lyrics'] ,dtype='object')
        song_id_field_type = FieldType()
        song_id_field_type.setStored(False)  # no need to store the id field

        singer_name_field_type = FieldType() 
        singer_name_field_type.setStored(True)
        singer_name_field_type.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS)

        song_name_field_type = FieldType()
        song_name_field_type.setStored(True)
        song_name_field_type.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS)

        song_link_field_type = FieldType()
        song_link_field_type.setStored(True)

        song_lyrics_field_type = FieldType()
        song_lyrics_field_type.setStored(True)


        stemmed_lyrics_field_type = FieldType()
        stemmed_lyrics_field_type.setStored(True)
        stemmed_lyrics_field_type.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS)

        # Add the document to the index
        for _, row in new_doc_df.iterrows():
            doc = Document()
            doc.add(Field("singer_name", row['singer_name'],singer_name_field_type))
            doc.add(Field("song_name", row['song_name'], song_name_field_type))
            doc.add(Field("link", row['link'], song_link_field_type))
            doc.add(Field("lyrics", row['lyrics'], song_lyrics_field_type))
            doc.add(Field("stemmed_lyrics", row['stemmed_lyrics'], stemmed_lyrics_field_type))
                
            index_writer.addDocument(doc)

        index_writer.commit()
        print("Document added to the existing index successfully.")
    except Exception as e:
        print(f"An error occurred while adding document: {e}")
    finally:
        index_writer.close()

# Function to index documents 
def add_csv_to_index(new_data_df,file_name):
    

    print("Indexing documents:")
    try:
        existing_fsDir = MMapDirectory(Paths.get('data/index/main_index'))
        # Initialize IndexWriterConfig with the analyzer
        writerConfig = IndexWriterConfig(StandardAnalyzer())
        # Initialize IndexWriter with the existing index and config
        index_writer = IndexWriter(existing_fsDir, writerConfig)
        new_data_df = pd.read_csv(file_name)
                 
        # Preprocess the data
        new_data_df['stemmed_lyrics'] = new_data_df['lyrics'].apply(preprocess_data)
        # Save the preprocessed data to a new CSV file
        new_data_df.to_csv('data/add_doc.csv', index=False)
        # Read existing CSV
        existing_data = pd.read_csv('data/preprocessed_data/merged_songs_lyrics_stemmed.csv')

        # Append new data to existing data
        updated_data = pd.concat([existing_data, new_data_df], ignore_index=True)

        # Save the updated data to the CSV file
        updated_data.to_csv('data/preprocessed_data/merged_songs_lyrics_stemmed.csv', index=False)

        print("Data added to the CSV file successfully.")
        # Define field types in Lucene for indexing
        #The columns of the stemmed are: Index(['Unnamed: 0_x', 'song_id', 'singer_name', 'song_name', 'Unnamed: 0_y','link', 'lyrics', 'stemmed_lyrics'] ,dtype='object')
        song_id_field_type = FieldType()
        song_id_field_type.setStored(False)  # no need to store the id field

        singer_name_field_type = FieldType() 
        singer_name_field_type.setStored(True)
        singer_name_field_type.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS)

        song_name_field_type = FieldType()
        song_name_field_type.setStored(True)
        song_name_field_type.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS)

        song_link_field_type = FieldType()
        song_link_field_type.setStored(True)

        song_lyrics_field_type = FieldType()
        song_lyrics_field_type.setStored(True)


        stemmed_lyrics_field_type = FieldType()
        stemmed_lyrics_field_type.setStored(True)
        stemmed_lyrics_field_type.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS)

        # Add the document to the index
        for existing_fsDir, row in new_data_df.iterrows():
            doc = Document()
            doc.add(Field("singer_name", row['singer_name'],singer_name_field_type))
            doc.add(Field("song_name", row['song_name'], song_name_field_type))
            doc.add(Field("link", row['link'], song_link_field_type))
            doc.add(Field("lyrics", row['lyrics'], song_lyrics_field_type))
            doc.add(Field("stemmed_lyrics", row['stemmed_lyrics'], stemmed_lyrics_field_type))
                
            index_writer.addDocument(doc)

        index_writer.commit()
        print("Document added to the existing index successfully.")
    except Exception as e:
        print(f"An error occurred while adding document: {e}")
    finally:
        index_writer.close()

