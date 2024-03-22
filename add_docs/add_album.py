"""
File: add_album.py
Authors: Giannopoulos Georgios, Ioannis Giannopoulos
Purpose: Add new data in the album index by input or from a csv
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
from nltk.stem import SnowballStemmer
from preprocessing import preprocess


def check_album_columns(csv_file):
    required_columns = ['singer_name', 'name', 'type', 'year']
    
    # Read the first row (header) of the CSV file
    header = pd.read_csv(csv_file, nrows=0).columns.tolist()
    
    # Check if all required columns are present in the CSV file
    return all(column in header for column in required_columns)

# user input the data
def add_album_document_to_index(singer_name, album_name, album_type, year):
    try:
        existing_fsDir = MMapDirectory(Paths.get('data/index/album_index'))
        # Initialize IndexWriterConfig with the analyzer
        writerConfig = IndexWriterConfig(StandardAnalyzer())
        # Initialize IndexWriter with the existing index and config
        index_writer = IndexWriter(existing_fsDir, writerConfig)
        new_doc_df = pd.DataFrame({
            'Unnamed: 0_x': [None],
            'song_id': [None],
            'singer_name': [singer_name],
            'name': [album_name],
            'type': [album_type],
            'year': [year]
        })

        # Save the preprocessed data to a new CSV file
        new_doc_df.to_csv('data/add_album_doc.csv', index=False)
        # Read existing CSV
        existing_data = pd.read_csv('data/preprocessed_data/albums_normalized.csv')

        # Append new data to existing data
        updated_data = pd.concat([existing_data, new_doc_df], ignore_index=True)

        # Save the updated data to the CSV file
        updated_data.to_csv('data/preprocessed_data/merged_songs_lyrics_stemmed.csv', index=False)

        print("Data added to the CSV file successfully.")
        # Define field types in Lucene for indexing
        song_id_field_type = FieldType()
        song_id_field_type.setStored(False)  # no need to store the id field

        singer_name_field_type = FieldType() 
        singer_name_field_type.setStored(True)
        singer_name_field_type.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS)

        album_name_field_type = FieldType()
        album_name_field_type.setStored(True)
        album_name_field_type.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS)

        type_field_type = FieldType()
        type_field_type.setStored(True)
        type_field_type.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS)


        year_field_type = FieldType()
        year_field_type.setStored(True)
        year_field_type.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS)


        # Add the document to the index
        for existing_fsDir, row in new_doc_df.iterrows():
            albums_doc = Document()
            albums_doc.add(Field("singer_name", row['singer_name'], singer_name_field_type))
            albums_doc.add(Field("name", row['name'], album_name_field_type))
            albums_doc.add(Field("type", row['type'], type_field_type))
            albums_doc.add(Field("year",row['year'],year_field_type)) 
                
                
            index_writer.addDocument(albums_doc)

        index_writer.commit()

    except Exception as e:
        print(f"An error occurred while adding document: {e}")

    finally:
        index_writer.close()


def add_album_csv_to_index(new_data_df,file_name):
    print("Indexing documents:")
    try:
        existing_fsDir = MMapDirectory(Paths.get('data/index/album_index'))
        # Initialize IndexWriterConfig with the analyzer
        writerConfig = IndexWriterConfig(StandardAnalyzer())
        # Initialize IndexWriter with the existing index and config
        index_writer = IndexWriter(existing_fsDir, writerConfig)
        new_data_df = pd.read_csv(file_name)

        # Save the preprocessed data to a new CSV file
        #new_data_df.to_csv(file, index=False)
        # Read existing CSV
        existing_data = pd.read_csv('data/preprocessed_data/albums_normalized.csv')

        # Append new data to existing data
        updated_data = pd.concat([existing_data, new_data_df], ignore_index=True)

        # Save the updated data to the CSV file
        updated_data.to_csv('data/preprocessed_data/albums_normalized.csv', index=False)


        # Define field types in Lucene for indexing
        song_id_field_type = FieldType()
        song_id_field_type.setStored(False)  # no need to store the id field

        singer_name_field_type = FieldType() 
        singer_name_field_type.setStored(True)
        singer_name_field_type.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS)

        album_name_field_type = FieldType()
        album_name_field_type.setStored(True)
        album_name_field_type.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS)

        type_field_type = FieldType()
        type_field_type.setStored(True)
        type_field_type.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS)


        year_field_type = FieldType()
        year_field_type.setStored(True)
        year_field_type.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS)


        # Add the document to the index
        for existing_fsDir, row in new_data_df.iterrows():
            albums_doc = Document()
            albums_doc.add(Field("singer_name", row['singer_name'], singer_name_field_type))
            albums_doc.add(Field("name", row['name'], album_name_field_type))
            albums_doc.add(Field("type", row['type'], type_field_type))
            albums_doc.add(Field("year",row['year'],year_field_type)) 
                
                
            index_writer.addDocument(albums_doc)

        index_writer.commit()
    except Exception as e:

        print(f"An error occurred while adding document: {e}")
    finally:
        index_writer.close()