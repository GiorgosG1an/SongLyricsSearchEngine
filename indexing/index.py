"""
File: index.py
Authors: Giannopoulos Georgios, Ioannis Giannopoulos
Purpose: Index the data 
License: This script is licensed under Apache License v2.0.

Project: FALSE search engine 
Course: Information Retrieval and Mining Lesson
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
from org.apache.lucene.document import Document, Field, FieldType
from org.apache.lucene.index import (IndexOptions, IndexWriter,
                                     IndexWriterConfig, DirectoryReader)
from org.apache.lucene.store import MMapDirectory, SimpleFSDirectory, FSDirectory
from org.apache.lucene.search import (IndexSearcher, TermQuery, MatchAllDocsQuery,
                                     PhraseQuery, BooleanQuery, BooleanClause)
from org.apache.lucene.queryparser.classic import QueryParser
from java.nio.file import Paths




def delete_files_in_folder(folder_path):
    try:
        # Check if the path exists and is a directory
        if os.path.exists(folder_path) and os.path.isdir(folder_path):
            # Iterate through all files in the directory
            for filename in os.listdir(folder_path):
                file_path = os.path.join(folder_path, filename)
                # Check if the path is a file
                if os.path.isfile(file_path):
                    os.remove(file_path)  # Remove the file
            print("All files inside the folder have been deleted.")
        else:
            print("The specified path is not a directory or does not exist.")
    except Exception as e:
        print(f"An error occurred: {e}")





def index():
    stemmed_df = pd.read_csv('data/preprocessed_data/merged_songs_lyrics_stemmed.csv')
    # Initialize Lucene MMapDirectory and IndexWriter for songs

    main_fsDir = MMapDirectory(Paths.get('data/index/main_index'))
    main_writerConfig = IndexWriterConfig(StandardAnalyzer())
    main_writer = IndexWriter(main_fsDir, main_writerConfig)
    print(f"{main_writer.numRamDocs()} docs found in songs index")


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

    # Loop through merged songs and lyrics data and add documents to the index
    for main_fsDir, row in stemmed_df.iterrows():
        main_doc = Document()
        main_doc.add(Field("singer_name", row['singer_name'], singer_name_field_type))
        main_doc.add(Field("song_name", row['song_name'], song_name_field_type))
        main_doc.add(Field("link", row['link'], song_link_field_type))
        main_doc.add(Field("lyrics",row['lyrics'],song_lyrics_field_type)) #the lyrics field not stemmed
        main_doc.add(Field("stemmed_lyrics", row['stemmed_lyrics'], stemmed_lyrics_field_type)) # the stemmed lyrics field
        
        main_writer.addDocument(main_doc)

    print(f"{main_writer.numRamDocs()} docs found in index for merged_songs_lyrics_stemmed.csv")

    # Commit changes and close the IndexWriter
    main_writer.commit()
    main_writer.close()

    #-------------------------indexing done!!!-------------------------------------------------------------

#   index the album data
    
def albums_index():
    try:
        albums_df = pd.read_csv('data/preprocessed_data/albums_normalized.csv')

        albums_fsDir = MMapDirectory(Paths.get('data/index/album_index'))
        albums_writerConfig = IndexWriterConfig(StandardAnalyzer())
        albums_writer = IndexWriter(albums_fsDir, albums_writerConfig)

        print(f"{albums_writer.numRamDocs()} docs found in songs index")


        # Define field types in Lucene for indexing
        #The columns of the stemmed are: Index(['Unnamed: 0_x', 'song_id', 'singer_name', 'song_name', 'Unnamed: 0_y','link', 'lyrics', 'stemmed_lyrics'] ,dtype='object')
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

        # Loop through merged songs and lyrics data and add documents to the index
        for albums_fsDir, row in albums_df.iterrows():
            albums_doc = Document()
            albums_doc.add(Field("singer_name", row['singer_name'], singer_name_field_type))
            albums_doc.add(Field("name", row['name'], album_name_field_type))
            albums_doc.add(Field("type", row['type'], type_field_type))
            albums_doc.add(Field("year",row['year'],year_field_type)) 
                
            albums_writer.addDocument(albums_doc)

        print(f"{albums_writer.numRamDocs()} docs found in index for merged_songs_lyrics_stemmed.csv")

        # Commit changes and close the IndexWriter
        albums_writer.commit()
        albums_writer.close()


    except Exception as e:
        print(f"An error occured: {e}")
        

