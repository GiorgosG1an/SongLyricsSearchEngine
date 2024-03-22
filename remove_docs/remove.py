"""
File: remove.py
Authors: Giannopoulos Georgios, Ioannis Giannopoulos
Purpose: Remove documents from the index
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
from tabulate import tabulate
from java.nio.file import Paths
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.document import Document, Field, FieldType, TextField
from org.apache.lucene.index import (IndexOptions, Term,IndexWriter,
                                     IndexWriterConfig, DirectoryReader)
from org.apache.lucene.store import MMapDirectory, SimpleFSDirectory, FSDirectory
from org.apache.lucene.search import (IndexSearcher, TermQuery, MatchAllDocsQuery,
                                     PhraseQuery, BooleanQuery, BooleanClause)
from org.apache.lucene.queryparser.classic import QueryParser
from java.nio.file import Paths



# Function to remove documents from the index based on singer names and song names
def remove_from_index(singer_names, song_names):
    try:
        index_directory = "data/index/main_index"  
        fsDir = MMapDirectory(Paths.get(index_directory))
        index_writer = IndexWriter(fsDir, IndexWriterConfig(StandardAnalyzer()))
        # Initialize IndexSearcher
        searcher = IndexSearcher(DirectoryReader.open(fsDir))
        m=0
        # Remove documents based on singer names and song names
        for singer_name, song_name in zip(singer_names, song_names):
                #start_time = time.time()  # Record start time
    
                # Creating separate queries for song name and singer name
                singer_query_parser = QueryParser('singer_name', StandardAnalyzer())
                song_query_parser = QueryParser('song_name', StandardAnalyzer())


                singer_query = singer_query_parser.parse(singer_name)
                print(f"Singer query = {singer_query}")
                song_query = song_query_parser.parse(song_name)
                print(f"Song name query = {song_query}")
                # Combining queries using BooleanQuery
                boolean_query_builder = BooleanQuery.Builder()
              
                boolean_query_builder.add(singer_query, BooleanClause.Occur.MUST)  # Ensure the artist must match
                boolean_query_builder.add(song_query, BooleanClause.Occur.MUST)    # Ensure the song name must match
                combined_query = boolean_query_builder.build()

                searcher = IndexSearcher(DirectoryReader.open(fsDir))


                topDocs = searcher.search(combined_query, 1)

                #elapsed_time = (time.time() - start_time) * 1000  # Calculate elapsed time in ms

                # Display search results
            
                if topDocs.scoreDocs is not None and len(topDocs.scoreDocs) > 0:
                    print("The term is found in the following documents:")

                    for scoreDoc in topDocs.scoreDocs:
                        docId = scoreDoc.doc
                        print(f"Doc_id ={docId}")
                        doc = searcher.doc(docId)
                        m+=1
                        print(f"Number {m}")
                        print(f"Song name: {doc.getField('song_name').stringValue()}")
                        print(f"Singer name: {doc.getField('singer_name').stringValue()}")
                        print(f"Lyrics: {doc.getField('lyrics').stringValue()} ")
                        print(f"Link:{doc.getField('link').stringValue()}")
                        print("---------------------------------------")
                        while True:
                            answer = input("Are you sure you want to delete this document? [y]/[n]")
                            if answer == 'y':
                                index_writer.deleteDocuments(combined_query)
                                print(f"Document with docID: {docId} and singer name: {singer_name} and song name: {song_name} removed from the index.")
                                print('Updating csv file...')
                                df = pd.read_csv('data/preprocessed_data/merged_songs_lyrics_stemmed.csv')
                                # Filter out rows where 'singer_name' is given singer name  and  'song_name' is the given song name
                                df = df[(df['singer_name'] != singer_name) & (df['song_name'] != song_name)]
                                # Save the updated DataFrame back to the same CSV file
                                df.to_csv('data/preprocessed_data/merged_songs_lyrics_stemmed.csv', index=False)
                                print('Csv updated.')
                                break
                            elif answer == 'n':
                                print("Exiting without delete...") 
                                break
                            else:
                                print("Invalid input\n")
                                               
                else:
                    print(f"No document found with singer name: {singer_name} and song name: {song_name}.")

        searcher.getIndexReader().close()  # Close the IndexReader
        index_writer.commit()  # Commit the changes to the index
        #index_writer.close()
            
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if index_writer is not None:
            index_writer.close()  # Close the IndexWriter




