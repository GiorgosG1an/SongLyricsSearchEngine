"""
File: remove_album.py
Authors: Giannopoulos Georgios, Ioannis Giannopoulos
Purpose: Remove album documents from the index
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
def remove_album_from_index(singer_names, album_names):
    """remove album"""
    try:
        index_directory = "data/index/album_index"  
        fsDir = MMapDirectory(Paths.get(index_directory))
        index_writer = IndexWriter(fsDir, IndexWriterConfig(StandardAnalyzer()))
        # Initialize IndexSearcher
        searcher = IndexSearcher(DirectoryReader.open(fsDir))
        m=0
        # Remove documents based on singer names and song names
        for singer_name, album_name in zip(singer_names, album_names):
    
                # Creating separate queries for song name and singer name
                singer_query_parser = QueryParser('singer_name', StandardAnalyzer())
                album_query_parser = QueryParser('name', StandardAnalyzer())


                singer_query = singer_query_parser.parse(singer_name)
                print(f"Singer query = {singer_query}")
                album_query = album_query_parser.parse(album_name)
                print(f"Album name query = {album_query}")
                # Combining queries using BooleanQuery
                boolean_query_builder = BooleanQuery.Builder()
              
                boolean_query_builder.add(singer_query, BooleanClause.Occur.MUST)  # Ensure the artist must match
                boolean_query_builder.add(album_query, BooleanClause.Occur.MUST)    # Ensure the song name must match
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
                        print(f"Singer name: {doc.getField('singer_name').stringValue()}")
                        print(f"Album name: {doc.getField('name').stringValue()}")
                        print(f"Type: {doc.getField('type').stringValue()} ")
                        print(f"Year:{doc.getField('year').stringValue()}")
                        print("---------------------------------------")
                        while True:
                            answer = input("Are you sure you want to delete this document? [y]/[n]")
                            if answer == 'y':
                                index_writer.deleteDocuments(combined_query)
                                print(f"Document with docID: {docId} and singer name: {singer_name} and song name: {album_name} removed from the index.")
                                print('Updating csv file...')
                                df = pd.read_csv('data/preprocessed_data/albums_normalized.csv')
                                # Filter out rows where 'singer_name' is given singer name  and  'song_name' is the given song name
                                df = df[(df['singer_name'] != singer_name) & (df['name'] != album_name)]
                                # Save the updated DataFrame back to the same CSV file
                                df.to_csv('data/preprocessed_data/albums_normalized.csv', index=False)
                                print('Csv updated.')
                                break
                            elif answer == 'n':
                                print("Exiting without delete...") 
                                break
                            else:
                                print("Invalid input\n")
                                               
                else:
                    print(f"No document found with singer name: {singer_name} and song name: {album_name}.")

        searcher.getIndexReader().close()  # Close the IndexReader
        index_writer.commit()  # Commit the changes to the index
        
            
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if index_writer is not None:
            index_writer.close()  # Close the IndexWriter
