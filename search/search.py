"""
File: search.py
Authors: Giannopoulos Georgios, Ioannis Giannopoulos
Purpose: Perform a boolean query
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
import time
from org.apache.lucene.search import IndexSearcher, PhraseQuery
from org.apache.lucene.index import DirectoryReader
from org.apache.lucene.store import MMapDirectory
from org.apache.lucene.queryparser.classic import QueryParser
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.util import BytesRef
from org.apache.lucene.search import (IndexSearcher, TermQuery, MatchAllDocsQuery,
                                     PhraseQuery, BooleanQuery, BooleanClause)
from org.apache.lucene.queryparser.classic import QueryParser
from java.nio.file import Paths
from org.apache.lucene.queryparser.classic import MultiFieldQueryParser

from add_docs import add      #for preproccessing the lyrics



# Function to check if the index is empty
def is_index_empty(fsDir):
    try:
        reader = DirectoryReader.open(fsDir)
        num_docs = reader.numDocs()
        reader.close()
        return num_docs == 0
    except Exception as e:
        print(f"An error occurred while checking the index: {e}")
        return True  # Treat an error as an empty index

def perform_boolean_search(singer_name, song_name, lyrics, fsDir):
    singer_exists = 0
    song_exists = 0
    lyrics_exists = 0

    #start_time = time.time()  # Record start time
    
    # Creating separate queries for song name, singer name and lyrics
    if singer_name != '':
        singer_query_parser = QueryParser('singer_name', StandardAnalyzer())
        singer_query = singer_query_parser.parse(singer_name)
        #print(f"Singer query = {singer_query}")
        singer_exists = 1
    else:
        singer_exists = 0
        

    if song_name != '' :
        song_query_parser = QueryParser('song_name', StandardAnalyzer())
        song_query = song_query_parser.parse(song_name)
        #print(f"Song Query = {song_query}")
        song_exists = 1
    else:
        song_exists = 0

    if lyrics != '':
        lyrics = add.preprocess_data(lyrics)
        lyrics_query_parser = QueryParser('stemmed_lyrics', StandardAnalyzer())
        lyrics_query = lyrics_query_parser.parse(lyrics)
        #print(f"Lyrics query = {lyrics_query}")
        lyrics_exists = 1
    else:
        lyrics_exists = 0
    
    # Combining queries using BooleanQuery
    boolean_query_builder = BooleanQuery.Builder()
    if singer_exists ==1:
        if "OR" in singer_name:
            boolean_query_builder.add(singer_query, BooleanClause.Occur.SHOULD)  # OR detected show occur.SHOULD
        else:
            boolean_query_builder.add(singer_query, BooleanClause.Occur.MUST)  # AND  default operator for our search engine in boolean queries

    if song_exists == 1 :
        if "OR" in song_name:
            boolean_query_builder.add(song_query, BooleanClause.Occur.SHOULD)  # OR detected show occur.SHOULD
        else:
            boolean_query_builder.add(song_query, BooleanClause.Occur.MUST)  # AND  default operator for our search engine in boolean queries
    
    if lyrics_exists == 1 :
        if "OR" in lyrics:
            
            boolean_query_builder.add(lyrics_query, BooleanClause.Occur.SHOULD)  # OR detected show occur.SHOULD
        else:
            boolean_query_builder.add(lyrics_query, BooleanClause.Occur.MUST)# AND  default operator for our search engine in boolean queries
    
    
    
    combined_query = boolean_query_builder.build()
    #print(f"Combined query is : {combined_query}")
    k_output = int(input("How many results to output: "))
    if k_output == '' :
        k_output = 10


    # Open the index directory
    reader = DirectoryReader.open(fsDir)
    searcher = IndexSearcher(reader)
    start_time = time.time()  # Start the timer

    topDocs = searcher.search(combined_query, int(k_output))  

    end_time = time.time()  # Stop the timer
    elapsed_time = (end_time - start_time)*1000  # Calculate elapsed time
    m=0
    if topDocs.scoreDocs is not None and len(topDocs.scoreDocs) > 0:
        print("The term is found in the following documents:")
        
        for scoreDoc in topDocs.scoreDocs:
            docId = scoreDoc.doc
            #print(f"Doc_id ={docId}")
            doc = searcher.doc(docId)  
            m+=1
            print(f"Number {m}")
            print(f"Score: {scoreDoc.score}")
            print(f"Song name: {doc.getField('song_name').stringValue()}")
            print(f"Singer name: {doc.getField('singer_name').stringValue()}")
            #print(f"Lyrics: {doc.getField('lyrics').stringValue()} ")
            print(f"Link:{doc.getField('link').stringValue()}")
            print("---------------------------------------")
    else:
        print("The term is not found in any documents.")

    print(f"Search took {elapsed_time:.2f} milliseconds")


def albums_boolean_search(singer_name, album_name, album_type,  year:str, fsDir):

    singer_exists = 0
    album_name_exists = 0
    album_type_exists = 0
    year_exists = 0

    if singer_name != '':
        singer_query_parser = QueryParser('singer_name', StandardAnalyzer())
        singer_query = singer_query_parser.parse(singer_name)
        singer_exists = 1
    else:
        singer_exists = 0
    
    if album_name != '' :
        album_name_query_parser = QueryParser('name', StandardAnalyzer())
        album_name_query = album_name_query_parser.parse(album_name)
        album_name_exists = 1
    else:
        album_name_exists = 0

    if album_type != '' :
        album_type_query_parser = QueryParser('type', StandardAnalyzer())
        album_type_query = album_type_query_parser.parse(album_type)
        album_type_exists = 1
    else:
        album_type_exists = 0

    if year != '' :
        year_query_parser = QueryParser('year', StandardAnalyzer())
        year_query = year_query_parser.parse(year)
        year_exists = 1
    else:
        year_exists = 0


    # Combining queries using BooleanQuery
    boolean_query_builder = BooleanQuery.Builder()
    if singer_exists ==1:
        if "OR" in singer_name:
            boolean_query_builder.add(singer_query, BooleanClause.Occur.SHOULD)  # OR detected show occur.SHOULD
        else:
            boolean_query_builder.add(singer_query, BooleanClause.Occur.MUST)  # AND  default operator for our search engine in boolean queries

    if album_name_exists == 1 :
        if "OR" in album_name:
            boolean_query_builder.add(album_name_query, BooleanClause.Occur.SHOULD)  # OR detected show occur.SHOULD
        else:
            boolean_query_builder.add(album_name_query, BooleanClause.Occur.MUST)  # AND  default operator for our search engine in boolean queries
    
    if album_type_exists == 1 :
        if "OR" in album_type:
            
            boolean_query_builder.add(album_type_query, BooleanClause.Occur.SHOULD)  # OR detected show occur.SHOULD
        else:
            boolean_query_builder.add(album_type_query, BooleanClause.Occur.MUST)# AND  default operator for our search engine in boolean queries
    
    if year_exists == 1 :
        if "OR" in year:
            
            boolean_query_builder.add(year_query, BooleanClause.Occur.SHOULD)  # OR detected show occur.SHOULD
        else:
            boolean_query_builder.add(year_query, BooleanClause.Occur.MUST)# AND  default operator for our search engine in boolean queries
    
    
    combined_query = boolean_query_builder.build()
    #print(f"Combined query is : {combined_query}")
    k_output = int(input("How many results to output: "))
    
    # Open the index directory
    reader = DirectoryReader.open(fsDir)
    searcher = IndexSearcher(reader)
    start_time = time.time()  # Start the timer

    topDocs = searcher.search(combined_query, int(k_output))  

    end_time = time.time()  # Stop the timer
    elapsed_time = (end_time - start_time)*1000  # Calculate elapsed time
    m=0
    if topDocs.scoreDocs is not None and len(topDocs.scoreDocs) > 0:
        print("The term is found in the following documents:")
        
        for scoreDoc in topDocs.scoreDocs:
            docId = scoreDoc.doc
            #print(f"Doc_id ={docId}")
            doc = searcher.doc(docId)  
            m+=1
            print(f"Number {m}")
            print(f"Score: {scoreDoc.score}")
            print(f"Singer name: {doc.getField('singer_name').stringValue()}")
            print(f"Album name: {doc.getField('name').stringValue()}")
            print(f"Type: {doc.getField('type').stringValue()} ")
            print(f"Year:{doc.getField('year').stringValue()}")
            print("---------------------------------------")
    else:
        print("The term is not found in any documents.")

    print(f"Search took {elapsed_time:.2f} milliseconds")