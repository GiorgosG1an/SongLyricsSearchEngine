"""
File: VSM_search.py
Authors: Giannopoulos Georgios, Ioannis Giannopoulos
Purpose: Perform a vsm query
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
from org.apache.lucene.search.similarities import BM25Similarity, ClassicSimilarity
from org.apache.lucene.search.similarities import TFIDFSimilarity
from org.apache.lucene.index import DirectoryReader
from org.apache.lucene.store import MMapDirectory
from org.apache.lucene.queryparser.classic import QueryParser
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.util import BytesRef
from org.apache.lucene.search import (IndexSearcher, TermQuery, MatchAllDocsQuery,
                                     PhraseQuery, BooleanQuery, BooleanClause)
from java.nio.file import Paths

from add_docs import add      #for preproccessing the lyrics

def perform_vsm_search(singer_name, song_name, lyrics, fsDir):
    
    singer_exists = 0
    song_exists = 0
    lyrics_exists = 0

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
        song_exists = 1
    else:
        song_exists = 0

    if lyrics != '':
        lyrics = add.preprocess_data(lyrics)
        lyrics_query_parser = QueryParser('stemmed_lyrics', StandardAnalyzer())
        lyrics_query = lyrics_query_parser.parse(lyrics)
        lyrics_exists = 1
    else:
        lyrics_exists = 0

    boolean_query_builder = BooleanQuery.Builder()

    if singer_exists  == 1 :
        boolean_query_builder.add(singer_query, BooleanClause.Occur.MUST)
    
    if song_exists == 1:
        boolean_query_builder.add(song_query, BooleanClause.Occur.MUST)

    if lyrics_exists == 1:
        boolean_query_builder.add(lyrics_query, BooleanClause.Occur.MUST)
    
    combined_query = boolean_query_builder.build()

    k_output = int(input("How many results to output: "))
    if int(k_output) == '':
        k_output = 10


    reader = DirectoryReader.open(fsDir)
    searcher = IndexSearcher(reader)
    #searcher.setSimilarity(BM25Similarity())
    searcher.setSimilarity(ClassicSimilarity()) 
    start_time = time.time()  # Start the timer

    topDocs = searcher.search(combined_query, int(k_output))  # Retrieving top documents

    end_time = time.time()  # Stop the timer
    elapsed_time = (end_time - start_time)*1000  # Calculate elapsed time

    m=0
    if topDocs.scoreDocs is not None and len(topDocs.scoreDocs) > 0:
        print("The term is found in the following documents:")
        
        for scoreDoc in topDocs.scoreDocs:
            docId = scoreDoc.doc
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

def albums_vsm_search(singer_name, album_name, album_type,  year, fsDir):

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
    searcher.setSimilarity(ClassicSimilarity()) 
    
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