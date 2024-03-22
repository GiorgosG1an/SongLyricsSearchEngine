"""
File: Phrase_search.py
Authors: Giannopoulos Georgios, Ioannis Giannopoulos
Purpose: Perform a phrase query
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
import time
import string
from org.apache.lucene.search import IndexSearcher, PhraseQuery
from org.apache.lucene.index import DirectoryReader, Term
from org.apache.lucene.store import MMapDirectory
from org.apache.lucene.queryparser.classic import QueryParser
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.util import BytesRef
from org.apache.lucene.search import (IndexSearcher, TermQuery, MatchAllDocsQuery,
                                     PhraseQuery, BooleanQuery, BooleanClause)
from org.apache.lucene.queryparser.classic import QueryParser
from java.nio.file import Paths
from org.apache.lucene.queryparser.classic import MultiFieldQueryParser

from add_docs import add #for preproccessing the lyrics


def perform_phrase_search(singer_name, song_name, lyrics, fsDir):
    
    singer_exists = 0
    song_exists = 0
    lyrics_exists = 0

    if singer_name != '':
        if singer_name.startswith('"') and singer_name.endswith('"'):
            singer_phrase_text = singer_name[1:-1]  # Extract text within quotes
        else:
            singer_phrase_text = singer_name

        
        terms = singer_phrase_text.split()  # Split the phrase into terms

        builder = PhraseQuery.Builder()

        for position, term in enumerate(terms):
            builder.add(Term('singer_name', term), position + 1)
        
        singer_phrase_query = builder.build()
        #print(f"Singer query = {singer_phrase_query}")
        singer_exists = 1
    else:
        singer_exists = 0

    if song_name != '':
        if song_name.startswith('"') and song_name.endswith('"'):
            song_phrase_text = song_name[1:-1]  # Extract text within quotes
        else:
            song_phrase_text = song_name
        #print(f"song_phrase_text: {song_phrase_text}")
        
        terms = song_phrase_text.split()  # Split the phrase into terms

        builder = PhraseQuery.Builder()

        for position, term in enumerate(terms):
            builder.add(Term('song_name', term), position + 1)
        
        song_phrase_query = builder.build()
        #print(f"Song query = {song_phrase_query}")
        song_exists = 1
    else:
        song_exists = 0


    if lyrics != '':
        if lyrics.startswith('"') and lyrics.endswith('"'):
            lyrics_phrase_text = lyrics[1:-1]  # Extract text within quotes
            lyrics_phrase_text = add.preprocess_data(lyrics_phrase_text)
        else:
            lyrics_phrase_text = lyrics
            lyrics_phrase_text = add.preprocess_data(lyrics_phrase_text)

        
        terms = lyrics_phrase_text.split()  # Split the phrase into terms

        builder = PhraseQuery.Builder()

        for position, term in enumerate(terms):
            builder.add(Term('stemmed_lyrics', term), position + 1)
        
        lyrics_phrase_query = builder.build()
        #print(f"Lyrics query = {lyrics_phrase_query}")
        lyrics_exists = 1
    else:
        lyrics_exists = 0

    boolean_query_builder = BooleanQuery.Builder()

    if singer_exists  == 1 :
        boolean_query_builder.add(singer_phrase_query, BooleanClause.Occur.MUST)
    
    if song_exists == 1:
        boolean_query_builder.add(song_phrase_query, BooleanClause.Occur.MUST)

    if lyrics_exists == 1:
        boolean_query_builder.add(lyrics_phrase_query, BooleanClause.Occur.MUST)
    
    combined_query = boolean_query_builder.build()
    #print(f"Combined query is : {combined_query}")

    k_output = int(input("How many results to output: "))
    if int(k_output) == '':
        k_output = 10


    reader = DirectoryReader.open(fsDir)
    searcher = IndexSearcher(reader)
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
            print(f"Lyrics: {doc.getField('lyrics').stringValue()} ")
            print(f"Link:{doc.getField('link').stringValue()}")
            print("---------------------------------------")
    else:
        print("The term is not found in any documents.")

    print(f"Search took {elapsed_time:.2f} milliseconds")


def albums_phrase_search(singer_name, album_name, album_type, year, fsDir):

    singer_exists = 0
    album_name_exists = 0
    album_type_exists = 0
    #year_exists = 0

    if singer_name != '':
        if singer_name.startswith('"') and singer_name.endswith('"'):
            singer_phrase_text = singer_name[1:-1]  # Extract text within quotes
        else:
            singer_phrase_text = singer_name

        
        terms = singer_phrase_text.split()  # Split the phrase into terms

        builder = PhraseQuery.Builder()

        for position, term in enumerate(terms):
            builder.add(Term('singer_name', term), position + 1)
        
        singer_phrase_query = builder.build()
        #print(f"Singer query = {singer_phrase_query}")
        singer_exists = 1
    else:
        singer_exists = 0
    

    if album_name != '':
        if album_name.startswith('"') and album_name.endswith('"'):
            album_name_phrase_text = album_name[1:-1]  # Extract text within quotes
        else:
            album_name_phrase_text = album_name

        
        terms = album_name_phrase_text.split()  # Split the phrase into terms

        builder = PhraseQuery.Builder()

        for position, term in enumerate(terms):
            builder.add(Term('name', term), position + 1)
        
        album_name_phrase_query = builder.build()
        #print(f"Song query = {song_phrase_query}")
        album_name_exists = 1
    else:
        album_name_exists = 0
    

    
    if album_type != '':
        if album_type.startswith('"') and album_type.endswith('"'):
            album_type_phrase_text = album_type[1:-1]  # Extract text within quotes
        else:
            album_type_phrase_text = album_type

        
        terms = album_type_phrase_text.split()  # Split the phrase into terms

        builder = PhraseQuery.Builder()

        for position, term in enumerate(terms):
            builder.add(Term('type', term), position + 1)
        
        album_type_phrase_query = builder.build()
        #print(f"Song query = {song_phrase_query}")
        album_type_exists = 1
    else:
        album_type_exists = 0

    
    boolean_query_builder = BooleanQuery.Builder()

    if singer_exists  == 1 :
        boolean_query_builder.add(singer_phrase_query, BooleanClause.Occur.MUST)
    
    if album_name_exists == 1:
        boolean_query_builder.add(album_name_phrase_query, BooleanClause.Occur.MUST)

    if album_type_exists == 1:
        boolean_query_builder.add(album_type_phrase_query, BooleanClause.Occur.MUST)
    
    combined_query = boolean_query_builder.build()
    #print(f"Combined query is : {combined_query}")

    k_output = int(input("How many results to output: "))
    if int(k_output) == '':
        k_output = 10


    reader = DirectoryReader.open(fsDir)
    searcher = IndexSearcher(reader)
    start_time = time.time()  # Start the timer

    topDocs = searcher.search(combined_query, int(k_output))  # Retrieving top documents

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