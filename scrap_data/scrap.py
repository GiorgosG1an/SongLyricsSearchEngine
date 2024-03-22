"""
File: scrap.py
Authors: Giannopoulos Georgios, Ioannis Giannopoulos
Purpose: Scrap data from AZlyrics website
License: This script is licensed under Apache License v2.0.

Project: FALSE search engine 
Course: Information Retrieval and Mining
University: University of Peloponnese
Department: Informatics and Telecommunications
Date : December 22, 2023

Description:
This project is developed as part of the Information Retrieval and Mining course at the University of Peloponnese, Department of Informatics and Telecommunications. 

"""

import urllib.request
from bs4 import BeautifulSoup as bs
from time import sleep
import re
import csv
import pandas as pd
import lucene
import  time
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

# Function to index documents 
def add_csv_to_index(new_data_df,file):

    print("Indexing documents:")
    try:
        existing_fsDir = MMapDirectory(Paths.get('data/index/main_index'))
        # Initialize IndexWriterConfig with the analyzer
        writerConfig = IndexWriterConfig(StandardAnalyzer())
        # Initialize IndexWriter with the existing index and config
        index_writer = IndexWriter(existing_fsDir, writerConfig)
        new_data_df = pd.read_csv(file)

        # Preprocess the data
        new_data_df['stemmed_lyrics'] = new_data_df['lyrics'].apply(preprocess_data)
        # Save the preprocessed data to a new CSV file
        new_data_df.to_csv(file, index=False)
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

def add_album_csv_to_index(file):
    print("Indexing documents:")
    try:
        existing_fsDir = MMapDirectory(Paths.get('data/index/album_index'))
        # Initialize IndexWriterConfig with the analyzer
        writerConfig = IndexWriterConfig(StandardAnalyzer())
        # Initialize IndexWriter with the existing index and config
        index_writer = IndexWriter(existing_fsDir, writerConfig)
        new_data_df = pd.read_csv(file)

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


# ----------------------------              Scraping begins here                -----------------------------------------------------
def scrap_from_azlyrics():
    url = "https://www.azlyrics.com/lyrics/{}/{}.html" #azlyrics url pattern https://www.azlyrics.com/lyrics/artist/song.html
    short_url = "../lyrics/{}/{}.html"
    file = "scrap_data/scraped_azlyrics.csv"  

    delay = 0.1

    artist = input("Enter artist: ")
    song = input("Enter a song by the same artist: ")
    a= re.sub(r"\s+", "", artist).lower()#reg expression to remove spaces from artist name and convert it to lowercase for urls purposes
    s = re.sub(r"\s+", "", song).lower()#reg expression to remove spaces from song name and convert it to lowercase for url purposes

    with open(file, "w", newline='', encoding='utf-8') as csvfile:
        fieldnames = ['singer_name', 'song_name', 'link', 'lyrics']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames,delimiter=',')
        writer.writeheader()

        f_url = url.format(a, s)
        short_url = short_url.format(a,s)
        #print(f_url)
        try:

            html_page = urllib.request.urlopen(f_url).read()
            soup = bs(html_page, 'html.parser')
            #for artist
            '''
            HTML code in azlyrics
                <div class="lyricsh">
                    <h2><a href="//www.azlyrics.com/s/{artist}.html"><b>{artist}</b></a></h2>
                </div>
            '''
            #find div with class = 'lyricsh' where the artist name is located
            html_pointer_artist = soup.find('div', attrs={'class': 'lyricsh'})

            artist = html_pointer_artist.find_next('b').contents[0].strip()
            #print(artist)

            #for lyrics
            soup = bs(html_page, 'html.parser')
            html_pointer = soup.find('div', attrs={'class': 'ringtone'})
            song_name = html_pointer.find_next('b').contents[0].strip()
            song_name = song_name.replace('"','')
            lyrics = html_pointer.find_next('div').text.strip()


            # Write data to CSV
            writer.writerow({'singer_name': artist, 'song_name': song_name, 'link': short_url,'lyrics': lyrics})
            print("Lyrics successfully written to file for: " +artist  +song_name)

        except Exception as e:
            print("Lyrics not found for: " + song)
            print(e)

        finally:
            sleep(delay)

    df = pd.read_csv(file)
    pd.set_option('display.max_colwidth', None)
    add_csv_to_index(df,file)    #       index and add to the csv 

def scrap_album_from_azlyrics():
    url = "https://www.azlyrics.com/{}/{}.html" #azlyrics url pattern https://www.azlyrics.com/lyrics/first_letter_of_arist/artist.html
    short_url = "../lyrics/{}/{}.html"
    file = "scrap_data/scraped_album_azlyrics.csv"  

    delay = 0.1

    artist = input("Enter artist: ")
    a1 = artist[0]
    a2= re.sub(r"\s+", "", artist).lower()#reg expression to remove spaces from artist name and convert it to lowercase for urls purposes


    with open(file, "w", newline='', encoding='utf-8') as csvfile:
        fieldnames = ['id','singer_name', 'name', 'type', 'year']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames,delimiter=',')
        writer.writeheader()

        f_url = url.format(a1, a2)
        print(f_url)

        try:

            html_page = urllib.request.urlopen(f_url).read()
            soup = bs(html_page, 'html.parser')

            # Find the <strong> tag within the <h1> tag
            artist_tag = soup.find('h1').find('strong')

            if artist_tag:
                artist_name = artist_tag.get_text(strip=True)
                #print("Artist:", artist_name)
            else:
                print("Artist information not found.")

            # Find all div elements with class 'album'
            album_divs = soup.find_all('div', class_='album')

#<div id={id} class="album">{album_type}: <b>"{album_name}"</b> ({year})</div>
            
            for album_div in album_divs:
                # Extracting the ID from the div's ID attribute
                album_id = album_div.get('id')

                # Extracting album type from the class attribute
                # Get the text within the div element
                album_text = album_div.get_text(strip=True)
                album_type_start = album_text.find('>') + 1
                album_type_end = album_text.find(':')
                album_type = album_text[album_type_start:album_type_end].strip()

                # Get the text within the div element
                album_text = album_div.get_text(strip=True)

                # Extracting information using string manipulation
                album_name_start = album_text.find(':') + 1
                album_name_end = album_text.find('(')
                album_name = album_text[album_name_start:album_name_end].strip().strip('"')

                album_year_start = album_text.find('(') + 1
                album_year_end = album_text.find(')')
                year = album_text[album_year_start:album_year_end].strip()

                # Write data to CSV
                writer.writerow({'id':album_id,'singer_name': artist_name, 'name':album_name, 'type': album_type,'year': year})

                #print("Extracted Album Information:", album_info)
        except Exception as e:

            print(f"Problem:{e}")

        finally:

            sleep(delay)
    file_path = 'scrap_data/scraped_album_azlyrics.csv'
    scrapped_album = pd.read_csv(file_path)

#<div class="album"><b>other songs:</b></div>
    condition_column = 'year'
    search_value = 'other songs'
    # Find the row index where the condition is met
    row_index = scrapped_album.index[scrapped_album[condition_column] == search_value].tolist()

    if row_index:
        # Change data in another column of that row
        column_year = 'year'  
        new_value = 'Not Defined'  
        column_name = 'name'
        new_name = 'other songs'
        # Update the specified column's data in the row that meets the condition
        scrapped_album.loc[row_index[0], column_year] = new_value
        scrapped_album.loc[row_index[0], column_name] = new_name

        # Save the updated DataFrame back to the CSV file
        scrapped_album.to_csv(file_path, index=False)
    
    add_album_csv_to_index(file)