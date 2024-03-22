"""
File: main.py
Authors: Giannopoulos Georgios, Ioannis Giannopoulos
Purpose: 
License: This script is licensed under Apache License v2.0.

Project: Information Retrieval and Mining Lesson
University: University of Peloponnese
Department: Informatics and Telecommunications
Date : December 22, 2023

Description:
This project is developed as part of the Information Retrieval and Mining lesson at the University of Peloponnese, Department of Informatics and Telecommunications. 

"""

import lucene
# INIT the JavaVM in order to run lucene from python
env = lucene.initVM()

import os, time, pathlib
import pandas as pd
import sys
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
#!!!!!!!!!!!!!!!!!!!!!!!!!!
sys.path.append('/home/georgios/Documents/IRM_project1') #!!!! Replace '/home/georgios/Documents/IRM_project1' with the actual path to your project folder. !!!!
#!!!!!!!!!!!!!!!!!!!!!!!!!!

from preprocessing import preprocess
from indexing import index
from add_docs import add, add_album
from remove_docs import remove, remove_album
from search import search, VSM_search, Phrase_search
from scrap_data import scrap




def display_menu():
    print("_________Menu_________________")
    print("Press 1 to preprocess the data")
    print("Press 2 to index the data")
    print("Press 3 to add a document ")
    print("Press 4 to delete a document ")
    print("Press 5 to search a document ")
    print("Press 6 to scrap data directly from AZlyrics ")

    print("Press 0 to exit from the FALSE search engine")

print("_________________Welcome to the FALSE search engine_______________________")
while True:
    display_menu()

    user_input = input("")
    if user_input == "0":
        print("Exiting FALSE...")
        break
    elif user_input == '1':
        print("Data preprocessing started")

        songs_df = pd.read_csv('data/original_data/songs.csv')
        lyrics_df = pd.read_csv('data/original_data/lyrics.csv')

        # Merge songs and lyrics based on song_name,singer name
        merged_df = pd.merge(songs_df, lyrics_df, left_on=['singer_name','song_name'],right_on=['artist', 'song_name'],how='inner')

        # Drop duplicate/unwanted columns
        merged_df = merged_df.drop(columns=['artist'])
        merged_df = merged_df.drop(columns=['song_href'])

        # Rename columns for better clarity
        merged_df.rename(columns={'singer_name': 'singer_name'}, inplace=True)

        # Write the merged and cleaned data to a new CSV file
        merged_df.to_csv('data/preprocessed_data/merged_songs_lyrics.csv', index=False)    

        # Define columns to normalize
        columns_to_normalize = ['singer_name', 'song_name', 'link','lyrics'] #columns to apply lowercase

        # Apply lowercase normalization function
        normalized_df = preprocess.lowercase_normalize_columns(merged_df, columns_to_normalize)

        # Write the updated data to a new CSV file
        normalized_df.to_csv('data/preprocessed_data/merged_songs_lyrics_normalized.csv', index=False)


        print("Stemming started for column lyrics....")
        # Perform stemming and measure time taken
        stemmed_df, time_taken = preprocess.stem_lyrics(normalized_df)

        # Write the updated data to a new CSV file
        stemmed_df.to_csv('data/preprocessed_data/merged_songs_lyrics_stemmed.csv', index=False)

        # Print the time taken for stemming
        print("Stemming done")
        print(f"Stemming took {time_taken:.2f} seconds.")

        # album csv pre-proccessing
        albums_df = pd.read_csv('data/original_data/albums.csv')
        # Define columns to normalize
        columns_to_normalize = ['singer_name', 'name'] #columns to apply lowercase

        # Apply lowercase normalization function
        normalized_albums_df = preprocess.lowercase_normalize_columns(albums_df, columns_to_normalize)
        
        # Write the updated data to a new CSV file
        normalized_albums_df.to_csv('data/preprocessed_data/albums_normalized.csv', index=False)


        print("Data preprocessing completed")

    elif user_input =='2':
        # Call the function to delete files in the specified folder
        #from indexing import index
        folder_path = 'data/index/main_index'
        album_folder_path = 'data/index/album_index'
        index.delete_files_in_folder(folder_path)
        print("Indexing songs and lyrics...")
        index.index()
        

        print("Album indexing...")
        index.delete_files_in_folder(album_folder_path)
        index.albums_index()
        
    elif user_input =='3':                                            #------------____________ Add Document START ______________________-----------------------
        print("Add document...")
        while True: 
            print("Press 1 to add a new doc in songs and lyrics")
            print("Press 2 to add a new doc in albums")
            answer = input("Enter your input: ")
            if answer == '1': #add new doc in songs and lyrics

                print("Type 1 to add your input")
                print("Type 2 to add an already csv file ")
                print("Type exit in order to return to the main menu")
                answer = input("Enter your input: ").lower()

                if answer =='1':
                    # User inputs
                    singer_name = input("Enter singer name: ")
                    song_name = input("Enter song name: ")
                    link = input("Enter link: ")
                    lyrics = input("Enter lyrics: ")
                        
                    # Add document to index
                    add.add_document_to_index(singer_name, song_name, link, lyrics)
                elif answer == '2':
                    csv_file_name = input("Enter the name of the CSV file: ")

                    # Add '.csv' extension if missing
                    if not csv_file_name.endswith('.csv'):
                        csv_file_name += '.csv'

                    # Specify the directory where the CSV files are located
                    directory = 'user_csv'  # Change this to your directory
                    
                    csv_file_path = os.path.join(directory, csv_file_name)
                    print(csv_file_path)
                    # Check if the file exists
                    if os.path.isfile(csv_file_path):
                        # Check if the required columns are present in the CSV file
                        print(f"Opening {csv_file_name}")
                        if add.check_columns(csv_file_path):
                            print(f"Format is ok\nContinuing with indexing and updating the csv file...")
                            # Read the CSV file
                            new_data = pd.read_csv(csv_file_path)
                            
                            # Append data to the merged CSV and update the index
                            merged_csv_path = 'data/preprocessed_data/merged_songs_lyrics_stemmed.csv'
                            if os.path.isfile(merged_csv_path):
                                existing_data = pd.read_csv(merged_csv_path)
                                #updated_data = pd.concat([existing_data, new_data], ignore_index=True)
                                #updated_data.to_csv(merged_csv_path, index=False)
                                print(f"Data from {csv_file_path} added to the merged CSV file.")
                                #print(updated_data.head())
                                singer_name = new_data['singer_name']
                                song_name = new_data['song_name']
                                link = new_data['link']
                                lyrics = new_data['lyrics']
                                # call indexing function to update the index when user inputs csv file name
                                add.add_csv_to_index(new_data,csv_file_path)
                            else:
                                # Create a new merged CSV if it doesn't exist
                                new_data.to_csv(merged_csv_path, index=False)
                                print(f"{merged_csv_path} created with data from {csv_file_path}.")

                                #add_document_to_index(updated_data[singer_name], updated_data[song_name], updated_data[link], updated_data[lyrics])
                        else:
                            print(f"The CSV file {csv_file_path} doesn't contain the required columns: 'song_name', 'singer_name', 'link', 'lyrics'.")
                    else:
                        print("File does not exist. Please enter a valid CSV file name.")
                elif answer == 'exit':
                    break
                else:
                    print("Invalid input\nPlease enter a valid command...\n")

            # add a new doc in albums
            elif answer == '2':
                print("Type 1 to add your input")
                print("Type 2 to add an already csv file ")
                print("Type exit in order to return to the main menu")
                answer = input("Enter your input: ").lower()

                if answer == '1':
                    singer_name = input("Enter singer name: ")
                    album_name = input("Enter album name: ")
                    album_type = input("Enter album type: ")
                    year = input("Enter album year of release: ")
                    add_album.add_album_document_to_index(singer_name, album_name, album_type, year)
                elif answer == '2':
                    csv_file_name = input("Enter the name of the CSV file: ")

                    # Add '.csv' extension if missing
                    if not csv_file_name.endswith('.csv'):
                        csv_file_name += '.csv'

                    # Specify the directory where the CSV files are located
                    directory = 'user_csv'  # Change this to your directory
                    csv_file_path = os.path.join(directory, csv_file_name)
                    #print(csv_file_path)

                    # Check if the file exists
                    if os.path.isfile(csv_file_path):
                        # Check if the required columns are present in the CSV file
                        print(f"Opening {csv_file_name}")
                        if add_album.check_album_columns(csv_file_path):
                            print(f"Format is ok\nContinuing with indexing and updating the csv file...")
                            # Read the CSV file
                            new_data = pd.read_csv(csv_file_path)

                            
                            # Append data to the merged CSV and update the index
                            merged_csv_path = 'data/preprocessed_data/merged_songs_lyrics_stemmed.csv'

                        # Append data to the merged CSV and update the index
                            merged_csv_path = 'data/preprocessed_data/merged_songs_lyrics_stemmed.csv'
                            if os.path.isfile(merged_csv_path):
                                existing_data = pd.read_csv(merged_csv_path)
                                #updated_data = pd.concat([existing_data, new_data], ignore_index=True)
                                #updated_data.to_csv(merged_csv_path, index=False)
                                print(f"Data from {csv_file_path} added to the merged CSV file.")
                                #print(updated_data.head())
                                singer_name = new_data['singer_name']
                                album_name = new_data['name']
                                album_type = new_data['type']
                                year = new_data['year']
                                # call indexing function to update the index when user inputs csv file name

                                add_album.add_album_csv_to_index(new_data,csv_file_path)
                            else:
                                # Create a new merged CSV if it doesn't exist
                                new_data.to_csv(merged_csv_path, index=False)
                                print(f"{merged_csv_path} created with data from {csv_file_path}.")

                                #add_document_to_index(updated_data[singer_name], updated_data[song_name], updated_data[link], updated_data[lyrics])
                        else:
                            print(f"The CSV file {csv_file_path} doesn't contain the required columns: 'song_name', 'singer_name', 'link', 'lyrics'.")
                    else:
                        print("File does not exist. Please enter a valid CSV file name.")

                elif answer == 'exit':
                    break

                else:
                    print("Invalid input\nPlease enter a valid command...\n")
                
            else:
                print("Wrong input")
                break

        #add()                                                                        -----------------------------_________________ Add Document END _______________------------------------
    elif user_input =='4':                                                           #----------------------------  Remove Document START ----------------------------                                          
        print("Press 1 to remove a song")
        print("Press 2 to remove an album")
        answer = input("Enter your input:")

        if answer == '1':
            
            print("Remove document...")
            #from remove_docs import remove
            singer_names_input = input("Enter singer name : ").split(',')
            song_names_input = input("Enter song name : ").split(',')
            if singer_names_input != '' and singer_names_input != '':
                # Call the remove_from_index function
                remove.remove_from_index(singer_names_input, song_names_input)

            else: 
                pass
        elif answer == '2':
            singer_names_input = input("Enter singer name : ").split(',')
            album_names_input = input("Enter album name : ").split(',')
            if singer_names_input != '' and album_names_input != '':
                # Call the remove_from_index function
                remove_album.remove_album_from_index(singer_names_input, album_names_input)

            else: 
                pass


        else:
            print("Wrong input")
            break


    elif user_input =='5':
        print("Search document...")
        print("Where to search:")
        print("Press 1 to search a song based on artist, song name, lyrics")
        print("Press 2 to search an album based on artist, type of album, year ")
        where = input("Enter your input: ")

        if where == '1':
            # Initialize Lucene objects
            index_directory = "data/index/main_index"  # Replace with your index directory
            fsDir = MMapDirectory(Paths.get(index_directory))
            singer_name = input("Enter singer name:")
            song_name = input("Enter song name:")
            lyrics = input("Enter lyrics:")
            if ( ('AND' or 'OR' or 'NOT') in (singer_name) ) or ( ('AND' or 'OR' or 'NOT') in (song_name) ) or ( ('AND' or 'OR' or 'NOT') in (lyrics) ):
                print("Performing boolean query")
                search.perform_boolean_search(singer_name, song_name, lyrics, fsDir)

            elif '"' in (singer_name or song_name or lyrics):
                print("Performing phrase query")
                Phrase_search.perform_phrase_search(singer_name, song_name, lyrics, fsDir)

            else:
                print("Performing vsm query")
        
                VSM_search.perform_vsm_search(singer_name, song_name, lyrics, fsDir)

        elif where =='2':
                # Initialize Lucene objects
                index_directory = "data/index/album_index"  # Replace with your index directory
                fsDir = MMapDirectory(Paths.get(index_directory))

                singer_name = input("Enter singer name:")
                album_name = input("Enter album name:")
                album_type = input("Enter the type of the album :")
                year = str(input("Enter year of release of the album:"))
                

                if ( ('AND' or 'OR' or 'NOT') in (singer_name) ) or ( ('AND' or 'OR' or 'NOT') in (album_name) ) or ( ('AND' or 'OR' or 'NOT') in (album_type) ):
                    print("Performing boolean query in albums docs...")
                    search.albums_boolean_search(singer_name, album_name,album_type, year, fsDir)
                elif '"' in (singer_name or album_name or album_type):
                    print("Performing phrase query...")
                    Phrase_search.albums_phrase_search(singer_name, album_name, album_type, year,fsDir)
                else:
                    print("Performing vsm query")
            
                    VSM_search.albums_vsm_search(singer_name, album_name, album_type, year,fsDir)
        else:
            print("Wrong input")
            break


    elif user_input =='6':
        print("Scrap data from AZlyrics website:")
        print("Press 1 to scrap song lyrics")
        print("Press 2 to scrap album data for artist")
        answer= input("Enter your input: ")\
        
        if answer == '1':

            scrap.scrap_from_azlyrics()

        elif answer == '2':

            scrap.scrap_album_from_azlyrics()
        else:
            print("Wrong input!")
            break


    else:
        print("Invalid command!!!\nPlease enter a valid command...")

        
    







    
