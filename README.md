# Songs Lyrics Search Engine


## Description

This is a search engine built with [PyLucene](https://lucene.apache.org/pylucene/index.html) that allows users to search for songs based on the name, artist, and even within the lyrics of the song. It uses an indexed database of songs and albums to provide fast and accurate search results.
---
## Installation
1. Set up Docker on your computer. You can find a guide in ``IRM_Project1_Report.pdf`` for setting up in linux operating system
2. Pull the image:
```cmd
docker pull giorgosgian/irm_project:version1
```
3. Based on the directory where you have downloaded the project run:
```cmd
docker run -it -v {directory}:/project giorgosgian/irm_project:version1 bash
```
4. Go to ``project`` folder we created
```cmd
cd project
```
5. Run the ``main.py`` file
```cmd
python3 main/main.py
```
---
## Usage
1. Preprocess the data
2. Index the data
3. Search songs/albums
4. Add new songs/albums with input, from a csv or from [AZLyrics](https://www.azlyrics.com/) website
5. Delete songs/albums

## License
The project was created as the first semester project of Information Retrieval and Mining course, at University of Peloponnese dep. Informatics and Telecommunications.
See [License](LICENSE), [CREDITS](Credits), [User guide](IRM_Project1_Report.pdf) for more
