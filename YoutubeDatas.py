import streamlit as st
import google.auth
import google.oauth2.credentials
from googleapiclient.errors import HttpError
from google.auth.exceptions import DefaultCredentialsError
import json
import pymongo
import pandas as pd
import pymysql
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
import mysql.connector
from isodate import parse_duration
import sqlite3

# Create a custom theme with a violet background
st.set_page_config(
    layout="centered",
    initial_sidebar_state="auto",
    page_title="Loading page",
    page_icon="🌈"
)

st.success("Hi..")

# Set up the sidebar
st.sidebar.title("YouTube Data Warehouse")
menu = ["Home", "About"]
choice = st.sidebar.selectbox("Select an option", menu)

# Set up the home page
if choice == "Home":
    st.title("Welcome to the YouTube Data Harvesting App")
    st.write("This app allows you to collect and analyze data from multiple YouTube channels.")

# Set up the about page
elif choice == "About":
    st.title("About")
    st.write("This app was created by veera as a project for Data science at the guidence of GUVI.")
    
    
#Connecting mongodb
myclient = pymongo.MongoClient("mongodb://localhost:27017")


def get_channel_info(channel_id):
    # Retrieve the channel information from the YouTube API
    channel_response = youtube.channels().list(
        part='snippet,contentDetails,statistics',
        id=channel_id
    ).execute()

    #store the desired channel data which are extracted from youtube into a dictionary
    ch_details = {}
    for channel in channel_response['items']:
        ch_details = {'channel_id' : channel['id'] , 'title' : channel['snippet']['title'], 'description' : channel['snippet']['description'], 'subscriber_count' : channel['statistics']['subscriberCount'], 'video_count' : channel['statistics']['videoCount']}
    
    return(ch_details)

def get_playlist_info(channel_id):
    # Retrieve the PLaylist information from the YouTube API
    playlist_response = youtube.playlists().list(
        part = "snippet,contentDetails",
        channelId = channel_id,
        maxResults = 50
    ).execute()

    #store the desired playlist data which are extracted from youtube into a dictionary with dynamite key
    pl_details = {}
    pl_no = 1
    for pl in playlist_response['items']:
        playlist_key = 'playlist_' + str(pl_no)
        pl_details[playlist_key] = {'playlist_id' : pl['id'], 'channel_id' : pl['snippet']['channelId'], 'title' : pl['snippet']['title']}
        pl_no += 1
    
    return(pl_details)

def get_video_info(playlist_id):
    # Retrieve the playlistItems from the YouTube API
    videoid_response=youtube.playlistItems().list(
        part='snippet,contentDetails',
        playlistId=playlist_id,
        maxResults=50
    ).execute()

    #Extract videoId from playlistItems and store it in a dictionary with dynamite key
    vid = {}
    x = 1
    for videoid in videoid_response['items']:
        video_key = 'video_' + str(x)
        vid[video_key] = {'video_id' : videoid['contentDetails']['videoId']}
        x += 1

    #Retrieve video information from youtube API aith videoId as input and append in the vid dictionary
    for n in vid:
        video_response = youtube.videos().list(
            part='snippet,contentDetails,statistics',
            id=vid[n]['video_id']
        ).execute()
        if  len(video_response['items']) > 0:
            vid[n]['title'] = video_response['items'][0]['snippet']['title']
            vid[n]['description'] = video_response['items'][0]['snippet']['description']
            vid[n]['published_at'] = video_response['items'][0]['snippet']['publishedAt']
            vid[n]['view_count'] = video_response['items'][0]['statistics']['viewCount']
            vid[n]['like_count'] = video_response['items'][0]['statistics'].get('likeCount','0')
            vid[n]['favorite_count'] = video_response['items'][0]['statistics']['favoriteCount']
            vid[n]['comment_count'] = video_response['items'][0]['statistics'].get('commentCount','0')
            vid[n]['thumbnail_url'] = video_response['items'][0]['snippet']['thumbnails']['default']['url']
            vid[n]['duration'] = video_response['items'][0]['contentDetails']['duration']
            vid[n]['caption_status'] = video_response['items'][0]['contentDetails']['caption']
        else:
            vid[n]['title'] = "Video not available"
            vid[n]['description'] = ""
            vid[n]['published_at'] = ""
            vid[n]['view_count'] = "0"
            vid[n]['like_count'] = "0"
            vid[n]['favorite_count'] = "0"
            vid[n]['comment_count'] = "0"
            vid[n]['duration'] = ""
            vid[n]['caption_status'] = ""
    return(vid)

def get_comment_info(video_id):
    try:
        #Retrieve comments from a video from youtube API
        comment_response=youtube.commentThreads().list(
            part="snippet",
            videoId=video_id
        ).execute()

        #Extract the desired data from response and store in a dictionary with dynamtite key
        cmnts = {}
        cno = 1
        for comment in comment_response['items']:
            comment_key = 'comment_' + str(cno)
            cmnts[comment_key] = {'comment_id' : comment['id'], 'comment_text' : comment['snippet']['topLevelComment']['snippet']['textDisplay'], 'author_name' : comment['snippet']['topLevelComment']['snippet']['authorDisplayName'], 'published_at' : comment['snippet']['topLevelComment']['snippet']['publishedAt']}
            cno += 1
        return(cmnts)

    except HttpError as e:
        pass

def mongodb_to_sql(ch_name):
    documents = mycollection.find({'title': ch_name})

    # Prepare lists to store the data
    channel_data = []
    playlists = []
    videos = []
    comments = []

    for document in documents:
        channel = {
            'channel_id': document['channel_id'],
            'channel_name': ch_name,
            'description': document['description'],
            'video_count': document['video_count'],
            'subscriber_count': document['subscriber_count']
        }
        channel_data.append(channel)

        channel_playlists = document.get('playlists', {})
        for playlist in channel_playlists.values():
            playlist_id = playlist['playlist_id']
            playlist_data = {
                'playlist_id': playlist_id,
                'channel_id' : document['channel_id'],
                'playlist_title': playlist['title']
            }
            playlists.append(playlist_data)

            for video in playlist['videos'].values():
                video['playlist_id'] = playlist_id
                videos.append({key: value for key, value in video.items() if key != 'comments'})
                if video['comments'] is not None:
                    video_comments = video['comments'].values()
                    if video_comments:
                        for comment in video_comments:
                            comment['video_id'] = video['video_id']
                            comments.append(comment)

    # Create DataFrames
    channel_df = pd.DataFrame(channel_data)
    playlists_df = pd.DataFrame(playlists)
    videos_df = pd.DataFrame(videos)
    videos_df.drop_duplicates(subset = "video_id", keep = 'first', inplace = True)
    comments_df = pd.DataFrame(comments)
    comments_df.drop_duplicates(subset = "comment_id", keep = 'first', inplace = True)
    # st.dataframe(channel_df)
    # st.dataframe(playlists_df)
    # st.dataframe(videos_df)
    # st.dataframe(comments_df)

    # Connect to MySQL
    engine = create_engine('mysql+pymysql://root:W%402915djkq%23@localhost:3306/youtube_db')

    table1 = 'channel_details'
    table2 = 'playlist_details'
    table3 = 'video_details'
    table4 = 'comment_details'


    try:
        channel_df.to_sql(name=table1, con=engine, if_exists='append', index=False)
        playlists_df.to_sql(name=table2, con=engine, if_exists='append', index=False)
        videos_df.to_sql(name=table3, con=engine, if_exists='append', index=False)
        comments_df.to_sql(name=table4, con=engine, if_exists='append', index=False)
        st.success("Migrated from MongoDB to MySQL")
    except IntegrityError:
        st.error("Integrity error occurred. Some data already exists in MySQL.")

    # Close the database connection
    engine.dispose()
    mycollection.delete_one({'title': ch_name})

#def query_mysql(question):

#Designing streamlit app

st.title('**:blue[Youtube Data]**')
tab1,tab2,tab3 = st.tabs(["Youtube API to MongoDB", "MySQL", "Queries"])
with tab1:
    col1,col2,col3 = st.columns(3)
    try:
        with col1:
                # Get input from user
                API_KEY = st.text_input("Enter API Key:")
                channel_id = st.text_input('Enter channel ID:')
                clicked1 = st.button('Search')

        with col2:
            if col1.clicked1:
                from googleapiclient.discovery import build

                # Set up the YouTube API client
                youtube = build('youtube', 'v3', developerKey=API_KEY)
                try:
                    channel_details = get_channel_info(channel_id)
                    playlist_details = get_playlist_info(channel_details['channel_id'])

                    #Iterate over the keys in dictionary and get video and comments info
                    for i in playlist_details:
                        playlist_id = playlist_details[i]['playlist_id']
                        video_details = get_video_info(playlist_id)
                        for j in video_details:
                            video_id = video_details[j]['video_id']
                            comments = get_comment_info(video_id)
                            video_details[j]['comments'] = comments
                        playlist_details[i]['videos'] = video_details
                    channel_details['playlists'] = playlist_details
                    st.json(channel_details)

                except HttpError as error:
                    pass
                except KeyError as error:
                    pass
    except DefaultCredentialsError:
        pass

    with col3:
        clicked2 = st.button('Load into MongoDB')
        if clicked2:
            mydb = myclient.youtube_db
            mycollection = mydb.channel_details
            mycollection.insert_one(channel_details)
            st.success("Channel details loaded in mongodb")
with tab2:
    mydb = myclient.youtube_db
    mycollection = mydb.channel_details
    channel_name = mycollection.find({},{"_id" : 0, "title" : 1})
    name = []
    for names in channel_name:
        channel_title = names['title']
        name.append(channel_title)
    option = st.selectbox('Select a channel to move the channel details into MySQL',(name))
    clicked3 = st.button("Migrate from MongoDB to MySQL")
    if clicked3:
        mongodb_to_sql(option)

with tab3:
    ques1 = '1.	What are the names of all the videos and their corresponding channels?'
    ques2 = '2.	Which channels have the most number of videos, and how many videos do they have?'
    ques3 = '3.	What are the top 10 most viewed videos and their respective channels?'
    ques4 = '4.	How many comments were made on each video, and what are their corresponding video names?'
    ques5 = '5.	Which videos have the highest number of likes, and what are their corresponding channel names?'
    ques6 = '6.	What is the total number of likes and dislikes for each video, and what are their corresponding video names?'
    ques7 = '7.	What is the total number of views for each channel, and what are their corresponding channel names?'
    ques8 = '8.	What are the names of all the channels that have published videos in the year 2022?'
    ques9 = '9.	What is the average duration of all videos in each channel, and what are their corresponding channel names?'
    ques10 = '10.	Which videos have the highest number of comments, and what are their corresponding channel names?'
    question = st.selectbox('Lets find something..!',(ques1,ques2,ques3,ques4,ques5,ques6,ques7,ques8,ques9,ques10))
    clicked4 = st.button("Go..")
    if clicked4:
        mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="W@2915djkq#",
        database="youtube_db"
        )
        cursor = mydb.cursor()

        if question == ques1:
            query = "select c.channel_name,v.title FROM channel_details as c JOIN playlist_details as p JOIN video_details as v ON v.playlist_id = p.playlist_id and p.channel_id = c.channel_id"
        elif question == ques2:
            query = "SELECT channel_name,video_count FROM channel_details ORDER BY video_count DESC"
        elif question == ques3:
            query = "SELECT channel_name,v.title,view_count FROM channel_details as c JOIN playlist_details as p JOIN video_details as v ON v.playlist_id = p.playlist_id and p.channel_id = c.channel_id ORDER BY view_count DESC LIMIT 10"
        elif question == ques4:
            query = "SELECT title,comment_count from video_details ORDER BY comment_count DESC"
        elif question == ques5:
            query = "SELECT c.channel_name, v.title, v.like_count FROM channel_details as c JOIN playlist_details as p JOIN video_details as v ON v.playlist_id = p.playlist_id and p.channel_id = c.channel_id ORDER BY like_count DESC"
        elif question == ques6:
            query = "SELECT title,like_count from video_details ORDER BY like_count DESC"
        elif question == ques7:
            query = "SELECT c.channel_name,sum(v.view_count) as total_views FROM channel_details as c JOIN playlist_details as p JOIN video_details as v ON v.playlist_id = p.playlist_id and p.channel_id = c.channel_id GROUP BY channel_name ORDER BY sum(view_count) DESC"
        elif question == ques8:
            query = "SELECT c.channel_name, COUNT(video_id) as no_of_videos_published_in_2022 FROM video_details as v JOIN channel_details as c JOIN playlist_details as p ON v.playlist_id = p.playlist_id and p.channel_id = c.channel_id WHERE v.published_at LIKE '2022%' GROUP BY c.channel_name"
        elif question == ques9:
            query = "SELECT channel_name, duration FROM channel_details as c JOIN playlist_details as p JOIN video_details as v ON v.playlist_id = p.playlist_id and p.channel_id = c.channel_id"
        elif question == ques10:
            query = "SELECT c.channel_name, v.title, v.comment_count FROM channel_details as c JOIN playlist_details as p JOIN video_details as v ON v.playlist_id = p.playlist_id and p.channel_id = c.channel_id ORDER BY comment_count DESC"
        cursor.execute(query)
        results = cursor.fetchall()

        # Get column names from cursor description
        column_names = [desc[0] for desc in cursor.description]

        # Create DataFrame with column names and fetched results
        df = pd.DataFrame(results, columns=column_names)
        if question == ques9:
            # Convert duration to seconds
            df['duration_seconds'] = df['duration'].apply(lambda x: parse_duration(x).total_seconds())

            # Calculate average duration per channel
            average_duration = df.groupby('channel_name')['duration_seconds'].mean()
            df_average_duration = pd.DataFrame(average_duration)
            st.dataframe(df_average_duration)
        else:
            st.dataframe(df)

        cursor.close()
        mydb.close()
        