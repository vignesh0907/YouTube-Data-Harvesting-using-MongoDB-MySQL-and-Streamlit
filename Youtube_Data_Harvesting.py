from googleapiclient.discovery import build
import pandas as pd
import re
import streamlit as st
import pymongo
import pymysql
import mysql.connector
import sqlalchemy
from sqlalchemy import create_engine

st.set_page_config(layout="wide")
st.title("Youtube Data Harvesting and Warehousing")
tab1, tab2, tab3, tab4 = st.tabs(['Project Overview', 'Data Collection and Storage', 'Data Transformation and Migration', 'Performing Different Queries'])

with tab1:
    st.header('Project Overview')
    st.text('''
Youtube Data Harvesting project involves collecting, storing and analyzing data which we are gathering from the Youtube. The Project uses 
Youtube Data API to fetch the information about Channels, videos, comments, etc. The collected data is then stored in MongoDB and MySQL
databases. Which we can use to Analysis and Visualization in future...
        
MongoDB is used to store the unstructured data from the Youtube as it is a NoSQL Database. We have used it to store details about videos,
channels and other related data such as comments,subscriber count, view count,etc. Due this varying data we have used MongoDB.

Project involves in migrating data from MongoDB to SQL DB, so that we have used MySQL which is a relational database used to structure
and organize the collected data and able to query and analyis efficiently.
        
All these data and other related contents are displayed in a Web App using Python's Streamlit lib.''')
    st.header('Main Functionalities')
    st.write("""
1. Data Collection.
2. Data Storage - MongoDB.
3. Data Transformation.
4. Data Migration - MySQL.
5. Performing Queries.""")

with tab2:
    st.header('Data Collection and Storage')
    st.write("Data will be collected by using Youtube API and it's stored into MongoDB")
    
    api_key = 'AIzaSyAWgO1XtSBDEBQZJS2Q8E4qgtDhk17kzYw' # AIzaSyApC8Q9aH0hNyUIiMHwa6SV3-DCJ3EngUQ , AIzaSyAWgO1XtSBDEBQZJS2Q8E4qgtDhk17kzYw
    youtube = build('youtube', 'v3', developerKey = api_key)
    channel_id = st.text_input('Enter Channel ID', value='UC3Izrk2fUSIEwdcH0kNdzeQ') #value='UCEXud8c4yZmQUuVf33Exyiw'
    st.write('You can pass different channel ID, above is default one')  

#CHANNEL DETAILS
    def get_channel_stats(youtube, channel_id):
        try:
            request = youtube.channels().list(
            part = 'snippet, contentDetails, statistics',
            id = channel_id)
            response = request.execute()

            if 'items' not in response:
                st.error(f'Invalid Channel ID - {channel_id}')
                return None
            return response
    
        except:
            st.write('you have exceeded your API Quota')

    data = get_channel_stats(youtube, channel_id)
    Channel_name = data['items'][0]['snippet']['title']
    Channel_id = data['items'][0]['id']
    subscription_count = data['items'][0]['statistics']['subscriberCount']
    views = data['items'][0]['statistics']['viewCount']
    Describe = data['items'][0]['snippet']['description']
    playlist_id = data['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    Total_videos = data['items'][0]['statistics']['videoCount']

#channel details in dict...
    channel_details = {
        'Channel_Name': {
        'Channel_Name': Channel_name,
        'Channel_Id': Channel_id,
        'Video_Count': Total_videos,
        'Subscription_Count': subscription_count,
        'Channel_Views': views,
        'Channel_Description': Describe,
        'Playlist_Id': playlist_id
        }
    }
    #st.write(channel_details)    
   
#to get video ids...
    def get_video_id(youtube,playlist_id):
        video_id = []
        next_page_token = None
        while True :
            request = youtube.playlistItems().list(
                part="contentDetails",
                playlistId = playlist_id,
                maxResults=25,
                pageToken = next_page_token)
            response = request.execute()

            for item in response['items']:
                video_id.append(item['contentDetails']['videoId'])

        #to check there is next page...
            next_page_token = response.get('nextPageToken')
            if not next_page_token:
                break
        return video_id
    video_ids = get_video_id(youtube,playlist_id)
    # st.write(video_ids)

#to get video data...
    def get_video_stats(youtube,video_ids):
        video_data = []
        for video_id in video_ids:
            request = youtube.videos().list(
                part='snippet,statistics,contentDetails',
                id = video_id)
            response = request.execute()
            #st.write("Video Id:",response["items"][0]["id"])

            video = response['items'][0]

        #get cmts if avail...
            try:
                video['comment_threads'] = get_video_cmts(youtube,video_id,max_comments=2) #will call from cmts func
            except:
                video['comment_threads'] = None

        #time format changes
            duration = video.get('contentDetails',{}).get('duration','Not Available')
            if duration != 'Not Available':
                duration = convert_duration(duration) #will call from duration func
            video['contentDetails']['duration'] = duration
            video_data.append(video)

        return video_data

#func to get cmts...
    def get_video_cmts(youtube, video_ids, max_comments):
        request = youtube.commentThreads().list(part = 'snippet',
                    maxResults = max_comments,
                    textFormat = 'plainText',
                    videoId = video_ids)
        response = request.execute()
        return response

#func to convert the duration...
    def convert_duration(duration):
        regex = r'PT(\d+H)?(\d+M)?(\d+S)?'
        match = re.match(regex, duration)
        if not match:
            return '00:00:00'
        hours,minutes,seconds = match.groups()
        hours = int(hours[:-1]) if hours else 0
        minutes = int(minutes[:-1]) if minutes else 0
        seconds = int(seconds[:-1]) if seconds else 0
        total_seconds = hours * 3600 + minutes * 60 + seconds
        return '{:02d}:{:02d}:{:02d}'.format(int(total_seconds / 3600),
                int((total_seconds % 3600) / 60),
                int(total_seconds % 60))

    video_data = get_video_stats(youtube,video_ids)

    videos = {}
    for i,video in enumerate(video_data):
        video_id = video['id']
        video_name = video['snippet']['title']
        video_description = video['snippet']['description']
        tags = video['snippet'].get('tags', [])
        published_at = video['snippet']['publishedAt']
        view_count = video['statistics']['viewCount']
        like_count = video['statistics'].get('likeCount', 0)
        dislike_count = video['statistics'].get('dislikeCount', 0)
        favorite_count = video['statistics'].get('favoriteCount', 0)
        comment_count = video['statistics'].get('commentCount', 0)
        duration = video.get('contentDetails', {}).get('duration', 'Not Available')
        thumbnail = video['snippet']['thumbnails']['high']['url']
        caption_status = video.get('contentDetails', {}).get('caption', 'Not Available')
        comments = 'Unavailable'

    #if cmts are not None...
        if video['comment_threads'] is not None:
                comments = {}
                for index, comment_thread in enumerate(video['comment_threads']['items']):
                    comment = comment_thread['snippet']['topLevelComment']['snippet']
                    comment_id = comment_thread['id']
                    comment_text = comment['textDisplay']
                    comment_author = comment['authorDisplayName']
                    comment_published_at = comment['publishedAt']
                    comments[f"Comment_Id_{index + 1}"] = {
                        'Comment_Id': comment_id,
                        'Comment_Text': comment_text,
                        'Comment_Author': comment_author,
                        'Comment_PublishedAt': comment_published_at
                    }

    # Format processed video data into dictionary        
        videos[f"Video_Id_{i + 1}"] = {
            'Video_Id': video_id,
            'Video_Name': video_name,
            'Video_Description': video_description,
            'Tags': tags,
            'PublishedAt': published_at,
            'View_Count': view_count,
            'Like_Count': like_count,
            'Dislike_Count': dislike_count,
            'Favorite_Count': favorite_count,
            'Comment_Count': comment_count,
            'Duration': duration,
            'Thumbnail': thumbnail,
            'Caption_Status': caption_status,
            'Comments': comments
            }

#combining the channel and video Data into a single dict...
    final_video_op = {**channel_details,**videos}   
    # st.write(videos) 
    # st.write(final_video_op)

#Mongo DB procedure starts...
#Creating the instance with with MongoDB
    client_mongo = pymongo.MongoClient('mongodb://localhost:27017/')

#create a DB or use a new DB
    mydb = client_mongo['Youtube_Data_DB']

#to create a collection
    collection = mydb['Youtube_Data']

#data to insert
    insert_data = {
        'Channel_Name' : Channel_name,
        'Channel_data' : final_video_op
        }

#to upload data into collection if already present need to replace...
    upload_data = collection.replace_one({'_id': Channel_id}, insert_data, upsert = True)
#print it's result...
    st.write(f'Document ID: {upload_data.upserted_id if upload_data.upserted_id else upload_data.modified_count}')
    st.write(final_video_op)
#closing the connection
    client_mongo.close()

with tab3:
    #Data Transformation begins...
    st.header('Data Transformation')
    st.write("The document data stored in MongoDb is transformed to proper data")

    #MongoDB connection...
    client_mongo = pymongo.MongoClient("mongodb://localhost:27017/")
    #create a DB or use a new DB
    mydb = client_mongo['Youtube_Data_DB']
    #to create a collection
    collection = mydb['Youtube_Data']

    document_names = []
    for document in collection.find():
        document_names.append(document['Channel_Name'])
    document_name = st.selectbox('Select Channel Name',options = document_names, key = 'document_names')
   
    result = collection.find_one({'Channel_Name': document_name})
    client_mongo.close()

#conversion of channel details json to df...
    channel_details_to_sql = {
        'Channel_Name': result['Channel_Name'],
        'Channel_Id': result['_id'],
        'Video_Count': result['Channel_data']['Channel_Name']['Video_Count'],
        "Subscriber_Count": result['Channel_data']['Channel_Name']['Subscription_Count'],
        "Channel_Views": result['Channel_data']['Channel_Name']['Channel_Views'],
        "Channel_Description": result['Channel_data']['Channel_Name']['Channel_Description'],
        "Playlist_Id": result['Channel_data']['Channel_Name']['Playlist_Id']
    }
    #df
    channel_df = pd.DataFrame.from_dict(channel_details_to_sql, orient='index').T
    
    #conversion of playlist details json to df...
    playlist_to_sql = {"Channel_Id": result['_id'],
        "Playlist_Id": result['Channel_data']['Channel_Name']['Playlist_Id']}
    #df
    playlist_df = pd.DataFrame.from_dict(playlist_to_sql, orient='index').T

    #conversion of video details json to df...
    video_details_list = []
    for i in range(1,len(result['Channel_data'])-1):
        video_details_to_sql = {
            'Playlist_Id': result['Channel_data']['Channel_Name']['Playlist_Id'],
            'Video_Id': result['Channel_data'][f"Video_Id_{i}"]['Video_Id'],
            'Video_Name': result['Channel_data'][f"Video_Id_{i}"]['Video_Name'],
            'Video_Description': result['Channel_data'][f"Video_Id_{i}"]['Video_Description'],
            'Published_date': result['Channel_data'][f"Video_Id_{i}"]['PublishedAt'],
            'View_Count': result['Channel_data'][f"Video_Id_{i}"]['View_Count'],
            'Like_Count': result['Channel_data'][f"Video_Id_{i}"]['Like_Count'],
            'Dislike_Count': result['Channel_data'][f"Video_Id_{i}"]['Dislike_Count'],
            'Favorite_Count': result['Channel_data'][f"Video_Id_{i}"]['Favorite_Count'],
            'Comment_Count': result['Channel_data'][f"Video_Id_{i}"]['Comment_Count'],
            'Duration': result['Channel_data'][f"Video_Id_{i}"]['Duration'],
            'Thumbnail': result['Channel_data'][f"Video_Id_{i}"]['Thumbnail'],
            'Caption_Status': result['Channel_data'][f"Video_Id_{i}"]['Caption_Status']
            }
        video_details_list.append(video_details_to_sql)
    #df
    video_df = pd.DataFrame(video_details_list)

    #cmts data json to df
    comment_details_list = []
    for i in range(1, len(result['Channel_data'])-1):
        comments_access = result['Channel_data'][f'Video_Id_{i}']['Comments']
        #if cmts not avail
        if comments_access == 'Unavailable' or ('Comment_Id_1' not in comments_access or 'Comment_Id_2' not in comments_access):
            Comment_details_to_sql = {
                'Video_Id': 'Unavailable',
                    'Comment_Id': 'Unavailable',
                    'Comment_Text': 'Unavailable',
                    'Comment_Author':'Unavailable',
                    'Comment_Published_date': 'Unavailable'
                }
            comment_details_list.append(Comment_details_to_sql)
        #if cmts avail
        else:
            for j in range(1,3):
                Comment_details_to_sql = {
                    'Video_Id': result['Channel_data'][f'Video_Id_{i}']['Video_Id'],
                    'Comment_Id': result['Channel_data'][f"Video_Id_{i}"]['Comments'][f"Comment_Id_{j}"]['Comment_Id'],
                    'Comment_Text': result['Channel_data'][f"Video_Id_{i}"]['Comments'][f"Comment_Id_{j}"]['Comment_Text'],
                    'Comment_Author': result['Channel_data'][f"Video_Id_{i}"]['Comments'][f"Comment_Id_{j}"]['Comment_Author'],
                    'Comment_Published_date': result['Channel_data'][f"Video_Id_{i}"]['Comments'][f"Comment_Id_{j}"]['Comment_PublishedAt'],
                }
                comment_details_list.append(Comment_details_to_sql)
    #df
    comment_df = pd.DataFrame(comment_details_list)

#Migration to MySQL DB...
    connect_mysql = mysql.connector.connect(
    host = '127.0.0.1',
    port = 3303,
    user = 'root',
    password = 'root',
    )
    mycursor = connect_mysql.cursor()
    #creating the DB
    mycursor.execute('CREATE DATABASE IF NOT EXISTS youtube_db')
    mycursor.close()
    connect_mysql.close()
    #connecting to newly created DB using 
    engine = create_engine('mysql+mysqlconnector://root:root@127.0.0.1:3303/youtube_db', echo=False)

#using pandas to insert the Data Frames to the SQL Database, Creating tables and inserting values into them....
    #channel data to sql
    channel_df.to_sql('channel', engine, if_exists='append', index=False,
                    dtype = {
                        'Channel_Name': sqlalchemy.types.VARCHAR(length=225),
                        "Channel_Id": sqlalchemy.types.VARCHAR(length=225),
                        "Video_Count": sqlalchemy.types.INT,
                        "Subscriber_Count": sqlalchemy.types.BigInteger,
                        "Channel_Views": sqlalchemy.types.BigInteger,
                        "Channel_Description": sqlalchemy.types.TEXT,
                        "Playlist_Id": sqlalchemy.types.VARCHAR(length=225),
                    } )
    #playlist data to sql
    playlist_df.to_sql('playlist', engine, if_exists='append', index=False,
                dtype = {"Channel_Id": sqlalchemy.types.VARCHAR(length=225),
                        "Playlist_Id": sqlalchemy.types.VARCHAR(length=225),})
    #video data to sql
    video_df.to_sql('video', engine, if_exists = 'append', index = False,
                dtype= {'Playlist_Id': sqlalchemy.types.VARCHAR(length=225),
                        'Video_Id': sqlalchemy.types.VARCHAR(length=225),
                        'Video_Name': sqlalchemy.types.VARCHAR(length=225),
                        'Video_Description': sqlalchemy.types.TEXT,
                        'Published_date': sqlalchemy.types.String(length=50),
                        'View_Count': sqlalchemy.types.BigInteger,
                        'Like_Count': sqlalchemy.types.BigInteger,
                        'Dislike_Count': sqlalchemy.types.INT,
                        'Favorite_Count': sqlalchemy.types.INT,
                        'Comment_Count': sqlalchemy.types.INT,
                        'Duration': sqlalchemy.types.VARCHAR(length=1024),
                        'Thumbnail': sqlalchemy.types.VARCHAR(length=225),
                        'Caption_Status': sqlalchemy.types.VARCHAR(length=225),
                        } )
    #cmts data to sql
    comment_df.to_sql('comments',engine, if_exists='append', index=False,
                dtype = {'Video_Id': sqlalchemy.types.VARCHAR(length=225),
                        'Comment_Id': sqlalchemy.types.VARCHAR(length=225),
                        'Comment_Text': sqlalchemy.types.TEXT,
                        'Comment_Author': sqlalchemy.types.VARCHAR(length=225),
                        'Comment_Published_date': sqlalchemy.types.String(length=50),
                        } )
with tab4:
    st.header('Performing Queries on Collected Data')    
    # query as dropdown...
    questions = st.selectbox('Please select a question.',
('1. What are the names of all the videos and their corresponding channels?',
'2. Which channels have the most number of videos, and how many videos do they have?',
'3. What are the top 10 most viewed videos and their respective channels?',
'4. How many comments were made on each video, and what are their corresponding video names?',
'5. Which videos have the highest number of likes, and what are their corresponding channel names?',
'6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
'7. What is the total number of views for each channel, and what are their corresponding channel names?',
'8. What are the names of all the channels that have published videos in the year 2022?',
'9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
'10. Which videos have the highest number of comments, and what are their corresponding channel names?'), key = 'collection_question')
    
    #creating connection for SQL DB
    connecting_for_que = pymysql.connect(host = '127.0.0.1', port=3303, user='root',password='root', db='youtube_db')
    cursor = connecting_for_que.cursor()

    # que 1
    if questions == '1. What are the names of all the videos and their corresponding channels?':
        cursor.execute('SELECT DISTINCT channel.Channel_Name, video.Video_Name FROM channel JOIN playlist ON channel.Channel_ID = playlist.Channel_Id JOIN video ON playlist.Playlist_Id = video.Playlist_Id;')
        ans1 = cursor.fetchall()
        df1 = pd.DataFrame(ans1, columns = ['Channel Name', 'Video Name']).reset_index(drop = True)
        df1.index += 1
        st.dataframe(df1)
    # que2
    elif questions == '2. Which channels have the most number of videos, and how many videos do they have?':
        cursor.execute("SELECT DISTINCT Channel_Name, Video_Count FROM channel ORDER BY Video_Count DESC;")
        ans2 = cursor.fetchall()
        df2 = pd.DataFrame(ans2,columns=['Channel Name','Video Count']).reset_index(drop=True)
        df2.index += 1
        st.dataframe(df2)
    # que3
    elif questions == '3. What are the top 10 most viewed videos and their respective channels?':
        cursor.execute("SELECT DISTINCT channel.Channel_Name, video.Video_Name, video.View_Count FROM channel JOIN playlist ON channel.Channel_Id = playlist.Channel_Id JOIN video ON playlist.Playlist_Id = video.Playlist_Id ORDER BY video.View_Count DESC LIMIT 10;")
        ans3 = cursor.fetchall()
        df3 = pd.DataFrame(ans3,columns=['Channel Name', 'Video Name', 'View count']).reset_index(drop=True)
        df3.index += 1
        st.dataframe(df3)
    # que4
    elif questions == '4. How many comments were made on each video, and what are their corresponding video names?':
        cursor.execute("SELECT DISTINCT channel.Channel_Name, video.Video_Name, video.Comment_Count FROM channel JOIN playlist ON channel.Channel_Id = playlist.Channel_Id JOIN video ON playlist.Playlist_Id = video.Playlist_Id;")
        ans4 = cursor.fetchall()
        df4 = pd.DataFrame(ans4,columns=['Channel Name', 'Video Name', 'Comment count']).reset_index(drop=True)
        df4.index += 1
        st.dataframe(df4)
    # que5
    elif questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        cursor.execute("SELECT DISTINCT channel.Channel_Name, video.Video_Name, video.Like_Count FROM channel JOIN playlist ON channel.Channel_Id = playlist.Channel_Id JOIN video ON playlist.Playlist_Id = video.Playlist_Id ORDER BY video.Like_Count DESC;")
        ans5= cursor.fetchall()
        df5 = pd.DataFrame(ans5,columns=['Channel Name', 'Video Name', 'Like count']).reset_index(drop=True)
        df5.index += 1
        st.dataframe(df5)
    # que6
    elif questions == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
    # st.write('**Note:- In November 2021, YouTube removed the public dislike count from all of its videos.**')
        cursor.execute("SELECT DISTINCT channel.Channel_Name, video.Video_Name, video.Like_Count, video.Dislike_Count FROM channel JOIN playlist ON channel.Channel_Id = playlist.Channel_Id JOIN video ON playlist.Playlist_Id = video.Playlist_Id ORDER BY video.Like_Count DESC;")
        ans6= cursor.fetchall()
        df6 = pd.DataFrame(ans6,columns=['Channel Name', 'Video Name', 'Like count','Dislike count']).reset_index(drop=True)
        df6.index += 1
        st.dataframe(df6)
    # que7
    elif questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
        cursor.execute("SELECT DISTINCT Channel_Name, Channel_Views FROM channel ORDER BY Channel_Views DESC;")
        ans7= cursor.fetchall()
        df7 = pd.DataFrame(ans7,columns=['Channel Name', 'Total number of views']).reset_index(drop=True)
        df7.index += 1
        st.dataframe(df7)
    # que8
    elif questions == '8. What are the names of all the channels that have published videos in the year 2022?':
        cursor.execute("SELECT DISTINCT channel.Channel_Name, video.Video_Name, video.Published_date FROM channel JOIN playlist ON channel.Channel_Id = playlist.Channel_Id JOIN video ON playlist.Playlist_Id = video.Playlist_Id  WHERE EXTRACT(YEAR FROM Published_date) = 2022;")
        ans8= cursor.fetchall()
        df8 = pd.DataFrame(ans8,columns=['Channel Name','Video Name', 'Year 2022 only']).reset_index(drop=True)
        df8.index += 1
        st.dataframe(df8)
    # que9
    elif questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        cursor.execute("SELECT DISTINCT channel.Channel_Name, TIME_FORMAT(SEC_TO_TIME(AVG(TIME_TO_SEC(TIME(video.Duration)))), '%H:%i:%s') AS duration  FROM channel JOIN playlist ON channel.Channel_Id = playlist.Channel_Id JOIN video ON playlist.Playlist_Id = video.Playlist_Id GROUP by Channel_Name ORDER BY duration DESC ;")
        ans9= cursor.fetchall()
        df9 = pd.DataFrame(ans9,columns=['Channel Name','Average duration of videos (HH:MM:SS)']).reset_index(drop=True)
        df9.index += 1
        st.dataframe(df9)
    # que10
    elif questions == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        cursor.execute("SELECT DISTINCT channel.Channel_Name, video.Video_Name, video.Comment_Count FROM channel JOIN playlist ON channel.Channel_Id = playlist.Channel_Id JOIN video ON playlist.Playlist_Id = video.Playlist_Id ORDER BY video.Comment_Count DESC;")
        ans10= cursor.fetchall()
        df10 = pd.DataFrame(ans10,columns=['Channel Name','Video Name', 'Number of comments']).reset_index(drop=True)
        df10.index += 1
        st.dataframe(df10)
    #closing the mysql DB connection
    connecting_for_que.close()