import builtins
import streamlit as st
import firebase_admin
from firebase_admin import credentials, db
import json
import pandas as pd
import math
from pandasql import sqldf

st.title("EDFS")
st.text("This is a firebase hosted application. To connect, enter private key and url.")
st.text("Firebase--> project settings --> service accounts--> 'Generate New Private Key'")

# Connect to Database
# Get Private Key and URL from User
url = st.text_input("Paste your firebase URL here:")
accessKey = st.text_input("Paste your Firebase Private Key Here:")
accessKey = "{"+accessKey+"}"
accessKey = json.loads(accessKey)
st.write(type(accessKey))


if accessKey and url:
    if not firebase_admin._apps:
         firebase_admin.initialize_app(credentials.Certificate(accessKey), {
         'databaseURL': url })

    #UI Interface
    st.subheader("Part 1: File System Commands")
    st.subheader("To navigate thru the system, simply run 'ls'")
    st.text("The input can be interpreted as your command line on HDFS.")
    st.text("Input format for ls, mkdir, rm, cat is command + directory.")
    st.text("EXAMPLE: 'mkdir /user/Andrew'")
    st.text("Input format for getPartitions is command + file + path.")
    st.text("EXAMPLE: 'getPartitions CoralData.csv /user/john'")
    st.text("input format for put is command + file + directory + # of desired partitions.")
    st.text("EXAMPLE: 'put mushrooms.csv /user/john 3'")
    st.text("Similarly, input format for readPartition is command + file + directory + desired partition number.")
    st.text("EXAMPLE: 'readPartition mushrooms.csv /user/john 3'")
    # User upload if no data
    data = st.file_uploader('Upload your csv here if there is no data in system')

    #### TASK 1
    # Get Input
    command = st.text_input("Enter Input here")
    # Split input so that we can reference it by index for the HDFS task1 commands
    command_parts = command.split(' ')
    command_prefix = command_parts[0]
    command_suffix = ""

    # handle if there is nothing after first command (i.e. the user entered just "ls")
    if len(command_parts) > 1:
        command_suffix = command_parts[1]
    if len(command_parts) > 2:
        put_partitions = command_parts[2]

    # Firebase considers period an illegal character so need to convert periods to underscore

    # cat
    # Here for cat and rm we need to use command_suffix
    if command_prefix == 'cat':
        # st.write(db.reference(command_suffix.replace('.','_'))).get()
        cat_out = (db.reference(command_suffix.replace('.','_'))).get()
        st.write(cat_out)
    #rm
    if command_prefix == 'rm':
        # First we need to remove the file from the directory
        # This doesn't remove the actual data, just the representation within the directory
        # This is like removing the inumber
        inumber = db.reference(command_suffix.replace('.','_'))
        st.write(inumber)
        # Then We need to remove the actual data from /data
        datafile = command_suffix.split('/')
        #datafile.split('/')
        file = datafile[-1]
        #removal.replace('.', '_')
        #st.write(file)
        delete2 = db.reference('/data/'+file)
        st.write(delete2)
        inumber.delete()
        delete2.delete()
    # ls
    if command_prefix == 'ls':
        ls_output = db.reference(command_suffix)
        # Print out Navigable Interface
        st.json(ls_output.get())
    # mkdir
    if command_prefix == 'mkdir':
        db.reference(command_suffix).set('')
    # getPartitions
    if command_prefix == 'getPartitions':
        st.write((db.reference(command_parts[2] + '/' + command_parts[1].replace('.', '_'))).get())
    #readPartition
    if command_prefix == 'readPartition':
        filepath = command_parts[2] + '/' + command_parts[1].replace('.', '_') + '/p' + str(command_parts[3])
        # get location first to use as argument for .get for contents
        partition_location = (db.reference(filepath)).get()
        # then access the partition within
        partitionfile = db.reference(partition_location)
        # print out content
        st.write(partitionfile.get())
    #put
    if command_prefix == 'put':
        if not data:
            st.write('No File to upload')

        df = pd.read_csv(data)
        file = command_parts[1].replace('.', '_')
        fpath = command_parts[2]
        # Partition refers to partition #
        partition = int(command_parts[3])
        partition = int(partition)
        # st.write(partition)
        # Handle the remainder using .ceil to round up to avoid incomplete partitions
        x = 0
        partition_counter = math.ceil(len(df) / partition)
        last = partition_counter - 1

        for portion in range(1, partition+1):
            # for each partition, create a spot for it in /data to load as well as in the directory
            data_dir = db.reference(fpath+'/'+file+'/p'+str(portion))
            #st.write(dat_location)
            data_dir.set('/data/'+file+'/p'+str(portion))
            # declare location for the data to live to refer to in next loop
            data_location = db.reference('/data/'+file+'/p'+str(portion))

            # run until you do each partition
            if portion < partition:
                # x:last selects the specific partition out of the csv
                csv_section = df.iloc[x:last]
                #Prepare the partition from df to be loaded to location
                # Firebase accepts dictionary to represent json
                put_partition = csv_section.to_dict(orient='records')
                # Put the partition in the p location within /data
                data_location.set(put_partition)
                # move on to next partition
                x += partition_counter - 1
                last += partition_counter - 1
            #last partition
            if portion == partition:
                #Take remaining rows for last one
                csv_section = df.iloc[x:len(df)]
                put_partition = csv_section.to_dict(orient='records')
                # put it
                data_location.set(put_partition)


    #search function
    st.subheader("Part 2: PMR Search Function")
    st.text("To conduct map reduce search functions, you must first specify desired filepath.")
    st.text("Once specified, you may query using SQL.")

    pysqldf = lambda q: sqldf(q, globals())
    get_search_input = st.text_input("Filepath").replace(".", "_")
    if not get_search_input:
        st.write("Please enter the filepath")
    else:
        df_name = get_search_input.split("/")[-1].split("_")[0]
        sql = st.text_input("Sql")
        if not sql:
            st.write("Please write a sql query")
        else:
            all_rows = []
            #go thru each partition and run lines 136-145 if the selected value is there return and combine
            for partition in db.reference(get_search_input).get().values():
                #read partition
                rows = db.reference(partition).get()
                #all_rows.extend(rows)
                #convert into a dataframe
                df = pd.DataFrame.from_records(rows)
                #print(df)
                #print(globals())
                #st.write(globals())
                #st.success()
                # run sql query on that partition
                output = sqldf(sql, {df_name: df})
                #combine outputs for partitions, only if there was output returned
                #print(output)
                if len(output) != 0:
                    all_rows.append(output)
            #st.dataframe(df.head()
            final = pd.concat(all_rows)
            #Run the query on the combined final
            final_final= sqldf(sql, {df_name: final})
            st.dataframe(final_final)
