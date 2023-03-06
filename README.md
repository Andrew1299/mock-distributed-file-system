# mock-distributed-file-system
USC Masters Data Science Project

This is a simulation of a distributed file system using a firebase hosted application through Python with streamlit. It supports standard dfs commands like mkdir, ls, cat, etc. A user can upload a csv file and specify how many partitions to be made, which will then be split into said partitions and stored in multiple locations in the firebase with replicates. Users can locate, manipulate, and retrieve the files using commands like "ls, rm", etc. 
Finally the applicationn supports an analytical interface where a user can specifiy a file they want, and query it using SQL commands. 
