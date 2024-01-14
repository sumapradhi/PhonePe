Data Visualization and Exploration: A User-Friendly Tool Using Streamlit and Plotly

What is PhonePe Pulse?

PhonePe Pulse showcases more than 2000+ Crore transactions by consumers on an interactive map of India.
With over 45% market share, PhonePe's data is representative of the country's digital payment habits. 
The insights on the website and in the report have been drawn from two key sources - the entirety of 
PhonePe's transaction data combined with merchant and customer interviews. The report is available as a free download
 on the PhonePe Pulse website and GitHub.


Libraries/Modules needed for the project:

Plotly (To plot and visualize the data)
Pandas (To create a DataFrame with the scraped data)
mysql.connector (To store and retrieve the data)
Streamlit (To create Graphical User Interface)
json (To load the json files)
git.repo.base (To clone the GitHub repository)
Workflow:

Step 1: Importing the Libraries

The first step is to import all the required libraries/modules into the script. Use the below syntax to
 install the libraries if they are not installed:

!pip install ["Name of the library"]

If the libraries are already installed, use the below code to import themimport pandas as pd
import mysql.connector as sql
import streamlit as st
import plotly.express as px
import os
import json
from streamlit_option_menu import option_menu
from PIL import Image
from git.repo.base import Repo

Step 2: Data Extraction

Clone the PhonePe Pulse GitHub repository using scripting and store the data in a suitable format such as JSON.

from git import Repo
repository_url = "https://github.com/PhonePe/pulse.git"
local_directory_path = "C:/Users/Admin/Desktop/neww"

# Set the path to the Git executable explicitly using the env parameter
git_executable_path = "C:/Program Files/Git/cmd/git.exe"

# Clone the repository to the specified local directory
repo = Repo.clone_from(repository_url, local_directory_path, env={"GIT_EXECUTABLE": git_executable_path})

# Check if the repository was cloned successfully
if repo.bare:
    print("Cloning failed.")
else:
    print("Cloning successful.")

Step 3: Data Transformation

Transform the JSON files into a readable and understandable DataFrame format using a for loop and iterating file by file. Use the os, json, and pandas packages to perform this step, and convert the DataFrame into a CSV file for storing in the local drive.

path1 = "Path of the JSON files"
agg_trans_list = os.listdir(path1)

Define column names
columns1 = {'State': [], 'Year': [], 'Quarter': [], 'Transaction_type': [], 'Transaction_count': [],'Transaction_amount': []}

Loop through each folder and file, and append the required data into a DataFrame
for state in agg_trans_list:
cur_state = path1 + state + "/"
agg_year_list = os.listdir(cur_state)

for year in agg_year_list:
    cur_year = cur_state + year + "/"
    agg_file_list = os.listdir(cur_year)

    for file in agg_file_list:
        cur_file = cur_year + file
        data = open(cur_file, 'r')
        A = json.load(data)

        for i in A['data']['transactionData']:
            name = i['name']
            count = i['paymentInstruments'][0]['count']
            amount = i['paymentInstruments'][0]['amount']
            columns1['Transaction_type'].append(name)
            columns1['Transaction_count'].append(count)
            columns1['Transaction_amount'].append(amount)
            columns1['State'].append(state)
            columns1['Year'].append(year)
            columns1['Quarter'].append(int(file.strip('.json')))
        
df = pd.DataFrame(columns1)

Convert the DataFrame into a CSV file
df.to_csv('filename.csv',index=False)
Step 4:
Database insertion:

To insert the data frame into SQL, first create a new database and tables using "mysql-connector-python" library in Python to connect to a MySQL database and insert the transformed data using SQL commands.

Creating the connection between Python and MySQL:

mydb = sql.connect(host="localhost",
           user="username",
           password="password",
           database= "phonepe_pulse"
          )
mycursor = mydb.cursor(buffered=True)
Creating tables:

mycursor.execute("create table 'Table name' (col1 varchar(100), col2 int, col3 int, col4 varchar(100), col5 int, col6 double)")


for i,row in df.iterrows():

    #here %S means string values 
    sql = "INSERT INTO agg_trans VALUES (%s,%s,%s,%s,%s,%s)"
    mycursor.execute(sql, tuple(row))
    
    # the connection is not auto committed by default, so we must commit to save our changes
    mydb.commit()
Step 5: Dashboard Creation

Once the data is extracted, transformed, and inserted into the database, the next step is to create an interactive 
dashboard for data visualization. I have used Plotly and Streamlit libraries to create an interactive and visually 
appealing dashboard.
The dashboard features at least ten different dropdown options for users to select different facts and figures to display.
The dashboard displays information and insights from the Phonepe Pulse GitHub repository in an interactive and visually app
ealing manner.
Using Streamlit, I have created a user-friendly interface with multiple dropdown options for users to select different 
facts and figures to display. The users can select any of the dropdown options, and the dashboard will update automatically
providing real-time insights.

Step 6: Data Retrieval

To retrieve the data from the MySQL database, I have used the "mysql-connector-python" library in Python. I have connected
 to the MySQL database and fetched the data into a Pandas dataframe. This data is used to update the dashboard dynamically.
In conclusion, this project demonstrates how Python libraries can be used to extract, transform, and visualize data from 
the Phonepe Pulse GitHub repository. The resulting dashboard provides valuable insights and information about the data in 
the Phonepe Pulse GitHub repository, making it a valuable tool for data analysis and decision-making in the fintech domain.
To run this project, the user needs to clone the GitHub repository and install the necessary libraries. .

The project can be run using the command "streamlit run phonepe.py" in the command prompt or terminal. The user can then access 
the dashboard from a web browser and easily navigate the different visualizations and facts and figures displayed.









