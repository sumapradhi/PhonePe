import os
import json
import pandas as pd
import mysql.connector
import git
from sqlalchemy import create_engine

#aggre_transaction

path1="C:/Users/sumat/Downloads/New folder (3)/PhonePe/pulse/data/aggregated/transaction/country/india/state/"
agg_tran_list= os.listdir(path1)

column1={"States":[],"Years":[],"Quarter":[],"Transaction_type":[],"Transaction_count":[], "Transaction_amount":[]}

# Looping through state folders, years, and files to extract data
for state in agg_tran_list:
    cur_states =path1+state+"/"
    agg_year_list = os.listdir(cur_states)
    
    for year in agg_year_list:
        cur_years = cur_states+year+"/"
        agg_file_list = os.listdir(cur_years)

        for file in agg_file_list:
            cur_files = cur_years+file
            data = open(cur_files,"r")
            A = json.load(data)

            for i in A["data"]["transactionData"]:
                name = i["name"]
                count = i["paymentInstruments"][0]["count"]
                amount = i["paymentInstruments"][0]["amount"]
                column1["Transaction_type"].append(name)
                column1["Transaction_count"].append(count)
                column1["Transaction_amount"].append(amount)
                column1["States"].append(state)
                column1["Years"].append(year)
                column1["Quarter"].append(int(file.strip(".json")))
# Creating a Pandas DataFrame for aggregated transaction data 
aggre_transaction=pd.DataFrame(column1)

# Cleaning and standardizing state names in the DataFrame
aggre_transaction["States"] = aggre_transaction["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
aggre_transaction["States"] = aggre_transaction["States"].str.replace("-"," ")
aggre_transaction["States"] = aggre_transaction["States"].str.title()
aggre_transaction['States'] = aggre_transaction['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")

# Setting the path for aggregated user data
path2="C:/Users/sumat/Downloads/New folder (3)\PhonePe/pulse/data/aggregated/user/country/india/state/"
agg_user_list= os.listdir(path2)

# Creating an initial dictionary for aggregated user data
column2={"States":[],"Years":[],"Quarter":[],"Brands":[],"Transaction_count":[], "Percentage":[]}

# Looping through state folders, years, and files to extract data
for state in agg_user_list:
    cur_states = path2+state+"/"
    agg_year_list = os.listdir(cur_states)
    
    for year in agg_year_list:
        cur_years = cur_states+year+"/"
        agg_file_list = os.listdir(cur_years)
        
        for file in agg_file_list:
            cur_files = cur_years+file
            data = open(cur_files,"r")
            B = json.load(data)

            try:

                for i in B["data"]["usersByDevice"]:
                    brand = i["brand"]
                    count = i["count"]
                    percentage = i["percentage"]
                    column2["Brands"].append(brand)
                    column2["Transaction_count"].append(count)
                    column2["Percentage"].append(percentage)
                    column2["States"].append(state)
                    column2["Years"].append(year)
                    column2["Quarter"].append(int(file.strip(".json")))
            
            except:
                pass

# Creating a Pandas DataFrame for aggregated user data
aggre_user = pd.DataFrame(column2)

# Cleaning and standardizing state names in the DataFrame
aggre_user["States"] = aggre_user["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
aggre_user["States"] = aggre_user["States"].str.replace("-"," ")
aggre_user["States"] = aggre_user["States"].str.title()
aggre_user['States'] = aggre_user['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")

# Setting the path for Map transaction data
path3="C:/Users/sumat/Downloads/New folder (3)/PhonePe/pulse/data/map/transaction/hover/country/india/state/"
map_tran_list=os.listdir(path3)

# Creating an initial dictionary for Map transaction data
column3={"States":[],"Years":[],"Quarter":[],"Districts":[],"Transaction_count":[], "Transaction_amount":[]}
# Looping through state folders, years, and files to extract data
for state in map_tran_list:
    cur_states = path3+state+"/"
    map_year_list = os.listdir(cur_states)
    
    for year in map_year_list:
        cur_years = cur_states+year+"/"
        map_file_list = os.listdir(cur_years)
        
        for file in map_file_list:
            cur_files = cur_years+file
            data = open(cur_files,"r")
            C = json.load(data)

            for i in C['data']["hoverDataList"]:
                name = i["name"]
                count = i["metric"][0]["count"]
                amount = i["metric"][0]["amount"]
                column3["Districts"].append(name)
                column3["Transaction_count"].append(count)
                column3["Transaction_amount"].append(amount)
                column3["States"].append(state)
                column3["Years"].append(year)
                column3["Quarter"].append(int(file.strip(".json")))

# Creating a Pandas DataFrame for Map transaction data
map_transaction=pd.DataFrame(column3)

# Cleaning and standardizing state names in the DataFrame
map_transaction["States"] = map_transaction["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
map_transaction["States"] = map_transaction["States"].str.replace("-"," ")
map_transaction["States"] = map_transaction["States"].str.title()
map_transaction['States'] = map_transaction['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")

# Setting the path for Map user data
path4 = "C:/Users/sumat/Downloads/New folder (3)/PhonePe/pulse/data/map/user/hover/country/india/state/"
map_user_list = os.listdir(path4)

# Creating an initial dictionary for Map user data
column4 = {"States":[], "Years":[], "Quarter":[], "Districts":[], "RegisteredUser":[], "AppOpens":[]}

# Looping through state folders, years, and files to extract data
for state in map_user_list:
    cur_states = path4+state+"/"
    map_year_list = os.listdir(cur_states)
    
    for year in map_year_list:
        cur_years = cur_states+year+"/"
        map_file_list = os.listdir(cur_years)
        
        for file in map_file_list:
            cur_files = cur_years+file
            data = open(cur_files,"r")
            D = json.load(data)

            for i in D["data"]["hoverData"].items():
                districts = i[0]
                registereduser = i[1]["registeredUsers"]
                appopens = i[1]["appOpens"]
                column4["Districts"].append(districts)
                column4["RegisteredUser"].append(registereduser)
                column4["AppOpens"].append(appopens)
                column4["States"].append(state)
                column4["Years"].append(year)
                column4["Quarter"].append(int(file.strip(".json")))

# Creating a Pandas DataFrame for Map user data
map_user = pd.DataFrame(column4)

# Cleaning and standardizing state names in the DataFrame
map_user["States"] = map_user["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
map_user["States"] = map_user["States"].str.replace("-"," ")
map_user["States"] = map_user["States"].str.title()
map_user['States'] = map_user['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")

# Setting the path for Top transaction data
#top_transaction
path5="C:/Users/sumat/Downloads/New folder (3)/PhonePe/pulse/data/top/transaction/country/india/state/"
top_transaction_list=os.listdir(path5)

column5={"States":[],"Years":[],"Quarter":[],"Districts":[],"Transaction_count":[], "Transaction_amount":[]}
for state in top_transaction_list:
    cur_states=path5+state+"/"
    agg_year_list = os.listdir(cur_states)
    
    for year in agg_year_list:
            cur_year=cur_states+year+"/"
            agg_file_list= os.listdir(cur_year)
            
            
            for file in agg_file_list:
                cur_file=cur_year+file
                data=open(cur_file,"r")

                E=json.load(data)
                for i in E["data"]["districts"]:
                        entityName=i["entityName"]
                        count=i["metric"]["count"]
                        amount=i["metric"]["amount"]
                        column5["Districts"].append(entityName)
                        column5["Transaction_count"].append(count)
                        column5["Transaction_amount"].append(amount)
                        column5["States"].append(state)
                        column5["Years"].append(year)
                        column5["Quarter"].append(int(file.strip(".json")))
                        
# Creating a Pandas DataFrame for Top transaction data
top_transaction = pd.DataFrame(column5)

# Cleaning and standardizing state names in the DataFrame
top_transaction["States"] = top_transaction["States"].astype(str).str.replace("andaman-&-nicobar-islands", "Andaman & Nicobar")
top_transaction["States"] = top_transaction["States"].str.replace("-"," ")
top_transaction["States"] = top_transaction["States"].str.title()
top_transaction['States'] = top_transaction['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")

# Setting the path for Top user data
#top_user
path6="C:/Users/sumat/Downloads/New folder (3)/PhonePe/pulse/data/top/user/country/india/state/"
top_user_list=os.listdir(path6)

column6={"States":[],"Years":[],"Quarter":[],"Districts":[],"RegisteredUsers":[]}
for state in top_user_list:
    cur_states=path6+state+"/"
    agg_year_list = os.listdir(cur_states)
    
    for year in agg_year_list:
        cur_year=cur_states+year+"/"
        agg_file_list= os.listdir(cur_year)
        
        for file in agg_file_list:
            cur_file=cur_year+file
            data=open(cur_file,"r")

            F=json.load(data)
            for i in F["data"]["districts"]:
                district=i["name"]
                registeredUsers=i["registeredUsers"]
                column6["Districts"].append(district)
                column6["RegisteredUsers"].append(registeredUsers)
                column6["States"].append(state)
                column6["Years"].append(year)
                column6["Quarter"].append(int(file.strip(".json")))

# Creating a Pandas DataFrame for Top transaction data
top_user=pd.DataFrame(column6)           


aggre_transaction.to_csv('C:/Users/sumat/Downloads/New folder (3)/PhonePe/pulse/data/agg_transaction.csv', index=False)
aggre_user.to_csv('C:/Users/sumat/Downloads/New folder (3)/PhonePe/pulse/data/agg_user.csv', index=False)
map_transaction.to_csv('C:/Users/sumat/Downloads/New folder (3)/PhonePe/pulse/data/map_transaction.csv', index=False)
map_user.to_csv('C:/Users/sumat/Downloads/New folder (3)/PhonePe/pulse/data/map_user.csv', index=False)
top_transaction.to_csv('C:/Users/sumat/Downloads/New folder (3)/PhonePe/pulse/data/top_transaction.csv', index=False)
top_user.to_csv('C:/Users/sumat/Downloads/New folder (3)/PhonePe/pulse/data/top_user.csv', index=False)

import mysql.connector
conn_config = {
        'host': 'localhost',
        'user': 'root',
        'password': '12345678',
        'database': 'PhonePe_Pulse',
        'port': 3306,
    }

# Establish a connection
connection = mysql.connector.connect(**conn_config)

# Create a cursor
cursor = connection.cursor()

#Creating a aggregated transaction table and inserting data
create_query1 = '''CREATE TABLE if not exists aggregated_transaction (States varchar(50),
                                                                      Years int,
                                                                      Quarter int,
                                                                      Transaction_type varchar(50),
                                                                      Transaction_count bigint,
                                                                      Transaction_amount bigint
                                                                      )'''
cursor.execute(create_query1)
connection.commit()

for index,row in aggre_transaction.iterrows():
    insert_query1 = '''INSERT INTO aggregated_transaction (States, Years, Quarter, Transaction_type, Transaction_count, Transaction_amount)
                                                        values(%s,%s,%s,%s,%s,%s)'''
    values = (row["States"],
              row["Years"],
              row["Quarter"],
              row["Transaction_type"],
              row["Transaction_count"],
              row["Transaction_amount"]
              )
cursor.execute(insert_query1,values)
connection.commit()
# Close the cursor and the connection
cursor.close()
connection.close()

import mysql.connector
conn_config = {
        'host': 'localhost',
        'user': 'root',
        'password': '12345678',
        'database': 'PhonePe_Pulse',
        'port': 3306,
    }

# Establish a connection
connection = mysql.connector.connect(**conn_config)

# Create a cursor
cursor = connection.cursor()
#creating a aggregated user table and inserting data
create_query2 = '''CREATE TABLE if not exists aggregated_user (States varchar(50),
                                                                Years int,
                                                                Quarter int,
                                                                Brands varchar(50),
                                                                Transaction_count bigint,
                                                                Percentage float)'''
cursor.execute(create_query2)
connection.commit()

for index,row in aggre_user.iterrows():
    insert_query2 = '''INSERT INTO aggregated_user (States, Years, Quarter, Brands, Transaction_count, Percentage)
                                                    values(%s,%s,%s,%s,%s,%s)'''
    values = (row["States"],
              row["Years"],
              row["Quarter"],
              row["Brands"],
              row["Transaction_count"],
              row["Percentage"])
cursor.execute(insert_query2,values)
connection.commit()

# Close the cursor and the connection
cursor.close()
connection.close()

import mysql.connector
conn_config = {
        'host': 'localhost',
        'user': 'root',
        'password': '12345678',
        'database': 'PhonePe_Pulse',
        'port': 3306,
    }

# Establish a connection
connection = mysql.connector.connect(**conn_config)

# Create a cursor
cursor = connection.cursor()
#Creating a map_transaction_table and inserting the data
create_query3 = '''CREATE TABLE if not exists map_transaction (States varchar(50),
                                                                Years int,
                                                                Quarter int,
                                                                Districts varchar(50),
                                                                Transaction_count bigint,
                                                                Transaction_amount float)'''
cursor.execute(create_query3)
connection.commit()

for index,row in map_transaction.iterrows():
            insert_query3 = '''
                INSERT INTO map_Transaction (States, Years, Quarter, Districts, Transaction_count, Transaction_amount)
                VALUES (%s, %s, %s, %s, %s, %s)

            '''
            values = (
                row['States'],
                row['Years'],
                row['Quarter'],
                row['Districts'],
                row['Transaction_count'],
                row['Transaction_amount']
            )
            cursor.execute(insert_query3,values)
            connection.commit()

import mysql.connector
conn_config = {
        'host': 'localhost',
        'user': 'root',
        'password': '12345678',
        'database': 'PhonePe_Pulse',
        'port': 3306,
    }

# Establish a connection
connection = mysql.connector.connect(**conn_config)

# Create a cursor
cursor = connection.cursor()

#creating a map_user_table and inserting data
create_query4 = '''CREATE TABLE if not exists map_user (States varchar(50),
                                                        Years int,
                                                        Quarter int,
                                                        Districts varchar(50),
                                                        RegisteredUser bigint,
                                                        AppOpens bigint)'''
cursor.execute(create_query4)
connection.commit()

for index,row in map_user.iterrows():
    insert_query4 = '''INSERT INTO map_user (States, Years, Quarter, Districts, RegisteredUser, AppOpens)
                        values(%s,%s,%s,%s,%s,%s)'''
    values = (row["States"],
              row["Years"],
              row["Quarter"],
              row["Districts"],
              row["RegisteredUser"],
              row["AppOpens"])
cursor.execute(insert_query4,values)
connection.commit()

# Close the cursor and the connection
cursor.close()
connection.close()

import mysql.connector
conn_config = {
        'host': 'localhost',
        'user': 'root',
        'password': '12345678',
        'database': 'PhonePe_Pulse',
        'port': 3306,
    }

# Establish a connection
connection = mysql.connector.connect(**conn_config)

# Create a cursor
cursor = connection.cursor()
#creating a top_transaction_table and inserting data
create_query5 = '''CREATE TABLE if not exists top_transaction (States varchar(50),
                                                                Years int,
                                                                Quarter int,
                                                                Districts varchar(50),
                                                                Transaction_count bigint,
                                                                Transaction_amount bigint)'''
cursor.execute(create_query5)
connection.commit()

# Alter the data type of the 'Districts' column to VARCHAR(50)
alter_query = "ALTER TABLE top_transaction MODIFY COLUMN Districts VARCHAR(50)"
cursor.execute(alter_query)
connection.commit()

for index,row in top_transaction.iterrows():
    insert_query5 = '''INSERT INTO top_transaction (States, Years, Quarter, Districts, Transaction_count, Transaction_amount)
                                                    values(%s,%s,%s,%s,%s,%s)'''
    values = (row["States"],
              row["Years"],
              row["Quarter"],
              row["Districts"],
              row["Transaction_count"],
              row["Transaction_amount"])
cursor.execute(insert_query5,values)
connection.commit()

# Close the cursor and the connection
cursor.close()
connection.close()

import mysql.connector
conn_config = {
        'host': 'localhost',
        'user': 'root',
        'password': '12345678',
        'database': 'PhonePe_Pulse',
        'port': 3306,
    }

# Establish a connection
connection = mysql.connector.connect(**conn_config)

# Create a cursor
cursor = connection.cursor()

#creating a top_user_table and inserting data
create_query6 = '''CREATE TABLE if not exists top_user (States varchar(50),
                                                        Years int,
                                                        Quarter int,
                                                        Districts varchar(50),
                                                        RegisteredUsers bigint
                                                        )'''
cursor.execute(create_query6)
connection.commit()

for index,row in top_user.iterrows():
    insert_query6 = '''INSERT INTO top_user (States, Years, Quarter, Districts, RegisteredUsers)
                        values(%s,%s,%s,%s,%s)'''
    values = (row["States"],
              row["Years"],
              row["Quarter"],
              row["Districts"],
              row["RegisteredUsers"]
              )
    cursor.execute(insert_query6,values)
    
connection.commit()
cursor.close()
connection.close()   


import json
import streamlit as st
from PIL import Image
import pandas as pd
import requests
import mysql.connector
import plotly.express as px
import plotly.graph_objects as go

#CREATE DATAFRAMES FROM SQL
#sql connection

conn_config = {
        'host': 'localhost',
        'user': 'root',
        'password': '12345678',
        'database': 'PhonePe_Pulse',
        'port': 3306,
    }

# Establish a connection
connection = mysql.connector.connect(**conn_config)

# Create a cursor
cursor = connection.cursor()

#Aggregated_transaction
cursor.execute("select * from aggregated_transaction")
table1 = cursor.fetchall()
Aggre_trans = pd.DataFrame(table1,columns = ("States", "Years", "Quarter", "Transaction_type", "Transaction_count", "Transaction_amount"))
connection.commit()

#Aggregated_user
cursor.execute("select * from aggregated_user")
table2 = cursor.fetchall()
connection.commit()
Aggre_user = pd.DataFrame(table2,columns = ("States", "Years", "Quarter", "Brands", "Transaction_count", "Percentage"))

#Map_transaction
cursor.execute("select * from map_transaction")
table3 = cursor.fetchall()
connection.commit()
Map_trans = pd.DataFrame(table3,columns = ("States", "Years", "Quarter", "Districts", "Transaction_count", "Transaction_amount"))

#Map_user
cursor.execute("select * from map_user")
table4 = cursor.fetchall()
connection.commit()
Map_user = pd.DataFrame(table4,columns = ("States", "Years", "Quarter", "Districts", "RegisteredUser", "AppOpens"))

#Top_transaction
cursor.execute("select * from top_transaction")
table5 = cursor.fetchall()
connection.commit()
Top_trans = pd.DataFrame(table5,columns = ("States", "Years", "Quarter", "Districts", "Transaction_count", "Transaction_amount"))

#top_user
cursor.execute("select * from top_user")
table6 = cursor.fetchall()
connection.commit()
top_user = pd.DataFrame(table6,columns = ("States", "Years", "Quarter", "Districts", "RegisteredUser"))

# Close the cursor and the connection
cursor.close()
connection.close()

import requests
# [pandas, numpy and file handling libraries]
import pandas as pd
import numpy as np
import json

import mysql.connector
# [Dash board libraries]
import streamlit as st
import plotly.express as px
from streamlit_option_menu import option_menu
import PIL
from PIL import Image

# CONNECT TO SQL

conn_config = {
        'host': 'localhost',
        'user': 'root',
        'password': '12345678',
        'database': 'PhonePe_Pulse',
        'port': 3306,
    }

# Establish a connection
connection = mysql.connector.connect(**conn_config)

# Create a cursor
cursor = connection.cursor()

# Configuring Streamlit GUI

st.set_page_config(layout="wide")

image=Image.open("Capture.PNG")
st.image(image, width=250)
st.markdown('<h1 style="color: blueviolet;text-align: center;">PhonePe Data Visualization And Exploration</h1>', unsafe_allow_html=True)

with st.sidebar:
    selected = option_menu(None,
                       options = ["Home","About","Reports","Explore DATA",],
                       icons = ["rocket", "info-circle", "clipboard", "binoculars"],
                       default_index=0,
                       orientation="vertical",
                       styles={"container": {"width": "100%"},
                               "icon": {"color": "white", "font-size": "24px"},
                               "nav-link": {"font-size": "24px", "text-align": "left", "margin": "-2px"},
                               "nav-link-selected": {"background-color": "#6F36AD"}})



# HOME TAB
if selected == "Home":
    col1,col2 = st.columns(2)
    with col1:
        st.image(Image.open("phonepe2.PNG"), width=300)
    with col2:
        st.title(':violet[PHONEPE PULSE DATA VISUALISATION]')
        st.subheader(':violet[Phonepe Pulse]:')
        st.write('PhonePe Pulse is your window to the world of how India transacts with interesting trends, deep insights and in-depth analysis based on our data put together by the PhonePe team.')
        st.subheader(':violet[Phonepe Pulse Data Visualisation using Streamlit and Plotly]:')
        st.write('Data visualisation brings data to life, making you the master storyteller of the insights hidden within your numbers.'
                 'Through data dashboards, interactive reports, charts, graphs, and other visual representations, data visualisation helps users develop powerful business insight quickly and effectively.')
        st.markdown("## :violet[Done by] : SUMATHY S")
        st.markdown("[Inspired from](https://www.phonepe.com/pulse/)")
                
    st.write("---")

# ABOUT TAB
if selected == "About":
    col1, col2, = st.columns(2)
    with col1:
        st.subheader("**SIMPLE FAST AND SECURE**")
        st.write("**One App For All Things Money.**")
        st.write("**Pay bills, recharge, send money, buy gold, invest and shop at your favourite stores.**")
        st.write("____________________________________________________")
        st.subheader("**Pay whenever you like, wherever you like.**")
        st.write("**Choose from options like UPI, the PhonePe wallet or your Debit and Credit Card.**")
        st.write("____________________________________________________")
        st.subheader("**Find all your favourite apps on PhonePe Switch.**")
        st.write("**Book flights, order food or buy groceries. Use all your favourite apps without downloading them.**")            
        st.markdown("[DOWNLOAD APP](https://www.phonepe.com/app-download/)")

    with col2:
        st.video("https://youtu.be/c_1H6vivsiA")


# REPORTS TAB
    
if selected == "Reports":
    st.title(':violet[REPORTS]')
    st.subheader('Analysis done on the basis of All India ,States and Top categories between 2018 and 2022')
    select = option_menu(None,
                         options=["INDIA", "STATE"],
                         default_index=0,
                         orientation="horizontal",
                         styles={"container": {"width": "100%"},
                                   "nav-link": {"font-size": "20px", "text-align": "center", "margin": "-2px"},
                                   "nav-link-selected": {"background-color": "#6F36AD"}})
    if select == "INDIA":
        tab1, tab2 = st.tabs(["TRANSACTION","USER"])

        # TRANSACTION TAB
        with tab1:
            col1, col2, col3 = st.columns(3)
            with col1:
                in_tr_yr = st.selectbox('**Select Year**', ('2018', '2019', '2020', '2021', '2022'), key='in_tr_yr')
            with col2:
                in_tr_qtr = st.selectbox('**Select Quarter**', ('1', '2', '3', '4'), key='in_tr_qtr')
            with col3:
                in_tr_tr_typ = st.selectbox('**Select Transaction type**',
                                            ('Recharge & bill payments', 'Peer-to-peer payments',
                                             'Merchant payments', 'Financial Services', 'Others'), key='in_tr_tr_typ')
            # SQL Query

            # Transaction Analysis bar chart query
            cursor.execute(
                f"SELECT States, Transaction_amount FROM aggregated_transaction WHERE Years = '{in_tr_yr}' AND Quarter = '{in_tr_qtr}' AND Transaction_type = '{in_tr_tr_typ}';")
            in_tr_tab_qry_rslt = cursor.fetchall()
            df_in_tr_tab_qry_rslt = pd.DataFrame(np.array(in_tr_tab_qry_rslt), columns=['States', 'Transaction_amount'])
            df_in_tr_tab_qry_rslt1 = df_in_tr_tab_qry_rslt.set_index(pd.Index(range(1, len(df_in_tr_tab_qry_rslt) + 1)))

            # Transaction Analysis table query
            cursor.execute(
                f"SELECT States, Transaction_count, Transaction_amount FROM aggregated_transaction WHERE Years = '{in_tr_yr}' AND Quarter = '{in_tr_qtr}' AND Transaction_type = '{in_tr_tr_typ}';")
            in_tr_anly_tab_qry_rslt = cursor.fetchall()
            df_in_tr_anly_tab_qry_rslt = pd.DataFrame(np.array(in_tr_anly_tab_qry_rslt),
                                                      columns=['States', 'Transaction_count', 'Transaction_amount'])
            df_in_tr_anly_tab_qry_rslt1 = df_in_tr_anly_tab_qry_rslt.set_index(
                pd.Index(range(1, len(df_in_tr_anly_tab_qry_rslt) + 1)))

            # Total Transaction Amount table query
            cursor.execute(
                f"SELECT SUM(Transaction_amount), AVG(Transaction_amount) FROM aggregated_transaction WHERE Years = '{in_tr_yr}' AND Quarter = '{in_tr_qtr}' AND Transaction_type = '{in_tr_tr_typ}';")
            in_tr_am_qry_rslt = cursor.fetchall()
            df_in_tr_am_qry_rslt = pd.DataFrame(np.array(in_tr_am_qry_rslt), columns=['Total', 'Average'])
            df_in_tr_am_qry_rslt1 = df_in_tr_am_qry_rslt.set_index(['Average'])

            # Total Transaction Count table query
            cursor.execute(
                f"SELECT SUM(Transaction_count), AVG(Transaction_count) FROM aggregated_transaction WHERE Years = '{in_tr_yr}' AND Quarter = '{in_tr_qtr}' AND Transaction_type = '{in_tr_tr_typ}';")
            in_tr_co_qry_rslt = cursor.fetchall()
            df_in_tr_co_qry_rslt = pd.DataFrame(np.array(in_tr_co_qry_rslt), columns=['Total', 'Average'])
            df_in_tr_co_qry_rslt1 = df_in_tr_co_qry_rslt.set_index(['Average'])

            # GEO VISUALISATION
            # Drop a State column from df_in_tr_tab_qry_rslt
            df_in_tr_tab_qry_rslt.drop(columns=['States'], inplace=True)
            # Clone the gio data
            url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
            response = requests.get(url)
            data1 = json.loads(response.content)
            # Extract state names and sort them in alphabetical order
            state_names_tra = [feature['properties']['ST_NM'] for feature in data1['features']]
            state_names_tra.sort()
            # Create a DataFrame with the state names column
            df_state_names_tra = pd.DataFrame({'States': state_names_tra})
            # Combine the Gio State name with df_in_tr_tab_qry_rslt
            df_state_names_tra['Transaction_amount'] = df_in_tr_tab_qry_rslt
            # convert dataframe to csv file
            df_state_names_tra.to_csv('State_trans.csv', index=False)
            # Read csv
            df_tra = pd.read_csv('State_trans.csv')
            # Geo plot
            fig_tra = px.choropleth(
                df_tra,
                geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                featureidkey='properties.ST_NM', locations='States', color='Transaction_amount',
                color_continuous_scale='thermal', title='Transaction Analysis')
            fig_tra.update_geos(fitbounds="locations", visible=False)
            fig_tra.update_layout(title_font=dict(size=33), title_font_color='#AD71EF', height=800)
            st.plotly_chart(fig_tra, use_container_width=True)

            # ---------   /   All India Transaction Analysis Bar chart  /  ----- #
            df_in_tr_tab_qry_rslt1['States'] = df_in_tr_tab_qry_rslt1['States'].astype(str)
            df_in_tr_tab_qry_rslt1['Transaction_amount'] = df_in_tr_tab_qry_rslt1['Transaction_amount'].astype(float)
            df_in_tr_tab_qry_rslt1_fig = px.bar(df_in_tr_tab_qry_rslt1, x='States', y='Transaction_amount',
                                                color='Transaction_amount', color_continuous_scale='thermal',
                                                title='Transaction Analysis Chart', height=700, )
            df_in_tr_tab_qry_rslt1_fig.update_layout(title_font=dict(size=33), title_font_color='#AD71EF')
            st.plotly_chart(df_in_tr_tab_qry_rslt1_fig, use_container_width=True)

            # -------  /  All India Total Transaction calculation Table   /   ----  #
            st.header(':violet[Total calculation]')

            col4, col5 = st.columns(2)
            with col4:
                st.subheader(':violet[Transaction Analysis]')
                st.dataframe(df_in_tr_anly_tab_qry_rslt1)
            with col5:
                st.subheader(':violet[Transaction Amount]')
                st.dataframe(df_in_tr_am_qry_rslt1)
                st.subheader(':violet[Transaction Count]')
                st.dataframe(df_in_tr_co_qry_rslt1)

        # USER TAB
        with tab2:
            col1, col2 = st.columns(2)
            with col1:
                in_us_yr = st.selectbox('**Select Years**', ('2018', '2019', '2020', '2021', '2022'), key='in_us_yr')
            with col2:
                in_us_qtr = st.selectbox('**Select Quarter**', ('1', '2', '3', '4'), key='in_us_qtr')

            # SQL Query

            # User Analysis Bar chart query
            cursor.execute(f"SELECT States, SUM(Transaction_count) FROM aggregated_user WHERE Years = '{in_us_yr}' AND Quarter = '{in_us_qtr}' GROUP BY States;")
            in_us_tab_qry_rslt = cursor.fetchall()
            df_in_us_tab_qry_rslt = pd.DataFrame(np.array(in_us_tab_qry_rslt), columns=['States', 'Transaction_count'])
            df_in_us_tab_qry_rslt1 = df_in_us_tab_qry_rslt.set_index(pd.Index(range(1, len(df_in_us_tab_qry_rslt) + 1)))

            # Total User Count table query
            cursor.execute(f"SELECT SUM(Transaction_count), AVG(Transaction_count) FROM aggregated_user WHERE Years = '{in_us_yr}' AND Quarter = '{in_us_qtr}';")
            in_us_co_qry_rslt = cursor.fetchall()
            df_in_us_co_qry_rslt = pd.DataFrame(np.array(in_us_co_qry_rslt), columns=['Total', 'Average'])
            df_in_us_co_qry_rslt1 = df_in_us_co_qry_rslt.set_index(['Average'])

            # GEO VISUALIZATION FOR USER

            # Drop a State column from df_in_us_tab_qry_rslt
            df_in_us_tab_qry_rslt.drop(columns=['States'], inplace=True)
            # Clone the gio data
            url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
            response = requests.get(url)
            data2 = json.loads(response.content)
            # Extract state names and sort them in alphabetical order
            state_names_use = [feature['properties']['ST_NM'] for feature in data2['features']]
            state_names_use.sort()
            # Create a DataFrame with the state names column
            df_state_names_use = pd.DataFrame({'States': state_names_use})
            # Combine the Gio State name with df_in_tr_tab_qry_rslt
            df_state_names_use['User_count'] = df_in_us_tab_qry_rslt
            # convert dataframe to csv file
            df_state_names_use.to_csv('State_user.csv', index=False)
            # Read csv
            df_use = pd.read_csv('State_user.csv')
            # Geo plot
            fig_use = px.choropleth(
                df_use,
                geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                featureidkey='properties.ST_NM', locations='States', color='User_count',
                color_continuous_scale='thermal', title='User Analysis')
            fig_use.update_geos(fitbounds="locations", visible=False)
            fig_use.update_layout(title_font=dict(size=33), title_font_color='#AD71EF', height=800)
            st.plotly_chart(fig_use, use_container_width=True)

            # ----   /   All India User Analysis Bar chart   /     -------- #
            df_in_us_tab_qry_rslt1['States'] = df_in_us_tab_qry_rslt1['States'].astype(str)
            df_in_us_tab_qry_rslt1['User_count'] = df_in_us_tab_qry_rslt1['Transaction_count'].astype(int)
            df_in_us_tab_qry_rslt1_fig = px.bar(df_in_us_tab_qry_rslt1, x='States', y='User_count', color='User_count',
                                                color_continuous_scale='thermal', title='User Analysis Chart',
                                                height=700, )
            df_in_us_tab_qry_rslt1_fig.update_layout(title_font=dict(size=33), title_font_color='#AD71EF')
            st.plotly_chart(df_in_us_tab_qry_rslt1_fig, use_container_width=True)

            # -----   /   All India Total User calculation Table   /   ----- #
            st.header(':violet[Total calculation]')

            col3, col4 = st.columns(2)
            with col3:
                st.subheader(':violet[User Analysis]')
                st.dataframe(df_in_us_tab_qry_rslt1)
            with col4:
                st.subheader(':violet[User_count]')
                st.dataframe(df_in_us_co_qry_rslt1)

   # STATE TAB
    if select == "STATE":
        tab3 ,tab4 = st.tabs(["TRANSACTION","USER"])

        #TRANSACTION TAB FOR STATE
        with tab3:
            col1, col2, col3 = st.columns(3)
            with col1:
                st_tr_st = st.selectbox('**Select States**', (
                'Andaman & Nicobar', 'Andhra Pradesh', 'Arunachal Pradesh', 'Assam', 'Bihar',
                'Chandigarh', 'Chhattisgarh', 'Dadra and Nagar Haveli and Daman and Diu', 'Delhi', 'Goa', 'Gujarat',
                'Haryana', 'Himachal Pradesh',
                'Jammu & Kashmir', 'Jharkhand', 'Karnataka', 'Kerala', 'Ladakh', 'Lakshadweep', 'Madhya Pradesh',
                'Maharashtra', 'Manipur',
                'Meghalaya', 'Mizoram', 'Nagaland', 'Odisha', 'Puducherry', 'Punjab', 'Rajasthan', 'Sikkim',
                'Tamil nadu', 'Telangana',
                'Tripura', 'Uttar Pradesh', 'Uttarakhand', 'West Bengal'), key='st_tr_st')
            with col2:
                st_tr_yr = st.selectbox('**Select Years**', ('2018', '2019', '2020', '2021', '2022'), key='st_tr_yr')
            with col3:
                st_tr_qtr = st.selectbox('**Select Quarters**', ('1', '2', '3', '4'), key='st_tr_qtr')


            # SQL QUERY

            #Transaction Analysis bar chart query
            
            query = (f"SELECT Transaction_type, Transaction_amount FROM aggregated_transaction WHERE States = '{st_tr_st}' AND Years = '{st_tr_yr}' AND Quarter = '{st_tr_qtr}';")
            cursor.execute(query)
            st_tr_tab_bar_qry_rslt = cursor.fetchall()
            df_st_tr_tab_bar_qry_rslt = pd.DataFrame(np.array(st_tr_tab_bar_qry_rslt),
                                                     columns=['Transaction_type', 'Transaction_amount'])
            df_st_tr_tab_bar_qry_rslt1 = df_st_tr_tab_bar_qry_rslt.set_index(
                pd.Index(range(1, len(df_st_tr_tab_bar_qry_rslt) + 1)))

            # Transaction Analysis table query
            cursor.execute(f"SELECT Transaction_type, Transaction_count, Transaction_amount FROM aggregated_transaction WHERE States = '{st_tr_st}' AND Years = '{st_tr_yr}' AND Quarter = '{st_tr_qtr}';")
            st_tr_anly_tab_qry_rslt = cursor.fetchall()
            df_st_tr_anly_tab_qry_rslt = pd.DataFrame(np.array(st_tr_anly_tab_qry_rslt),
                                                      columns=['Transaction_type', 'Transaction_count',
                                                               'Transaction_amount'])
            df_st_tr_anly_tab_qry_rslt1 = df_st_tr_anly_tab_qry_rslt.set_index(
                pd.Index(range(1, len(df_st_tr_anly_tab_qry_rslt) + 1)))

            # Total Transaction Amount table query
            cursor.execute(f"SELECT SUM(Transaction_amount), AVG(Transaction_amount) FROM aggregated_transaction WHERE States = '{st_tr_st}' AND Years = '{st_tr_yr}' AND Quarter = '{st_tr_qtr}';")
            st_tr_am_qry_rslt = cursor.fetchall()
            df_st_tr_am_qry_rslt = pd.DataFrame(np.array(st_tr_am_qry_rslt), columns=['Total', 'Average'])
            df_st_tr_am_qry_rslt1 = df_st_tr_am_qry_rslt.set_index(['Average'])

            # Total Transaction Count table query
            cursor.execute(f"SELECT SUM(Transaction_count), AVG(Transaction_count) FROM aggregated_transaction WHERE States = '{st_tr_st}' AND Years ='{st_tr_yr}' AND Quarter = '{st_tr_qtr}';")
            st_tr_co_qry_rslt = cursor.fetchall()
            df_st_tr_co_qry_rslt = pd.DataFrame(np.array(st_tr_co_qry_rslt), columns=['Total', 'Average'])
            df_st_tr_co_qry_rslt1 = df_st_tr_co_qry_rslt.set_index(['Average'])



            # -----    /   State wise Transaction Analysis bar chart   /   ------ #

            df_st_tr_tab_bar_qry_rslt1['Transaction_type'] = df_st_tr_tab_bar_qry_rslt1['Transaction_type'].astype(str)
            df_st_tr_tab_bar_qry_rslt1['Transaction_amount'] = df_st_tr_tab_bar_qry_rslt1['Transaction_amount'].astype(
                float)
            df_st_tr_tab_bar_qry_rslt1_fig = px.bar(df_st_tr_tab_bar_qry_rslt1, x='Transaction_type',
                                                    y='Transaction_amount', color='Transaction_amount',
                                                    color_continuous_scale='thermal',
                                                    title='Transaction Analysis Chart', height=500, )
            df_st_tr_tab_bar_qry_rslt1_fig.update_layout(title_font=dict(size=33), title_font_color='#AD71EF')
            st.plotly_chart(df_st_tr_tab_bar_qry_rslt1_fig, use_container_width=True)

            # ------  /  State wise Total Transaction calculation Table  /  ---- #
            st.header(':violet[Total calculation]')

            col4, col5 = st.columns(2)
            with col4:
                st.subheader(':violet[Transaction Analysis]')
                st.dataframe(df_st_tr_anly_tab_qry_rslt1)
            with col5:
                st.subheader(':violet[Transaction Amount]')
                st.dataframe(df_st_tr_am_qry_rslt1)
                st.subheader(':violet[Transaction Count]')
                st.dataframe(df_st_tr_co_qry_rslt1)


        # USER TAB FOR STATE
        with tab4:
            col5, col6 = st.columns(2)
            with col5:
                st_us_st = st.selectbox('**Select States**', (
                'Andaman & Nicobar', 'Andhra Pradesh', 'Arunachal Pradesh', 'Assam', 'Bihar',
                'Chandigarh', 'Chhattisgarh', 'Dadra and Nagar Haveli and Daman and Diu', 'Delhi', 'Goa', 'Gujarat',
                'Haryana', 'Himachal Pradesh',
                'Jammu & Kashmir', 'Jharkhand', 'Karnataka', 'Kerala', 'Ladakh', 'Lakshadweep', 'Madhya Pradesh',
                'Maharashtra', 'Manipur',
                'Meghalaya', 'Mizoram', 'Nagaland', 'Odisha', 'Puducherry', 'Punjab', 'Rajasthan', 'Sikkim',
                'Tamil nadu', 'Telangana',
                'Tripura', 'Uttar Pradesh', 'Uttarakhand', 'West Bengal'), key='st_us_st')
            with col6:
                st_us_yr = st.selectbox('**Select Years**', ('2018', '2019', '2020', '2021', '2022'), key='st_us_yr')
            # SQL QUERY

            # User Analysis Bar chart query
            cursor.execute(f"SELECT Quarter, SUM(Transaction_count) FROM aggregated_user WHERE States = '{st_us_st}' AND Years = '{st_us_yr}' GROUP BY Quarter;")
            st_us_tab_qry_rslt = cursor.fetchall()
            df_st_us_tab_qry_rslt = pd.DataFrame(np.array(st_us_tab_qry_rslt), columns=['Quarter', 'User Count'])
            df_st_us_tab_qry_rslt1 = df_st_us_tab_qry_rslt.set_index(pd.Index(range(1, len(df_st_us_tab_qry_rslt) + 1)))

            # Total User Count table query
            cursor.execute(f"SELECT SUM(Transaction_count), AVG(Transaction_count) FROM aggregated_user WHERE States = '{st_us_st}' AND Years = '{st_us_yr}';")
            st_us_co_qry_rslt = cursor.fetchall()
            df_st_us_co_qry_rslt = pd.DataFrame(np.array(st_us_co_qry_rslt), columns=['Total', 'Average'])
            df_st_us_co_qry_rslt1 = df_st_us_co_qry_rslt.set_index(['Average'])
            
            # -----   /   All India User Analysis Bar chart   /   ----- #
            df_st_us_tab_qry_rslt1['Quarter'] = df_st_us_tab_qry_rslt1['Quarter'].astype(int)
            df_st_us_tab_qry_rslt1['User Count'] = df_st_us_tab_qry_rslt1['User Count'].astype(int)
            df_st_us_tab_qry_rslt1_fig = px.bar(df_st_us_tab_qry_rslt1, x='Quarter', y='User Count', color='User Count',
                                                color_continuous_scale='thermal', title='User Analysis Chart',
                                                height=500, )
            df_st_us_tab_qry_rslt1_fig.update_layout(title_font=dict(size=33), title_font_color='#AD71EF')
            st.plotly_chart(df_st_us_tab_qry_rslt1_fig, use_container_width=True)

            # ------    /   State wise User Total User calculation Table   /   -----#
            st.header(':violet[Total calculation]')

            col3, col4 = st.columns(2)
            with col3:
                st.subheader(':violet[User Analysis]')
                st.dataframe(df_st_us_tab_qry_rslt1)
            with col4:
                st.subheader(':violet[User Count]')
                st.dataframe(df_st_us_co_qry_rslt1)

    
#EXPLORE DATA TAB
if selected == "Explore DATA":
    select = option_menu(None,
                         options=["TOP CATEGORIES", "BASIC INSIGHTS"],
                         default_index=0,
                         orientation="horizontal",
                         styles={"container": {"width": "100%"},
                                   "nav-link": {"font-size": "20px", "text-align": "center", "margin": "-2px"},
                                   "nav-link-selected": {"background-color": "#6F36AD"}})
    # TOP CATEGORIES
    if select == "TOP CATEGORIES":
        st.title(':violet[TOP CATEGORIES]')
        tab5, tab6 = st.tabs(["TRANSACTION", "USER"])

        # Overall top transaction
        #TRANSACTION TAB
        with tab5:
            top_tr_yr = st.selectbox('**Select Years**', ('2018', '2019', '2020', '2021', '2022'), key='top_tr_yr')

            #SQL QUERY

            #Top Transaction Analysis bar chart query
            cursor.execute(
                f"SELECT States, SUM(Transaction_amount) As Transaction_amount FROM top_transaction WHERE Years = '{top_tr_yr}' GROUP BY States ORDER BY Transaction_amount DESC LIMIT 10;")
            top_tr_tab_qry_rslt = cursor.fetchall()
            df_top_tr_tab_qry_rslt = pd.DataFrame(np.array(top_tr_tab_qry_rslt),
                                                  columns=['States', 'Top Transaction amount'])
            df_top_tr_tab_qry_rslt1 = df_top_tr_tab_qry_rslt.set_index(
                pd.Index(range(1, len(df_top_tr_tab_qry_rslt) + 1)))

            # Top Transaction Analysis table query
            cursor.execute(
                f"SELECT States, SUM(Transaction_amount) as Transaction_amount, SUM(Transaction_count) as Transaction_count FROM top_transaction WHERE Years = '{top_tr_yr}' GROUP BY States ORDER BY Transaction_amount DESC LIMIT 10;")
            top_tr_anly_tab_qry_rslt = cursor.fetchall()
            df_top_tr_anly_tab_qry_rslt = pd.DataFrame(np.array(top_tr_anly_tab_qry_rslt),
                                                       columns=['States', 'Top Transaction amount',
                                                                'Total Transaction count'])
            df_top_tr_anly_tab_qry_rslt1 = df_top_tr_anly_tab_qry_rslt.set_index(
                pd.Index(range(1, len(df_top_tr_anly_tab_qry_rslt) + 1)))



            # All India Transaction Analysis Bar chart
            df_top_tr_tab_qry_rslt1['States'] = df_top_tr_tab_qry_rslt1['States'].astype(str)
            df_top_tr_tab_qry_rslt1['Top Transaction amount'] = df_top_tr_tab_qry_rslt1[
                'Top Transaction amount'].astype(float)
            df_top_tr_tab_qry_rslt1_fig = px.bar(df_top_tr_tab_qry_rslt1, x='States', y='Top Transaction amount',
                                                 color='Top Transaction amount', color_continuous_scale='thermal',
                                                 title='Top Transaction Analysis Chart', height=600, )
            df_top_tr_tab_qry_rslt1_fig.update_layout(title_font=dict(size=33), title_font_color='#AD71EF')
            st.plotly_chart(df_top_tr_tab_qry_rslt1_fig, use_container_width=True)


            #All India Total Transaction calculation Table
            st.subheader(':violet[Top Transaction Analysis]')
            st.dataframe(df_top_tr_anly_tab_qry_rslt1)

        # OVERALL TOP USER DATA
        # USER TAB
        with tab6:
            top_us_yr = st.selectbox('**Select Years**', ('2018', '2019', '2020', '2021', '2022'), key='top_us_yr')

            #SQL QUERY

            #Top User Analysis bar chart query
            cursor.execute(f"SELECT States, SUM(RegisteredUsers) AS Top_user FROM top_user WHERE Years='{top_us_yr}' GROUP BY States ORDER BY Top_user DESC LIMIT 10;")
            top_us_tab_qry_rslt = cursor.fetchall()
            df_top_us_tab_qry_rslt = pd.DataFrame(np.array(top_us_tab_qry_rslt), columns=['States', 'Total User count'])
            df_top_us_tab_qry_rslt1 = df_top_us_tab_qry_rslt.set_index(
                pd.Index(range(1, len(df_top_us_tab_qry_rslt) + 1)))



            #All India User Analysis Bar chart
            df_top_us_tab_qry_rslt1['States'] = df_top_us_tab_qry_rslt1['States'].astype(str)
            df_top_us_tab_qry_rslt1['Total User count'] = df_top_us_tab_qry_rslt1['Total User count'].astype(float)
            df_top_us_tab_qry_rslt1_fig = px.bar(df_top_us_tab_qry_rslt1, x='States', y='Total User count',
                                                 color='Total User count', color_continuous_scale='thermal',
                                                 title='Top User Analysis Chart', height=600, )
            df_top_us_tab_qry_rslt1_fig.update_layout(title_font=dict(size=33), title_font_color='#AD71EF')
            st.plotly_chart(df_top_us_tab_qry_rslt1_fig, use_container_width=True)

            #All India Total Transaction calculation Table
            st.subheader(':violet[Total User Analysis]')
            st.dataframe(df_top_us_tab_qry_rslt1)

    if select == "BASIC INSIGHTS":

        st.title(':violet[BASIC INSIGHTS]')
        st.subheader("The basic insights are derived from the Analysis of the Phonepe Pulse data. It provides a clear idea about the analysed data.")
        options = ["--select--",
                "1.Top 10 states based on year and amount of transaction",
                "2.Least 10 states based on year and amount of transaction",
                "3.Top 10 States and Districts based on Registered Users",
                "4.Least 10 States and Districts based on Registered Users",
                "5.Top 10 Districts based on the Transaction Amount",
                "6.Least 10 Districts based on the Transaction Amount",
                "7.Top 10 Districts based on the Transaction count",
                "8.Least 10 Districts based on the Transaction count",
                "9.Top Transaction types based on the Transaction Amount",
                "10. Top 10 Mobile Brands based on the User count of transaction"]
        select = st.selectbox(":violet[Select the option]",options)

        #1
        if select == "1.Top 10 states based on year and amount of transaction":
            cursor.execute(
                "SELECT DISTINCT States,Years, SUM(Transaction_amount) AS Total_Transaction_Amount FROM top_transaction GROUP BY States,Years ORDER BY Total_Transaction_Amount DESC LIMIT 10");

            data = cursor.fetchall()
            columns = ['States', 'Years', 'Transaction_amount']
            df = pd.DataFrame(data, columns=columns, index=range(1, len(data) + 1))

            col1, col2 = st.columns(2)
            with col1:
                st.write(df)
            with col2:
                st.bar_chart(data=df, x="Transaction_amount", y="States")

        #2
        elif select == "2.Least 10 states based on year and amount of transaction":
            cursor.execute(
                "SELECT DISTINCT States,Years, SUM(Transaction_amount) as Total FROM top_transaction GROUP BY States, Years ORDER BY Total ASC LIMIT 10");
            data = cursor.fetchall()
            columns = ['States', 'Years', 'Transaction_amount']
            df = pd.DataFrame(data, columns=columns, index=range(1,len(data)+1))
            col1, col2 = st.columns(2)
            with col1:
                st.write(df)
            with col2:
                st.bar_chart(data=df, x="Transaction_amount", y="States")

        #3
        elif select == "3.Top 10 States and Districts based on Registered Users":
            cursor.execute("SELECT DISTINCT States, Districts, SUM(RegisteredUsers) AS Users FROM top_user GROUP BY States, Districts ORDER BY Users DESC LIMIT 10");
            data = cursor.fetchall()
            columns = ['States', 'Districts', 'RegisteredUsers']
            df = pd.DataFrame(data, columns=columns, index=range(1, len(data) + 1))

            col1, col2 = st.columns(2)
            with col1:
                st.write(df)
            with col2:
                st.bar_chart(data=df, x="RegisteredUsers", y="States")

        #4
        elif select == "4.Least 10 States and Districts based on Registered Users":
            cursor.execute("SELECT DISTINCT States, Districts, SUM(RegisteredUsers) AS Users FROM top_user GROUP BY States, Districts ORDER BY Users ASC LIMIT 10");
            data = cursor.fetchall()
            columns = ['States', 'Districts', 'RegisteredUsers']
            df = pd.DataFrame(data, columns=columns, index=range(1, len(data) + 1))
            col1, col2 = st.columns(2)
            with col1:
                st.write(df)
            with col2:
                st.bar_chart(data=df, x="RegisteredUsers", y="States")

        #5
        elif select == "5.Top 10 Districts based on the Transaction Amount":
            cursor.execute(
                "SELECT DISTINCT States ,Districts,SUM(Transaction_Amount) AS Total FROM map_transaction GROUP BY States ,Districts ORDER BY Total DESC LIMIT 10");
            data = cursor.fetchall()
            columns = ['States', 'Districts', 'Transaction_Amount']
            df = pd.DataFrame(data, columns=columns, index=range(1, len(data) + 1))
            col1, col2 = st.columns(2)
            with col1:
                st.write(df)
            with col2:
                st.bar_chart(data=df, x="Districts", y="Transaction_Amount")

        #6
        elif select == "6.Least 10 Districts based on the Transaction Amount":
            cursor.execute(
                "SELECT DISTINCT States,Districts,SUM(Transaction_amount) AS Total FROM map_transaction GROUP BY States, Districts ORDER BY Total ASC LIMIT 10");
            data = cursor.fetchall()
            columns = ['States', 'Districts', 'Transaction_amount']
            df = pd.DataFrame(data, columns=columns, index=range(1, len(data) + 1))

            col1, col2 = st.columns(2)
            with col1:
                st.write(df)
            with col2:
                st.bar_chart(data=df, x="Districts", y="Transaction_amount")

        #7
        elif select == "7.Top 10 Districts based on the Transaction count":
            cursor.execute(
                "SELECT DISTINCT States,Districts,SUM(Transaction_Count) AS Counts FROM map_transaction GROUP BY States ,Districts ORDER BY Counts DESC LIMIT 10");
            data = cursor.fetchall()
            columns = ['States', 'Districts', 'Transaction_Count']
            df = pd.DataFrame(data, columns=columns, index=range(1, len(data) + 1))

            col1, col2 = st.columns(2)
            with col1:
                st.write(df)
            with col2:
                st.bar_chart(data=df, x="Transaction_Count", y="Districts")

        #8
        elif select == "8.Least 10 Districts based on the Transaction count":
            cursor.execute(
                "SELECT DISTINCT States ,Districts,SUM(Transaction_Count) AS Counts FROM map_transaction GROUP BY States ,Districts ORDER BY Counts ASC LIMIT 10");
            data = cursor.fetchall()
            columns = ['States', 'Districts', 'Transaction_Count']
            df = pd.DataFrame(data, columns=columns, index=range(1, len(data) + 1))

            col1, col2 = st.columns(2)
            with col1:
                st.write(df)
            with col2:
                st.bar_chart(data=df, x="Transaction_Count", y="Districts")

        #9
        elif select == "9.Top Transaction types based on the Transaction Amount":
            cursor.execute(
                "SELECT DISTINCT Transaction_type, SUM(Transaction_amount) AS Amount FROM aggregated_transaction GROUP BY Transaction_type ORDER BY Amount DESC LIMIT 5");
            data = cursor.fetchall()
            columns = ['Transaction_type', 'Transaction_amount']
            df = pd.DataFrame(data, columns=columns, index=range(1, len(data) + 1))

            col1, col2 = st.columns(2)
            with col1:
                st.write(df)
            with col2:
                st.bar_chart(data=df, x="Transaction_type", y="Transaction_amount")

        #10
        elif select == "10. Top 10 Mobile Brands based on the User count of transaction":
            cursor.execute(
                "SELECT DISTINCT Brands,SUM(Transaction_count) as Total FROM aggregated_user GROUP BY Brands ORDER BY Total DESC LIMIT 10");
            data = cursor.fetchall()
            columns = ['Brands', 'Transaction_count']
            df = pd.DataFrame(data, columns=columns, index=range(1, len(data) + 1))

            col1, col2 = st.columns(2)
            with col1:
                st.write(df)
            with col2:
                st.bar_chart(data=df , x="Transaction_count", y="Brands")


