# Capstone_project
Data Engineering - Capstone Project 
## Project Description					
This project is a comprehensive data engineering solution developed as the capstone project for the Perscholas  bootcamp. The project focuses on extracting credit card data from various sources, transforming it using Apache Spark, loading it into a MySQL database, and visualizing the results through interactive dashboards created using Tableau.
			
### Key Features:
Data ingestion module to extract credit data from JSON files and RESTful APIs
Apache Spark for performing complex data transformations and data quality checks
MySQL database for efficient storage of transformed financial data
Interactive visualizations and dashboards created using Tableau for presenting key financial metrics and trends
### Technologies Used:
- Python
- Apache Spark
- MySQL
- Tableau
- Git
## Architecture

The technical architecture for this project is as show below	
![](images/WorkflowDiagram.png)

## Credit Card Dataset Overview
The Credit Card System database is an independent system developed for managing activities such as registering new customers and approving or canceling requests, etc., using the architecture.
A credit card is issued to users to enact the payment system. It allows the cardholder to access financial services in exchange for the holder's promise to pay for them later. Below are three files that contain the customer’s transaction information and inventories in the credit card information.
- CDW_SAPP_CUSTOMER.JSON: This file has the existing customer details.
- CDW_SAPP_CREDITCARD.JSON: This file contains all credit card transaction information.
- CDW_SAPP_BRANCH.JSON: Each branch’s information and details are recorded in this
file.
## 1.Functional Requirements - Load Credit Card Database (SQL) 
						
### Data Extraction and Transformation with Python and PySpark 
				
### Functional Requirement 1.1 
a) For “Credit Card System,” create a Python and PySpark SQL program to read/extract the following JSON files according to the specifications found in the mapping document.				
-  CDW_SAPP_BRANCH.JSON
-  CDW_SAPP_CREDITCARD.JSON 
-  CDW_SAPP_CUSTOMER.JSON
  ### Req-1.2 						
Data loading into Database 					
### Function Requirement 1.2
Once PySpark reads data from JSON files, and then utilizes Python, PySpark, and Python modules to load data into RDBMS(SQL),
perform the following:

a)  Create a Database in SQL(MySQL), named “creditcard_capstone.”
 				
b)  Create a Python and Pyspark Program to load/write the “Credit
 								
Card System Data” into RDBMS(creditcard_capstone).
 							
						 						
Tables should be created by the following names in RDBMS: 

 CDW_SAPP_BRANCH
 
 CDW_SAPP_CREDIT_CARD
 
 CDW_SAPP_CUSTOMER 
 
### 2. Functional Requirements - Application Front-End
						
Once data is loaded into the database, we need a front-end (console) to see/display data. For that, create a console-based Python program to satisfy System Requirements 2 (2.1 and 2.2).
						![](images/1.png)
      
### 2.1 Transaction Details Module 	
![](images/2.png)
Functional Requirements 2.1 
1) Used to display the transactions made by customers living in a given zip code for a given month and year.
 Order by day in descending order.
![](images/3.png)
2) Used to display the number and total values of transactions for a given type.
![](images/4.png)
3) Used to display the total number and total values of transactions for branches in a given state.
   ![](images/5.png)
   # 2.2 Customer Details Module 					
##Functional Requirements 2.2 
 ![](images/6.png)
1) Used to check the existing account details of a customer.
   ![](images/7.png)
   2) Used to modify the existing account details of a customer.
      ![](images/8.png)
      ![](images/9.png)
 3) Used to generate a monthly bill for a credit card number for a given month and year
     ![](images/10.png)
4) Used to display the transactions made by a customer between two dates. Order by year, month, and day in descending order.
   ![](images/11.png)
# 3. Functional Requirements - Data Analysis and Visualization
						
After data is loaded into the database, users can make changes from the front end, and they can also view data from the front end. Now, the business analyst team wants to analyze and visualize the data.
Use Python libraries for the below requirements:
## Functional Requirements 3.1 
				
Find and plot which transaction type has a high rate of transactions.
 <img src="images/3.1.png" width="500" > 
 ## Functional Requirements 3.2 
			
Find and plot which state has a high number of customers. 

<img src="images/3.2new.png" width="600" > 

## Functional Requirements 3.3 
			
Find and plot the sum of all transactions for the top 10 customers, and which customer has the highest transaction amount.
<img src="images/3.3new.png" width="600" > 

# Overview of LOAN Application Data API
						
Banks deal in all home loans. They have a presence across all urban, semi-urban, and rural areas. Customers first apply for a home loan; after that, a company will validate the customer's eligibility for a loan.
Banks want to automate the loan eligibility process (in realtime) based on customer details provided while filling out the online application form. These details are Gender, Marital Status, Education, Number of Dependents, Income, Loan Amount, Credit History, and others. To automate this process, they have the task of identifying the customer segments to those who are eligible for loan amounts so that they can specifically target these customers. 
Here they have provided a partial dataset.
# API Endpoint:
https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json
						
The above URL allows you to access information for loan application information. This dataset has all of the required fields for a loan application. You can access data from a REST API by sending an HTTP request and processing the response
## 4. Functional Requirements - LOAN Application Dataset 
					
### Functional Requirements 4.1 
					
Create a Python program to GET (consume) data from the above API endpoint for the loan application dataset. 
					
### Functional Requirements 4.2 
				
						
Find the status code of the above API endpoint. 
					
### Functional Requirements 4.3 
				
						
Once Python reads data from the API, utilize PySpark to load data into RDBMS (SQL). 
The table name should be CDW-SAPP_loan_application in the database. 
		
## 5. Functional Requirements - Data Analysis and Visualization for LOAN Application
						
After the data is loaded into the database, the business analyst team wants to analyze and visualize the data.
						
Use Python libraries for the below requirements:
## Functional Requirements 5.1 
					
Find and plot the percentage of applications approved for self-employed applicants.
<img src="images/4.1.png" width="600" >
## Functional Requirements 5.2 
					
						
Find the percentage of rejection for married male applicants.
<img src="images/5.2.png" width="600" >
## Functional Requirements 5.3 

Find and plot the top three months with the largest transaction data. 
<img src="images/5.3.png" width="600" >
## Functional Requirements 5.4 
						
Find and plot which branch processed the highest total dollar value of healthcare transactions.
<img src="images/5.4.png" width="600" >
## Tableau Dashboard	
Developed an interactive dashboard using Tableau for presenting key financial metrics and trends.
Features of the dashboard include:
- Filters that enable users to drill down onto specific regions .
- Interactive charts and graphs visualize trends and comparisons
![](images/Credit%20card%20transaction%20dashboard.png)

Link to the Dashboard: https://public.tableau.com/shared/S5YSYK6DB?:display_count=n&:origin=viz_share_link

Challenges faced:
Exception while deleting Spark temp directory error.
It's a known windows error 
https://issues.apache.org/jira/browse/SPARK-12216
https://github.com/dotnet/spark/issues/312

I changed the winutil.exe file permission using chmod command and it got resolved.