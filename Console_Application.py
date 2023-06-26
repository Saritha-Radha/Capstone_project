#Importing all the libraries 

#import os
import sys
from datetime import datetime
import re
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType,VarcharType,TimestampType,StringType
import mysql.connector as dbconnect

#Creating the Spark Session
spark = SparkSession.builder.appName("CreditCardSystemApp").master("local[*]").getOrCreate()


#Function to print main menu
def menu():
    print('\t\t\t+-' + '-' * 61+ '-+')
    print('\t\t\t!\t\t''Credit Card Management System\t\t\t!')
    print('\t\t\t+-' + '-' * 61+ '-+')
    print()
    print('\t'*3+'!\t\t'+'1.Transaction Details'+'\t'*4+'!\n')
    print('\t'*3+'!\t\t'+'2.Customer Details '+'\t'*4+'!\n')
    print('\t'*3+'!\t\t'+'3.Quit'+'\t'*6+'!')
    print('\t\t\t+-' + '-' * 61+ '-+')


#Function to print Submenu for Transaction
def submenu():
    print('\t\t\t+-' + '-' * 61+ '-+')
    print('\t\t\t!\t\tCredit Card Management System\t\t\t!')
    print('\t\t\t+-' + '-' * 61+ '-+')
    print()
    print('\t'*3+'!\t\t'+'1.Main Menu'+'\t'*5+'!\n')
    print('\t'*3+'!\t\t'+'2.Transactions by zipcode,month & year'+'\t'*2+'!\n')
    print('\t'*3+'!\t\t'+'3.Transaction count and value for a given type'+'\t'+'!\n')
    print('\t'*3+'!\t\t'+'4.Transaction count and value by State'+'\t'*2+'!')
    print('\t\t\t+-' + '-' * 61+ '-+')

#Function to print Submeny for Customer module
def customersubmenu():
    print('\t\t\t+-' + '-' * 61+ '-+')
    print('\t\t\t!\t\tCredit Card Management System\t\t\t!')
    print('\t\t\t+-' + '-' * 61+ '-+')
    print()
    print('\t'*3+'!\t\t'+'1.Main Menu'+'\t'*5+'!\n')
    print('\t'*3+'!\t\t'+'2.Check Existing Account Details of a Customer'+'\t'*1+'!\n')
    print('\t'*3+'!\t\t'+'3.Modify Existing Account Details of a Customer'+'\t'*1+'!\n')
    print('\t'*3+'!\t\t'+'4.Monthly Bill for Creditcard Number'+'\t'*2+'!\n')
    print('\t'*3+'!\t\t'+'5.Transactions Made by a Customer'+'\t'*2+'!\n')
    print('\t\t\t+-' + '-' * 61+ '-+')

#Main function
def main():
    menu()

#Function to validate zipcode input
def check_zipcode_valid():
    zipcode=""
    while True:
        zipcode=input("Please Enter your 5 digit Zipcode:")
        if zipcode.isdigit() and len(zipcode)==5:
            #print('valid ')
            #print(zipcode)
            return zipcode
        else:
         print("Please enter a valid Zipcode")
         break           
    return check_zipcode_valid()


#Function to validate month input
def check_month_valid():
    month=input("Please Enter Month.Use format ex:06 for June: ")
    while True:
        
        if month.isdigit() and len(month)>0 and len(month)<3 and int(month)<13:
          #print('valid')
          #print(month)
          return month
        else:
            print("Please enter a valid Month")
            break
    return check_month_valid()


#Function to validate year
def check_year_valid():
    print("")
    year=input("Please Enter Year: ")
    year=year.strip()
    while True:
        
        if year.isdigit() and len(year)>0 and len(year)<5:
          #print('valid')
          return year
        else:
            print("Please enter a valid Year")
            break
    return check_year_valid()

"""Functional Requirements 2.1 1) Used to display the transactions made by customers living in a
given zip code for a given month and year. Order by day in
descending order."""

def customer_zipcode_month_year(zipcode,month,year):
    #print(zipcode,month,year)
    query='(select cust.FIRST_NAME,cust.LAST_NAME,cust.CUST_PHONE,cr.TRANSACTION_ID,cr.TRANSACTION_VALUE,cr.TIMEID' \
            ' from cdw_sapp_credit_card cr  join cdw_sapp_customer cust'\
            ' on cr.cust_cc_no=cust.credit_card_no'\
            ' where cust.cust_zip='+zipcode+' and year(cr.TimeID)='+year+ ' and month(cr.timeId)='+month+' order by day(cr.timeid) desc) as customer'

    data_table1=spark.read.format("jdbc") \
     .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
     .option("dbtable",query ) \
     .option("user", "root") \
     .option("password", "password")\
     .load()
    data_table1.show(n=100)
    if data_table1.count()==0:
        print("No Records found!")

#Function to check for the valid transaction Type
def check_transactiontype_valid():
    transactiontype=input("Please Enter Transaction Type : ")
    print()
    transactiontype=transactiontype.strip().title()
    query='(select distinct transaction_type from cdw_sapp_credit_card) as distinct_transactions'
    data_distinct_transactiontype=spark.read.format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
        .option("dbtable",query ) \
        .option("user", "root") \
        .option("password", "password")\
        .load()
    data_distinct_transactiontype_list=data_distinct_transactiontype.select(data_distinct_transactiontype.transaction_type).toPandas()
    data_distinct_transactiontype_listnew=list(data_distinct_transactiontype_list['transaction_type'])

    while True:
        if transactiontype.title() in data_distinct_transactiontype_listnew:
            return transactiontype
        else:
         print("Please enter a valid Transaction Type from ",data_distinct_transactiontype_listnew)
         break
    return check_transactiontype_valid()

"""Functional Requirements 2.1 2) Used to display the number and total values of transactions for a
given type."""
def trasactiontype_details(transactiontype):
    #print("hello",transaction_type)
    query="(select count(*) as 'Number of Transactions',round(sum(TRANSACTION_VALUE),3) as 'Total Amount',\
        TRANSACTION_TYPE from cdw_sapp_credit_card where TRANSACTION_TYPE='"+transactiontype+"' group by TRANSACTION_TYPE order by 1 desc) as Transactiontypedetails"
    #print(query)
    data_trasactiontype=spark.read.format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
        .option("dbtable",query ) \
        .option("user", "root") \
        .option("password", "password")\
        .load()
    data_trasactiontype.show()
    if data_trasactiontype.count()==0:
        print("No Records found!")

#Function to validate State abbreviation
def check_state_valid():
     state=input("Please Enter State : ")
     state=state.strip()
     query='(select distinct BRANCH_STATE from cdw_sapp_branch) as distinct_state'
     data_distinct_state=spark.read.format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
        .option("dbtable",query ) \
        .option("user", "root") \
        .option("password", "password")\
        .load()
     #data_distinct_state.show()
     data_distinct_state=data_distinct_state.select(data_distinct_state.BRANCH_STATE).toPandas()
     data_distinct_statenew=list(data_distinct_state['BRANCH_STATE'])
     #print(data_distinct_statenew)
     while True:
        if state.upper() in data_distinct_statenew:
            return state
        else:
         print("Please enter a valid state abbreviation from ",data_distinct_statenew)
         break
     return check_state_valid()

"""Functional Requirements 2.1 .3) Used to display the total number and total values of transactions
for branches in a given state."""
def trasactiontype_statewise(state):
    query="(select count(*) AS'Number Of Transactions',round(sum(credit.TRANSACTION_VALUE),3) \
            as 'Total Amount' ,br.BRANCH_CODE from cdw_sapp_credit_card credit join cdw_sapp_branch br\
            on credit.BRANCH_CODE=br.BRANCH_CODE\
             WHERE br.BRANCH_STATE='"+state+"'\
             group by credit.BRANCH_CODE order by 1 desc )as distinct_transaction_state"
   # print(query)
    data_trasaction_state=spark.read.format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
        .option("dbtable",query ) \
        .option("user", "root") \
        .option("password", "password")\
        .load()
    data_trasaction_state.show()
    if data_trasaction_state.count()==0:
        print("No Records found!")

# Function to validate customer input 
def check_customer_valid():
    customer_firstname=input("Please Enter Customer's First Name : ")
    print("")
    customer_firstname=customer_firstname.strip()
    customer_lastname=input("Please Enter Customer's Last Name : ")
    print("")
    customer_lastname=customer_lastname.strip()
    customer_ssn=input("Please Enter last 4 digits of Customer's SSN : ")
    print("")
    customer_ssn=customer_ssn.strip()
    while True:
        if customer_firstname.title().isalpha() and customer_lastname.title().isalpha() and customer_ssn.isdigit() and len(customer_ssn)==4:
            #print('valid')
            return customer_firstname,customer_lastname,customer_ssn
           
        else:
         print("Please enter valid customer details")
         break
    return check_customer_valid()

"""Functional
Requirements 2.2
1) Used to check the existing account details of a customer."""
def customer_details(customer_firstname,customer_lastname,customer_ssn):
    query="( select FIRST_NAME as 'First Name',MIDDLE_NAME as 'Middle Name',LAST_NAME as 'Last Name',\
            CREDIT_CARD_NO as 'Credit Card Number',SSN ,CUST_PHONE as 'Phone Number',CUST_EMAIL AS 'Email',\
            CUST_CITY as 'City',cust_state AS 'State',CUST_COUNTRY as 'Country',FULL_STREET_ADDRESS as 'Address',\
            CUST_ZIP as 'Zip Code',LAST_UPDATED as 'Last Updated' from cdw_sapp_customer where FIRST_NAME='"+customer_firstname+"' and LAST_NAME='"+customer_lastname+"' and SSN like '%"+customer_ssn+"') as customer_details"
    data_customer_details=spark.read.format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
        .option("dbtable",query ) \
        .option("user", "root") \
        .option("password", "password")\
        .load()
    data_customer_details.show(truncate=False,vertical=True)
    if data_customer_details.count()==0:
        print("No Records found!")

#Function to validate SSN
def check_ssn():
   
    customer_ssn=input("Please Enter last 4 digits of your SSN : ")
    print("")
    customer_ssn=customer_ssn.strip()
    while True:
        if  customer_ssn.isdigit()  and len(customer_ssn)==4:
            #print('valid')
            return customer_ssn
           
        else:
         print("Please enter a valid customer SSN")
         break
    return check_ssn()

#Function to validate Creditcard input
def check_creditcard():
   
    customer_creditcard=input("Please Enter last 4 digits of your creditcard : ")
    print("")
    customer_creditcard=customer_creditcard.strip()
    while True:
        if  customer_creditcard.isdigit()  and len(customer_creditcard)==4:
            #print('valid')
            return customer_creditcard
           
        else:
         print("Please enter a valid Creditcard number")
         break
    return check_creditcard()

# Function to retrieve customer information
def get_customer_details(customer_ssn):
    #print("func",customer_ssn)
    query="( select FIRST_NAME as 'First Name',MIDDLE_NAME as 'Middle Name',LAST_NAME as 'Last Name',\
            CREDIT_CARD_NO as 'Credit Card Number',SSN ,CUST_PHONE as 'Phone Number',CUST_EMAIL AS 'Email',\
            CUST_CITY as 'City',cust_state AS 'State',CUST_COUNTRY as 'Country',FULL_STREET_ADDRESS as 'Address',\
            CUST_ZIP as 'Zip Code',LAST_UPDATED as 'Last Updated' from cdw_sapp_customer \
            where  SSN like '%"+customer_ssn+"') as customer_details"
    data_customer_details=spark.read.format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
        .option("dbtable",query ) \
        .option("user", "root") \
        .option("password", "password")\
        .load()
    print("\t\t Your Account Details ")
    if data_customer_details.count()>0:
        data_customer_details.show(vertical=True)
        customer_oldphonenumber=data_customer_details.first()[5]
        customer_oldemail=data_customer_details.first()[6]
        customer_oldcity=data_customer_details.first()[7]
        customer_oldstate=data_customer_details.first()[8]
        customer_oldcountry=data_customer_details.first()[9]
        customer_address=data_customer_details.first()[10]
        customer_zipcode=data_customer_details.first()[11]
        update_customer_details(customer_ssn,customer_oldphonenumber,customer_oldemail,customer_oldcity,customer_oldstate,customer_oldcountry,customer_address,customer_zipcode)
    else:
        print("Invalid Customer SSN.Please try again !")
"""Functional
Requirements 2.2
2) Used to modify the existing account details of a customer."""

def update_customer_details(customer_ssn,customer_oldphonenumber,customer_oldemail,customer_oldcity,customer_oldstate,customer_oldcountry,customer_address,customer_zipcode):
    print("\t\t\t Please Update Your Credentials!")
    new_phonenumber=input("Enter the new phone number.Please use this format (XXX)-XXX-XXXX (Press enter to skip): ")
    print("")
    if new_phonenumber!="":
        while not check_phonenumber(new_phonenumber):
            print("Invalid Phone Number format.Please use this format (XXX)-XXX-XXXX")
            new_phonenumber=input("Enter the new phone number(Press enter to skip): ")
            if new_phonenumber=="":
                break
      
       
    new_email=input("Enter the new email address(Press enter to skip): ")
    print("")
    if new_email!="":
        while not check_email(new_email):
            print("Invalid email format")
            new_email=input("Enter the new email address(Press enter to skip): ")
            if new_email=="":
                break

    new_city=input("Enter the new City(Press enter to skip): ")
    print("")
    if new_city!="":
        while not check_city(new_city):
            print("Invalid city format")
            new_city=input("Enter the new City(Press enter to skip): ")
            if new_city=="":
                break
    new_State=input("Enter the new State(Press enter to skip): ")
    print("")
    if new_State!="":
        while not check_state(new_State):
            print("Invalid State abbreviation format .Please enter 2 letter State Abbreviation.Ex:GA")
            new_State=input("Enter the new State(Press enter to skip): ")
            if new_State=="":
                break
    
    new_country=input("Enter the new Country(Press enter to skip): ")
    print("")
    if new_country!="":
        while not check_country(new_country):
            print("Invalid Country format")
            new_country=input("Enter the new Country(Press enter to skip): ")
            if new_country=="":
                break
    new_address=input("Enter the new Address(Press enter to skip): ")
    print("")
    if new_address!="":
        while not check_address(new_address):
            print("Invalid Address format")
            new_address=input("Enter the new Address(Press enter to skip): ")
            if new_address=="":
                break
    new_Zipcode=input("Enter the new Zipcode(Press enter to skip): ")
    print("")
    if new_Zipcode!="":
        while not check_zipcode(new_Zipcode):
            print("Invalid Zipcode format")
            new_Zipcode=input("Enter the new Zipcode(Press enter to skip): ")
            if new_Zipcode=="":
                break

    if(new_phonenumber)=="":
        new_phonenumber=customer_oldphonenumber
    else:
        new_phonenumber=new_phonenumber.strip()
    if(new_email)=="":
        new_email=customer_oldemail
    else:
        new_email=new_email.strip()
    if(new_city)=="":
        new_city=customer_oldcity
    else:
        new_city=new_city.strip().title()
    if(new_State)=="":
        new_State=customer_oldstate
    else:
        new_State=new_State.upper()        
    if(new_country)=="":
        new_country=customer_oldcountry
    else:
        new_country=new_country.strip().title()
    if(new_address)=="":
        new_address=customer_address
    else:
        new_address=new_address.strip().title()
    if(new_Zipcode)=="":
        new_Zipcode=customer_zipcode
    timeid=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("Updated details",new_phonenumber,new_email,new_city,new_State,new_country,new_Zipcode,new_address)
    query="UPDATE cdw_sapp_customer SET CUST_CITY='"+new_city+\
        "' ,CUST_EMAIL='"+new_email+"', CUST_PHONE='"+new_phonenumber+\
        "', CUST_STATE='"+new_State+"', CUST_ZIP="+str(new_Zipcode)+" ,FULL_STREET_ADDRESS='"+\
        new_address+"', LAST_UPDATED='"+str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))+"' where SSN like '%"+customer_ssn+"'"
   
    myconnection=dbconnect.connect(host='localhost',database='creditcard_capstone',user='root',password='password')
    if myconnection.is_connected():
        print("")
    cursor=myconnection.cursor()
    cursor.execute(query)
    myconnection.commit()
    print(cursor.rowcount,"record(s) affected")
    #print("Updated")
    cursor.close()
    myconnection.close()

def check_phonenumber(new_phonenumber):
    phone_pattern=r'^\(\d{3}\)\d{3}-\d{4}'
    #print("hi")
    if re.match(phone_pattern,new_phonenumber):
       # print("valid phone patter")
        return True
    else:
        #print("not valid pattern")
        return False
def check_email(new_email):
    email_pattern=r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b'
    print("hi")
    if re.match(email_pattern,new_email):
        #print("valid email patter")
        return True
    else:
       # print("not valid pattern")
        return False
def check_city(new_city):
    
    if type(new_city.strip())==str:
        
        return True
    else:
        return False
def check_state(new_state):
    if new_state.strip().isalpha() and len(new_state)==2:
        return True
    else:
        return False
def check_country(new_country):
    if type(new_country.strip())==str:
        return True
    else:
        return False
def check_address(new_address):
    addresspattern='\d+,\w+( \w+)*$'
    if re.match(addresspattern,new_address):
       # print("valid address patter")
        return True
    else:
        #print("not valid pattern")
        return False
def check_zipcode(new_Zipcode):
    zipcodepattern='^\d{5}'
    if re.match(zipcodepattern,new_Zipcode):
       # print("valid zipcode patter")
        return True
    else:
       # print("not valid pattern")
        return False
def check_dates(startdate):
    datepattern='^\d{4}-\d{2}-\d{2}$'
    if re.match(datepattern,startdate):
        #print("Valid date pattern")
        return True
    else:
        #print("not valid pattern")
        return False
def credit_bill_month_year():
    #customer_creditcard=input("Enter the last 4 digits of Credit Card Number")
    #customer_ssn=input("Please enter last 4 digits of ssn")
    customer_creditcard=check_creditcard()
    customer_ssn=check_ssn()
    month=check_month_valid()
    year=check_year_valid()
    #print(customer_creditcard,customer_ssn,month,year)
    query="( select BRANCH_CODE as 'Branch Code' ,TRANSACTION_ID as 'Transaction ID',TRANSACTION_TYPE as 'Transaction Type',TRANSACTION_VALUE as 'Transaction Value' ,TIMEID \
            from cdw_sapp_credit_card where  CUST_SSN like '%"+customer_ssn+"' and CUST_CC_NO like '%"+customer_creditcard+"' and MONTH(TIMEID)='"+str(month)+"' and year(TIMEID)='"+str(year)+"') as customer_details"
    #print(query)
    
    data_monthly_bill_details=spark.read.format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
        .option("dbtable",query ) \
        .option("user", "root") \
        .option("password", "password")\
        .load()
    print('\t\t\t+-' + '-' * 61+ '-+')
    print('\t\t\t!''Bill for the Credit Card Number ending in '+customer_creditcard+' for '+month+'/'+year+'\t!')
    print('\t\t\t+-' + '-' * 61+ '-+')
    print()
    data_monthly_bill_details.show(n=100)
    if data_monthly_bill_details.count()==0:
        print("No Records found!")
  
    df_total = data_monthly_bill_details.withColumn("Transaction Value", data_monthly_bill_details['Transaction Value'].cast('integer'))
    df_total.select(func.sum('Transaction Value').alias('Total Amount')).show()
  
def transaction_customer():
    print("")
    customer_ssn=check_ssn()
    print("")
    startdate=input("Please enter Start date (Use the format YYYY-MM-DD ) : ")
    print("")
    while not check_dates(startdate):
            print("Invalid date format")
            startdate=input("Enter date format as YYYY-MM-DD: ")
    enddate=input("Please enter end date (Use the format YYYY-MM-DD ): ")
    while not check_dates(enddate):
        print("Invalid date format")
        enddate=input("Enter date format as YYYY-MM-DD: ")
    query="( select BRANCH_CODE as 'Branch Code' ,TRANSACTION_ID as 'Transaction ID',TRANSACTION_TYPE as 'Transaction Type',TRANSACTION_VALUE as 'Transaction Value' ,TIMEID \
            from cdw_sapp_credit_card where  CUST_SSN like '%"+customer_ssn+"' and TIMEID BETWEEN'"+startdate+"' and '"+enddate+"' order by year(TIMEID) DESC,MONTH(TIMEID) DESC ,DAY(TIMEID) DESC) as customer_details"
    #print(query)
    monthly_bill_details=spark.read.format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
        .option("dbtable",query ) \
        .option("user", "root") \
        .option("password", "password")\
        .load()
    print('\t\t\t+-' + '-' * 61+ '-+')
    print('\t\t\t!''Transaction made by customer  between'+startdate+' and '+enddate+'\t!')
    print('\t\t\t+-' + '-' * 61+ '-+')
    print()
    monthly_bill_details.show(n=100)
    if monthly_bill_details.count()==0:
        print("No Records found!")



# The main function to loop through the menu 
if __name__=='__main__':
    #os.system("CLS")
    main()
    while True:
      
        choice=input("Enter your Choice: ")
        choice=choice.strip()
        if(choice=='1'):
             submenu()
             while True:
                 submenu_choice=input('Enter your choice: ')
                 submenu_choice=submenu_choice.strip()
                 if(submenu_choice=='1'):
                   
                     menu()
                     break
                 elif(submenu_choice=='2'):
                    print('\t\t\t+-' + '-' * 61+ '-+')
                    print('\t\t\t!\t\t'+'Transaction details by Zipcode,Month and Year'+'\t!')
                    print('\t\t\t+-' + '-' * 61+ '-+')
                    print()
                    # Check Valid Zipcode .Line No:59
                    zipcode=check_zipcode_valid()
                    print()
                    #Check Valid Month.Line no:74
                    month=check_month_valid()
                    print()
                    #Check Valid Year .Line no:89
                    year=check_year_valid()
                    #print(zipcode,month,year)
                    print('\t\t\t+-' + '-' * 61+ '-+')
                    customer_zipcode_month_year(zipcode,month,year) # Function to read data from database .Line no:107
                    inputenter=input("Press Enter Key to Continue: ")
                    if(inputenter==""):
                        submenu()

                 elif(submenu_choice=='3'):
                    print('\t\t\t+-' + '-' * 61+ '-+')
                    print('\t\t\t!\t\t'+'Transactions  By Transaction Type'+'\t\t!')
                    print('\t\t\t+-' + '-' * 61+ '-+')
                    print("")
                    #Check if the input is a valid transaction type.Line no:123
                    transaction_type=check_transactiontype_valid()
                    print('\t\t\t+-' + '-' * 61+ '-+')
                    trasactiontype_details(transaction_type) # Function to get the data from database .Line no:147
                    inputenter=input("Press Enter Key to Continue: ")
                    if(inputenter==""):
                        submenu()
                    
                 elif(submenu_choice=='4'):
                    print('\t\t\t+-' + '-' * 61+ '-+')
                    print('\t\t\t!\t\t'+'Transactions by State'+'\t'*4+'!')
                    print('\t\t\t+-' + '-' * 61+ '-+')
                    #Check the input valid .Line no:161
                    state=check_state_valid()
                    trasactiontype_statewise(state) # Function to get the data.Line no:185
                    inputenter=input("Press Enter Key to Continue: ")
                    if(inputenter==""):
                        submenu()
                 else:
                     print("invalid Input.Please Try Again !")
                     submenu()
        
        elif(choice=='2'):
            customersubmenu()
            while True:
                 customersubmenu_choice=input('Enter your choice: ')
                 customersubmenu_choice=customersubmenu_choice.strip()
                 if(customersubmenu_choice=='1'):
                     menu()
                     break
                 elif(customersubmenu_choice=='2'):
                     print("")
                     # Checking Customer details input . line:201
                     customer_firstname,customer_lastname,customer_ssn=check_customer_valid()
                     print('\t\t\t+-' + '-' * 61+ '-+')
                     print('\t\t\t!\t\t'+'Customer Details'+'\t'*4+'!')
                     print('\t\t\t+-' + '-' * 61+ '-+')
                     # Function to get the details .Line no 224
                     customer_details(customer_firstname,customer_lastname,customer_ssn)
                     inputenter=input("Press Enter Key to Continue: ")
                     if(inputenter==""):
                        customersubmenu()
                 elif (customersubmenu_choice=='3'):
                     #Checking SSN .Line no:238
                     customer_ssn=check_ssn()
                     #print(customer_ssn)
                     #Customer details by SSN .Line no:270
                     get_customer_details(customer_ssn)
                     inputenter=input("Press Enter Key to Continue: ")
                     if(inputenter==""):
                        customersubmenu()
                 
                 elif (customersubmenu_choice=='4'):
                     print("")
                     #  Used to generate a monthly bill for a credit card number for a given month and year.Line :467

                     credit_bill_month_year()
                     inputenter=input("Press Enter Key to Continue: ")
                     if(inputenter==""):
                        customersubmenu()
                 elif (customersubmenu_choice=='5'):
                     #Used to display the transactions made by a customer betweentwo dates. 
                     # Order by year, month, and day in descending order.Line No:495

                     transaction_customer()
                     inputenter=input("Press Enter Key to Continue: ")
                     if(inputenter==""):
                        customersubmenu()
                 else :
                    print("invalid Input.Please Try Again !")
                    customersubmenu() 
        elif(choice=='3'):
            break
        else :
             print("invalid Input.Please Try Again !")
             main()

spark.stop()