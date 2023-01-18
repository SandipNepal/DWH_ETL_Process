from connection import cs
from connection import ctx

cs.execute("CREATE OR REPLACE SCHEMA DW_STG;")
cs.execute("CREATE OR REPLACE SCHEMA DW_TMP;")
cs.execute("CREATE OR REPLACE SCHEMA DW_TGT;")


# Queries for Staging Table
cs.execute("USE SCHEMA DW_STG")
cs.execute("create or replace table D_BHATBHATENI_CNTRY_T ( id NUMBER, country_desc VARCHAR(256),PRIMARY KEY (id));")
cs.execute("create or replace table D_BHATBHATENI_RGN_T(id NUMBER,country_id NUMBER,region_desc VARCHAR(256),PRIMARY KEY (id),FOREIGN KEY (country_id) references D_BHATBHATENI_CNTRY_T(id) );")
cs.execute("create or replace table D_BHATBHATENI_STORE_T(id NUMBER,region_id NUMBER,store_desc VARCHAR(256),PRIMARY KEY (id),FOREIGN KEY (region_id) references D_BHATBHATENI_RGN_T(id) );")
cs.execute("create or replace table D_BHATBHATENI_CTGRY_T(id NUMBER,category_desc VARCHAR(1024),PRIMARY KEY (id));")
cs.execute("create or replace table D_BHATBHATENI_SUB_CTGRY_T(id NUMBER,category_id NUMBER,subcategory_desc VARCHAR(256),PRIMARY KEY (id),FOREIGN KEY (category_id) references D_BHATBHATENI_CTGRY_T(id) );")
cs.execute("create or replace table D_BHATBHATENI_PRO_T(id NUMBER,subcategory_id NUMBER,product_desc VARCHAR(256),PRIMARY KEY (id),FOREIGN KEY (subcategory_id) references D_BHATBHATENI_SUB_CTGRY_T(id));")
cs.execute('create or replace table D_BHATBHATENI_CUSTOMER_T(id NUMBER,customer_first_name VARCHAR(256),customer_middle_name VARCHAR(256),customer_last_name VARCHAR(256),customer_address VARCHAR(256) ,primary key (id));')
cs.execute("create or replace table F_BHATBHATENI_SLS_T(id NUMBER,store_id NUMBER NOT NULL,product_id NUMBER NOT NULL,customer_id NUMBER,transaction_time TIMESTAMP,quantity NUMBER,amount NUMBER(20,2),discount NUMBER(20,2),primary key (id),FOREIGN KEY (store_id) references D_BHATBHATENI_STORE_T(id),FOREIGN KEY (product_id) references D_BHATBHATENI_PRO_T(id),FOREIGN KEY (customer_id) references D_BHATBHATENI_CUSTOMER_T(id));")
###############################################################################################################
