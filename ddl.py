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

# Queries for Temp Table
cs.execute("use schema DW_TMP;")

cs.execute("""
CREATE OR REPLACE TABLE D_BHATBHATENI_CNTRY_T
(
  CNTRY_KY NUMBER NOT NULL AUTOINCREMENT,
  CNTRY_ID NUMBER, 
  CNTRY_DESC VARCHAR(50),
  CONSTRAINT CNTRY_PK PRIMARY key (CNTRY_KY)
);""")

cs.execute("""
CREATE OR REPLACE TABLE D_BHATBHATENI_RGN_T
(
  RGN_KY NUMBER NOT NULL AUTOINCREMENT,
  RGN_ID NUMBER,
  CNTRY_KY NUMBER,
  RGN_DESC VARCHAR(50),
  CONSTRAINT RGN_PK PRIMARY key (RGN_KY),
  CONSTRAINT CNTRY_FK FOREIGN key (CNTRY_KY) REFERENCES D_BHATBHATENI_CNTRY_T(CNTRY_KY)
);
""")

cs.execute("""
create or replace TABLE D_BHATBHATENI_STORE_T (
	STORE_KY NUMBER(38,0) NOT NULL AUTOINCREMENT,
	STORE_ID NUMBER(38,0),
	RGN_KY NUMBER(38,0),
	STORE_DESC VARCHAR(50),
	constraint STORE_PK primary key (STORE_KY),
	constraint RGN_FK foreign key (RGN_KY) references D_BHATBHATENI_RGN_T(RGN_KY)
);
""")

# Queries for Target Table
cs.execute("USE SCHEMA DW_TGT")
cs.execute("""
            CREATE OR REPLACE TABLE D_BHATBHATENI_CNTRY_T
            (
            CNTRY_ID NUMBER,
            CNTRY_KY NUMBER NOT NULL,
            CNTRY_DESC VARCHAR(50),
            OPEN_CLOSE_CD VARCHAR(1), 
            ROW_INSRT_TMS TIMESTAMP_NTZ,
            ROW_UPDT_TMS TIMESTAMP_NTZ,
            CONSTRAINT CNTRY_PK PRIMARY key (CNTRY_KY)
            );
            """)

cs.execute("""
            CREATE OR REPLACE TABLE D_BHATBHATENI_RGN_T
            (
            RGN_ID NUMBER,
            RGN_KY NUMBER NOT NULL,
            CNTRY_KY NUMBER,
            RGN_DESC VARCHAR(50),
            OPEN_CLOSE_CD VARCHAR(1),  
            ROW_INSRT_TMS TIMESTAMP_NTZ,
            ROW_UPDT_TMS TIMESTAMP_NTZ,
            CONSTRAINT RGN_PK PRIMARY key (RGN_KY),
            CONSTRAINT CNTRY_FK FOREIGN key (CNTRY_KY) REFERENCES D_BHATBHATENI_CNTRY_T(CNTRY_KY) 
            );
            """)
cs.execute("""
            CREATE OR REPLACE TABLE D_BHATBHATENI_STORE_T
            (
            STORE_ID NUMBER,
            STORE_KY NUMBER NOT NULL,
            RGN_KY NUMBER,
            STORE_DESC VARCHAR(50),
            LAST_OPEN_TMS TIMESTAMP_NTZ,
            LAST_CLOSED_TMS TIMESTAMP_NTZ,
            ACTV_FLG VARCHAR(1),
            OPEN_CLOSE_CD VARCHAR(1), 
            ROW_INSRT_TMS TIMESTAMP_NTZ,
            ROW_UPDT_TMS TIMESTAMP_NTZ,
            CONSTRAINT STORE_PK PRIMARY KEY (STORE_KY),
            CONSTRAINT RGN_FK FOREIGN KEY (RGN_KY) REFERENCES D_BHATBHATENI_RGN_T(RGN_KY)
            );
            """)
cs.close()
