#!/usr/bin/python3

import psycopg2
from datetime import date, timedelta, datetime
from random import Random
from csv import reader as csvReader
from pathlib import Path
from subprocess import run as runCmd
from time import perf_counter
import logging

import config

# Logger for debuging and profiling SQL execution
SQL_DEBUG = False
LOG = logging.getLogger("dbgen")
LOG.setLevel(logging.DEBUG)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
file_handler = logging.FileHandler('dbgen-sql.prof')
file_handler.setLevel(logging.DEBUG)
LOG.addHandler(console_handler)
LOG.addHandler(file_handler)
# Postgresql cursor's execute function with debuging and profiling
def _executeWrapper(cur: psycopg2.extensions.cursor, sql: str):
    if SQL_DEBUG:
        LOG.debug(sql)
        st = perf_counter()
    cur.execute(sql)
    if SQL_DEBUG:
        et = perf_counter()
        LOG.debug("\nTime cost:{:.6f}s\n".format(et - st))





# Date constants
MIN_DATE = date(1970, 1, 1)
MAX_DATE = date(9999, 12, 31)
FROM_DATE = date(2000, 1, 1)
TO_DATE = date(2009, 12, 31)




# Random generators
UPDATE_P_RAND = Random(0.7579544029403025)
UPDATE_SCENARIO_P_RAND = Random(0.420571580830845)
UNIFORM_RAND = Random(0.25891675029296335)
TABLE_SAMPLE_SEED_RAND = Random(0.5112747213686085)




# Utility for generating random address
ALPHA_NUMERIC = "0123456789abcdefghijklmnopqrstuvwxyz ABCDEFGHIJKLMNOPQRSTUVWXYZ,"
def generateAddress() -> str:
    return "".join(UNIFORM_RAND.sample(
        ALPHA_NUMERIC, UNIFORM_RAND.randint(10,40)
    ))




# Utility for generating random phone number
def generatePhone(nationkey: int) -> str:
    return "{}-{}-{}-{}".format(
        10 + nationkey, 
        UNIFORM_RAND.randint(100, 999),
        UNIFORM_RAND.randint(100, 999),
        UNIFORM_RAND.randint(1000, 9999)
    )




# Utility for generating random comment
GRAMMER = (
    ("NP", " ", "VP", "T"), 
    ("NP", " ","VP", " ","P", " ","the", " ","NP", "T"),
    ("NP", " ","VP", " ","NP", "T"),
    ("NP", " ","P", " ","the", " ","NP", " ","VP", " ","NP", "T"),
    ("NP", " ","P", " ","the", " ","NP", " ","VP", " ","P", " ","the", 
     " ","NP", "T")
)
GRAMMER_WEIGHT = (3, 3, 3, 1, 1)

NP = (
    ("N"), 
    ("J", " ", "N"), 
    ("J", ",", " ", "J", " ", "N"), 
    ("D", " ",  "J", " ", "N")
)
NP_WEIGHT = (1, 2, 1, 5)

VP = (
    ("V"),
    ("X", " ", "V"),
    ("V", " ", "D"),
    ("X", " ", "V", " ", "D")
)
VP_WEIGHT = (30, 1, 40, 1)

with open("nouns.csv") as nounsFile:
    reader = csvReader(nounsFile, delimiter="|")
    N, N_WEIGHT = (tuple(t) for t in zip(*(reader)))
    N_WEIGHT = tuple(map(int, N_WEIGHT))

with open("verbs.csv") as verbsFile:
    reader = csvReader(verbsFile, delimiter="|")
    V, V_WEIGHT = (tuple(t) for t in zip(*(reader)))
    V_WEIGHT = tuple(map(int, V_WEIGHT))

with open("adverbs.csv") as adverbsFile:
    reader = csvReader(adverbsFile, delimiter="|")
    D, D_WEIGHT = (tuple(t) for t in zip(*(reader)))
    D_WEIGHT = tuple(map(int, D_WEIGHT))

with open("prepositions.csv") as prepositionsFile:
    reader = csvReader(prepositionsFile, delimiter="|")
    P, P_WEIGHT = (tuple(t) for t in zip(*(reader)))
    P_WEIGHT = tuple(map(int, P_WEIGHT))

with open("auxillaries.csv") as auxillariesFile:
    reader = csvReader(auxillariesFile, delimiter="|")
    X, X_WEIGHT = (tuple(t) for t in zip(*(reader)))
    X_WEIGHT = tuple(map(int, X_WEIGHT))

T = (".", ";", ":", "?", "!", "--")
T_WEIGHT = (50, 1, 1, 1, 1, 1)

with open("adjectives.csv") as adjectivesFile:
    reader = csvReader(adjectivesFile, delimiter="|")
    J, J_WEIGHT = (tuple(t) for t in zip(*(reader)))
    J_WEIGHT = tuple(map(int, J_WEIGHT))

def generateComment(maxLen: int) -> str:
    commentGrammer = UNIFORM_RAND.choices(
        GRAMMER, weights=GRAMMER_WEIGHT
    ).pop()

    commentGrammerOnlyWords = []
    for elem in commentGrammer:
        if elem == "NP":
            commentGrammerOnlyWords.extend(
                UNIFORM_RAND.choices(NP, weights=NP_WEIGHT).pop()
            )
        elif elem == "VP":
            commentGrammerOnlyWords.extend(
                UNIFORM_RAND.choices(VP, weights=VP_WEIGHT).pop()
            )
        else:
            commentGrammerOnlyWords.append(elem)

    wordsOfComment = []
    for elem in commentGrammerOnlyWords:
        if elem == "N":
            wordsOfComment.extend(
                UNIFORM_RAND.choices(N, weights=N_WEIGHT)
            )
        elif elem == "V":
            wordsOfComment.extend(
                UNIFORM_RAND.choices(V, weights=V_WEIGHT)
            )
        elif elem == "D":
            wordsOfComment.extend(
                UNIFORM_RAND.choices(D, weights=D_WEIGHT)
            )
        elif elem == "P":
            wordsOfComment.extend(
                UNIFORM_RAND.choices(P, weights=P_WEIGHT)
            )
        elif elem == "X":
            wordsOfComment.extend(
                UNIFORM_RAND.choices(X, weights=X_WEIGHT)
            )
        elif elem == "T":
            wordsOfComment.extend(
                UNIFORM_RAND.choices(T, weights=T_WEIGHT)
            )
        elif elem == "J":
            wordsOfComment.extend(
                UNIFORM_RAND.choices(J, weights=J_WEIGHT)
            )
        else:
            wordsOfComment.append(elem)
    comment = "".join(wordsOfComment)
    return comment[:maxLen]




# Other Constants
MKS_SEGMENTS = ("AUTOMOBILE", "BUILDING", "FURNITURE", 
                "HOUSEHOLD", "MACHINERY")
SHIPPING_INSTRUCTIONS = ("DELIVER IN PERSON", "COLLECT COD", 
                         "TAKE BACK RETURN", "NONE")
SHIPPING_MODE = ("REG AIR", "AIR", "RAIL", "TRUCK", "MAIL", "FOB", "SHIP")
ORDER_PRIORITY = ("1-URGENT","2-HIGH","3-MEDIUM", 
                  "4-NOT SPECIFIED","5-LOW")





def initializeVersion1(connStr: str):

    conn = psycopg2.connect(connStr)
    cur = conn.cursor()

    # create tables
    tableNames = ("nation", "region", "part", "supplier", 
                  "partsupp", "customer", "orders", "lineitem")
    dropAllTblSql = "drop table if exists {}".format(",".join(tableNames))
    _executeWrapper(cur, dropAllTblSql)
    createNationSql = '''
        CREATE TABLE NATION (
            N_NATIONKEY INTEGER NOT NULL,
            N_NAME CHAR(25) NOT NULL,
            N_REGIONKEY INTEGER NOT NULL,
            N_COMMENT VARCHAR(152)
        );
    '''
    createRegionSql = '''
        CREATE TABLE REGION (
            R_REGIONKEY INTEGER NOT NULL,
            R_NAME CHAR(25) NOT NULL,
            R_COMMENT VARCHAR(152)
        );
    '''
    createPartSql = '''
        CREATE TABLE PART (
            P_PARTKEY INTEGER NOT NULL,
            P_NAME VARCHAR(55) NOT NULL,
            P_MFGR CHAR(25) NOT NULL,
            P_BRAND CHAR(10) NOT NULL,
            P_TYPE VARCHAR(25) NOT NULL,
            P_SIZE INTEGER NOT NULL,
            P_CONTAINER CHAR(10) NOT NULL,
            P_RETAILPRICE DECIMAL(15, 2) NOT NULL,
            P_COMMENT VARCHAR(23) NOT NULL
        );
    '''
    createSupplierSql = '''
        CREATE TABLE SUPPLIER (
            S_SUPPKEY INTEGER NOT NULL,
            S_NAME CHAR(25) NOT NULL,
            S_ADDRESS VARCHAR(40) NOT NULL,
            S_NATIONKEY INTEGER NOT NULL,
            S_PHONE CHAR(15) NOT NULL,
            S_ACCTBAL DECIMAL(15, 2) NOT NULL,
            S_COMMENT VARCHAR(101) NOT NULL
        );
    '''
    createPartsuppSql = '''
        CREATE TABLE PARTSUPP (
            PS_PARTKEY INTEGER NOT NULL,
            PS_SUPPKEY INTEGER NOT NULL,
            PS_AVAILQTY INTEGER NOT NULL,
            PS_SUPPLYCOST DECIMAL(15, 2) NOT NULL,
            PS_COMMENT VARCHAR(199) NOT NULL
        );
    '''
    createCustomerSql = '''
        CREATE TABLE CUSTOMER (
            C_CUSTKEY INTEGER NOT NULL,
            C_NAME VARCHAR(25) NOT NULL,
            C_ADDRESS VARCHAR(40) NOT NULL,
            C_NATIONKEY INTEGER NOT NULL,
            C_PHONE CHAR(15) NOT NULL,
            C_ACCTBAL DECIMAL(15, 2) NOT NULL,
            C_MKTSEGMENT CHAR(10) NOT NULL,
            C_COMMENT VARCHAR(117) NOT NULL
        );
    '''
    createOrderSql = '''
        CREATE TABLE ORDERS (
            O_ORDERKEY INTEGER NOT NULL,
            O_CUSTKEY INTEGER NOT NULL,
            O_ORDERSTATUS CHAR(1) NOT NULL,
            O_TOTALPRICE DECIMAL(15, 2) NOT NULL,
            O_ORDERDATE DATE NOT NULL,
            O_ORDERPRIORITY CHAR(15) NOT NULL,
            O_CLERK CHAR(15) NOT NULL,
            O_SHIPPRIORITY INTEGER NOT NULL,
            O_COMMENT VARCHAR(79) NOT NULL
        );
    '''
    createLineitemSql = '''
        CREATE TABLE LINEITEM (
            L_ORDERKEY INTEGER NOT NULL,
            L_PARTKEY INTEGER NOT NULL,
            L_SUPPKEY INTEGER NOT NULL,
            L_LINENUMBER INTEGER NOT NULL,
            L_QUANTITY DECIMAL(15, 2) NOT NULL,
            L_EXTENDEDPRICE DECIMAL(15, 2) NOT NULL,
            L_DISCOUNT DECIMAL(15, 2) NOT NULL,
            L_TAX DECIMAL(15, 2) NOT NULL,
            L_RETURNFLAG CHAR(1) NOT NULL,
            L_LINESTATUS CHAR(1) NOT NULL,
            L_SHIPDATE DATE NOT NULL,
            L_COMMITDATE DATE NOT NULL,
            L_RECEIPTDATE DATE NOT NULL,
            L_SHIPINSTRUCT CHAR(25) NOT NULL,
            L_SHIPMODE CHAR(10) NOT NULL,
            L_COMMENT VARCHAR(44) NOT NULL
        );
    '''
    createTableSqls = (createNationSql, 
                       createRegionSql, 
                       createPartSql, 
                       createSupplierSql, 
                       createPartsuppSql, 
                       createCustomerSql, 
                       createOrderSql, 
                       createLineitemSql)
    for createTableSql in createTableSqls:
        _executeWrapper(cur, createTableSql)

    conn.commit()

    # insert data into tables from dbgen-generated files
    tableDir = Path(config.tpchTblPath)
    for tblName in tableNames:
        copyFromFileStr = "\copy {} from '{}' with (delimiter '|')".format(
            tblName, tableDir / (tblName + ".tbl")
        )
        runCmd([config.psqlPath, "-d", connStr, "-c", copyFromFileStr])

    # generate V1data for lineitem
    alterLineitemAddATSql = '''
        alter table lineitem 
        add column l_active_time_begin date,
        add column l_active_time_end date;
    '''
    _executeWrapper(cur, alterLineitemAddATSql)
    updatelineitemATSql = '''
        update lineitem 
        set l_active_time_begin 
            = least(l_shipdate, l_commitdate, l_receiptdate),
            l_active_time_end 
            = greatest(l_shipdate, l_commitdate, l_receiptdate);
    '''
    _executeWrapper(cur, updatelineitemATSql)

    conn.commit()

    # generate V1data for orders
    alterOrdersAddATSql = '''
        alter table orders 
        add column o_active_time_begin date,
        add column o_active_time_end date;
    '''
    alterOrdersAddRTSql = '''
        alter table orders 
        add column o_RECEIVABLE_TIME_BEGIN date,
        add column o_RECEIVABLE_TIME_END date;
    '''
    _executeWrapper(cur, alterOrdersAddATSql)
    _executeWrapper(cur, alterOrdersAddRTSql)
    updateOrdersATSql1 = '''
        update orders
        set o_active_time_begin = date '{}',
            o_active_time_end = date '{}'
    '''.format(MAX_DATE, MIN_DATE)
    updateOrdersATSql2 = '''
        update
            orders
        set
            o_active_time_begin = least(
                o_orderdate,
                o_active_time_begin,
                min_l_active_time_begin
            ),
            o_active_time_end = greatest(o_active_time_end, 
                max_l_active_time_end)
        from
            (
                select
                    l_orderkey,
                    min(l_active_time_begin) as min_l_active_time_begin,
                    max(l_active_time_end) as max_l_active_time_end
                from
                    lineitem
                group by
                    l_orderkey
            ) as agg_lineitem
        where
            o_orderkey = agg_lineitem.l_orderkey;
    '''
    _executeWrapper(cur, updateOrdersATSql1)
    _executeWrapper(cur, updateOrdersATSql2)
    selectSetseedSql = "select setseed({})".format(0.8444218515250481)
    createUniformSql = '''
        CREATE OR REPLACE FUNCTION uniform(low date ,high date) 
            RETURNS date AS
        $$
        BEGIN
            RETURN low + cast(floor(random()* (high-low + 1)) as int);
        END;
        $$ language 'plpgsql' STRICT
        '''
    updateOrdersReceTimeSql1 = '''
        update
            orders
        set
            o_RECEIVABLE_TIME_BEGIN = uniform(o_active_time_begin, 
                o_active_time_end);
    '''
    updateOrdersReceTimeSql2 = '''
        update
            orders
        set
            o_RECEIVABLE_TIME_end = uniform(o_RECEIVABLE_TIME_BEGIN, 
                o_active_time_end);
    '''
    dropUniformSql = "drop function if exists uniform;"
    _executeWrapper(cur, selectSetseedSql)
    _executeWrapper(cur, createUniformSql)
    _executeWrapper(cur, updateOrdersReceTimeSql1)
    _executeWrapper(cur, updateOrdersReceTimeSql2)
    _executeWrapper(cur, dropUniformSql)
    
    conn.commit()

    # generate V1data for customer
    alterCustomerAddATSql = '''
        alter table customer
        add column c_active_time_begin date,
        add column c_active_time_end date
    '''
    _executeWrapper(cur, alterCustomerAddATSql)
    updateCustomerATSql1 = '''
        update customer 
        set c_active_time_begin = date '{}', 
            c_active_time_end = date '{}'
    '''.format(MAX_DATE,MAX_DATE)
    updateCustomerATSql2 = '''
        update
            customer
        set
            c_active_time_begin = least(
                c_active_time_begin,
                agg_orders.min_o_active_time_begin
            )
        from
            (
                select
                    o_custkey,
                    min(o_active_time_begin) as min_o_active_time_begin
                from
                    orders
                group by
                    o_custkey
            ) as agg_orders
        where
            c_custkey = agg_orders.o_custkey;
    '''
    _executeWrapper(cur, updateCustomerATSql1)
    _executeWrapper(cur, updateCustomerATSql2)

    conn.commit()

    # generate V1data for part
    alterPartAddAvailTimeSql = '''
        alter table part
        add column p_availablity_time_begin date,
        add column p_availablity_time_end date
        '''
    _executeWrapper(cur, alterPartAddAvailTimeSql)
    updatePartAvailTimeSql1 = '''
        update part 
        set p_availablity_time_begin = date '{}', 
            p_availablity_time_end = date '{}'
        '''.format(MAX_DATE,MAX_DATE)
    updatePartAvailTimeSql2 = '''
        update
            part
        set
            p_availablity_time_begin = least(
                p_availablity_time_begin,
                agg_lineitem.min_l_active_time_begin
            )
        from
            (
                select
                    l_partkey,
                    min(l_active_time_begin) as min_l_active_time_begin
                from
                    lineitem
                group by
                    l_partkey
            ) as agg_lineitem
        where
            p_partkey = agg_lineitem.l_partkey;
    '''
    _executeWrapper(cur, updatePartAvailTimeSql1)
    _executeWrapper(cur, updatePartAvailTimeSql2)

    conn.commit()

    # generate V1data for partsupp
    alterPartsuppAddVTSql = '''
        alter table partsupp
        add column ps_validity_time_begin date,
        add column ps_validity_time_end date;
    '''
    _executeWrapper(cur, alterPartsuppAddVTSql)
    updatePartsuppVTSql = '''
        update partsupp
        set ps_validity_time_begin = p_availablity_time_begin, 
            ps_validity_time_end = date '{}'
        from part
        where p_partkey = ps_partkey;
    '''.format(MAX_DATE)
    _executeWrapper(cur, updatePartsuppVTSql)

    conn.commit()

    cur.close()
    conn.close()

    # output data into files
    destPath = Path(config.destPath)
    for i in range(len(tableNames)):
        copyToFileStr = "\copy {} to '{}' with (delimiter '|')".format(
            tableNames[i], 
            destPath / ("bi-" + tableNames[i] + ".tbl")
        )
        runCmd(
            [config.psqlPath, "-d", connStr, "-c", copyToFileStr], 
            bufsize=8192
        )





def generataHistory(connStr: str):
    conn = psycopg2.connect(connStr)
    cur = conn.cursor()

    oneDay = timedelta(days=1)
    avgUTPerDay = config.updateTimes / (TO_DATE - FROM_DATE).days
    currentUT = avgUTPerDay
    totalUT = 0

    # create index on primary key of part, supplier, partsupp, 
    # customer, orders, lineitem
    createPartPKSql = '''
        create index if not exists part_pk on part 
            (p_partkey)
    '''
    createSupplierPKSql = '''
        create index if not exists supplier_pk on supplier 
            (s_suppkey)
    '''
    createPartsuppPKSql = '''
        create index if not exists partsupp_pk on partsupp 
            (ps_partkey, ps_suppkey)
    '''
    createCustomerPKSql = '''
        create index if not exists customer_pk on customer 
            (c_custkey)
    '''
    createOrdersPKSql = '''
        create index if not exists orders_pk on orders 
            (o_orderkey)
    '''
    createLineitemPKSql = '''
        create index if not exists lineitem_pk on lineitem 
            (l_orderkey, l_partkey, l_suppkey)
    '''
    createPKSqls = (createPartPKSql, 
                    createSupplierPKSql, 
                    createPartsuppPKSql, 
                    createCustomerPKSql, 
                    createOrdersPKSql, 
                    createLineitemPKSql)
    for createPKSql in createPKSqls:
        _executeWrapper(cur, createPKSql)

    conn.commit()

    # Get max and min c_custkey
    selectMinMaxCustkeySql = '''
        select min(c_custkey), max(c_custkey)
        from customer;
    '''
    _executeWrapper(cur, selectMinMaxCustkeySql)
    minCustkey, maxCustkey = cur.fetchone()

    conn.commit()

    # Get max and min n_nationkey
    selectMinMaxNationkeySql = '''
        select min(n_nationkey), max(n_nationkey) 
        from nation
    '''
    _executeWrapper(cur, selectMinMaxNationkeySql)
    minNationkey, maxNationkey = cur.fetchone()

    conn.commit()

    # Get max o_orderkey
    selectMaxOrderekySql = "select max(o_orderkey) from orders"
    _executeWrapper(cur, selectMaxOrderekySql)
    maxOrderkey = cur.fetchone()[0]

    conn.commit()

    # Get max and min p_partkey
    selectMinMaxPartkeySql = '''
        select min(p_partkey), max(p_partkey) 
        from part
    '''
    _executeWrapper(cur, selectMinMaxPartkeySql)
    minPartkey, maxPartkey = cur.fetchone()

    conn.commit()

    # create btree index on o_receivable_time to reduce cost from 
    # "Uniformly select orders still being opened in `currentTime`"
    createReceivableTimeBTreeIndexSql = '''
        create index if not exists o_receivable_time_btree on orders 
            (o_receivable_time_begin, o_receivable_time_end)
    '''
    _executeWrapper(cur, createReceivableTimeBTreeIndexSql)

    conn.commit()

    # Get max and min s_suppkey
    selectMinMaxSuppkeySql = "select min(s_suppkey), max(s_suppkey) from supplier"
    _executeWrapper(cur, selectMinMaxSuppkeySql)
    minSuppkey, maxSuppkey = cur.fetchone()

    conn.commit()

    # TODO:p_availablity_time_btree for # Update stock
    # create btree index on p_availablity_time to reduce cost from 
    # "Uniformly select lineitem with 'O' status and related part 
    # is available"
    createAvailablityTimeBTreeIndexSql = '''
        create index if not exists p_availablity_time_btree on part
            (p_availablity_time_begin, p_availablity_time_end);
    '''
    _executeWrapper(cur, createAvailablityTimeBTreeIndexSql)

    conn.commit()

    historySqlFile = (Path(config.destPath) / "history.sql").open(
        mode="+w", buffering=1048576
    )

    currentTime = FROM_DATE
    while totalUT < config.updateTimes:
        if UPDATE_P_RAND.random() < currentUT:
            p = UPDATE_SCENARIO_P_RAND.random()

            # New order
            if 0 <= p < 0.3: 
                newOrderHistorySqls = []

                # Update customer data
                if 0 <= p < 0.075: 
                    c_custkey = UNIFORM_RAND.randint(minCustkey, maxCustkey)
                    c_active_time_begin = currentTime + UNIFORM_RAND.randint(1,30) * oneDay
                    c_active_time_end = MAX_DATE
                    c_nationkey = UNIFORM_RAND.randint(minNationkey, maxNationkey)
                    c_address = generateAddress()
                    c_phone = generatePhone(c_nationkey)
                    updateCustomerDataSql = '''
                        update customer
                        set c_active_time_begin = date '{}',
                            c_active_time_end = date '{}',
                            c_nationkey = {},
                            c_address = '{}',
                            c_phone = '{}'
                        where c_custkey = {};
                    '''.format(
                        c_active_time_begin, 
                        c_active_time_end, 
                        c_nationkey,  
                        c_address, 
                        c_phone, 
                        c_custkey
                    )
                    _executeWrapper(cur, updateCustomerDataSql)
                    newOrderHistorySqls.append(updateCustomerDataSql)

                    
                # Do not change customer data
                elif 0.075 <= p < 0.15:
                    c_custkey = UNIFORM_RAND.randint(minCustkey, maxCustkey)
                
                # Insert new customer
                else: # 0.15 <= p < 0.3
                    maxCustkey += 1
                    c_custkey = maxCustkey
                    c_name = "Customer#{:09d}".format(c_custkey)
                    c_address = generateAddress()
                    c_nationkey = UNIFORM_RAND.randint(minNationkey, maxNationkey)
                    c_phone = generatePhone(c_nationkey)
                    c_acctbal = 0
                    c_mktSegment = UNIFORM_RAND.choice(MKS_SEGMENTS)
                    c_comment = generateComment(117)
                    c_active_time_begin = currentTime
                    c_active_time_end = MAX_DATE
                    insertNewCustomerSql = '''
                        insert into
                            customer
                        values
                        (
                                {},
                                '{}',
                                '{}',
                                {},
                                '{}',
                                {},
                                '{}',
                                '{}',
                                date '{}',
                                date '{}'
                            );
                    '''.format(c_custkey, 
                               c_name, 
                               c_address, 
                               c_nationkey, 
                               c_phone, 
                               c_acctbal, 
                               c_mktSegment, 
                               c_comment, 
                               c_active_time_begin, 
                               c_active_time_end)
                    _executeWrapper(cur, insertNewCustomerSql)
                    newOrderHistorySqls.append(insertNewCustomerSql)
                
                # Insert new order
                maxOrderkey += 1
                o_orderkey = maxOrderkey
                o_custkey = c_custkey
                o_orderstatus = "O"
                o_totalprice = 0

                # Insert lineitem
                itemNum = UNIFORM_RAND.randint(1,7)
                for l_linenumber in range(itemNum):
                    l_orderkey = o_orderkey
                    l_partkey = UNIFORM_RAND.randint(minPartkey, maxPartkey)

                    # Randomly choose one supplier of part with "l_partkey"
                    selectSuppkeySql = '''
                        select ps_suppkey 
                        from partsupp 
                        where ps_partkey = {}
                    '''.format(l_partkey)
                    _executeWrapper(cur, selectSuppkeySql)
                    l_suppkey = UNIFORM_RAND.choice(cur.fetchall())[0]

                    l_quantity = UNIFORM_RAND.randint(1,50)
                    l_discount = UNIFORM_RAND.randint(0, 10) / 100
                    l_tax = UNIFORM_RAND.randint(0, 8) / 100
                    l_extendedprice = (90000 
                                       + ((l_partkey / 10) % 20001) 
                                       + ((l_partkey % 1000) * 100)) * l_quantity
                    l_shipdate = currentTime
                    l_commitdate = l_shipdate + UNIFORM_RAND.randint(
                        0,(TO_DATE - l_shipdate).days
                    ) * oneDay
                    l_receiptdate = MAX_DATE
                    l_returnflag = "N"
                    l_linestatus = "O"
                    l_shipinstruct = UNIFORM_RAND.choice(SHIPPING_INSTRUCTIONS)
                    l_shipmode = UNIFORM_RAND.choice(SHIPPING_MODE)
                    l_comment = generateComment(44)
                    l_active_time_begin = currentTime
                    l_active_time_end = MAX_DATE
                    insertLineitemSql = '''
                        insert into
                            lineitem
                        values
                            (
                                {},
                                {},
                                {},
                                {},
                                {},
                                {},
                                {},
                                {},
                                '{}',
                                '{}',
                                date '{}',
                                date '{}',
                                date '{}',
                                '{}',
                                '{}',
                                '{}',
                                date '{}',
                                date '{}'
                            );
                    '''.format(l_orderkey, 
                               l_partkey, 
                               l_suppkey, 
                               l_linenumber, 
                               l_quantity, 
                               l_extendedprice, 
                               l_discount, 
                               l_tax, 
                               l_returnflag, 
                               l_linestatus, 
                               l_shipdate, 
                               l_commitdate, 
                               l_receiptdate, 
                               l_shipinstruct, 
                               l_shipmode, 
                               l_comment,
                               l_active_time_begin, 
                               l_active_time_end)
                    o_totalprice += l_extendedprice * (1 - l_discount) * (1 + l_tax)
                    _executeWrapper(cur, insertLineitemSql)
                    newOrderHistorySqls.append(insertLineitemSql)

                o_orderdate = currentTime
                o_orderpriority = UNIFORM_RAND.choice(ORDER_PRIORITY)
                o_clerk = "clerk#{:09d}".format(UNIFORM_RAND.randint(1, 1000))
                o_shippriority = 0
                o_comment = generateComment(79)
                o_active_time_begin = currentTime
                o_active_time_end = MAX_DATE
                o_receivable_time_begin = currentTime + UNIFORM_RAND.randint(1, 14) * oneDay
                o_receivable_time_end = MAX_DATE
                insertNewOrderSql = '''
                    insert into
                        orders
                    values
                        (
                            {},
                            {},
                            '{}',
                            {},
                            date '{}',
                            '{}',
                            '{}',
                            {},
                            '{}',
                            date '{}',
                            date '{}',
                            date '{}',
                            date '{}'
                        );
                '''.format(o_orderkey, 
                           o_custkey, 
                           o_orderstatus,
                           o_totalprice, 
                           o_orderdate, 
                           o_orderpriority,
                           o_clerk, 
                           o_shippriority, 
                           o_comment,
                           o_active_time_begin, 
                           o_active_time_end,
                           o_receivable_time_begin, 
                           o_receivable_time_end)
                _executeWrapper(cur, insertNewOrderSql)
                newOrderHistorySqls.append(insertNewOrderSql)
                
                conn.commit()
                historySqlFile.write("\n")
                historySqlFile.write("\n".join(newOrderHistorySqls))
                historySqlFile.write("\n")

            # Cancel order
            elif 0.3 <= p < 0.4:
                cancelOrderHistorySql = []

                # Uniformly select one order with non 'F' status
                selectCancelOrderSql = '''
                    select o_orderkey, o_orderstatus, o_custkey, o_totalprice
                    from orders tablesample bernoulli (100) repeatable ({})
                    where o_orderstatus != 'F'
                    limit 1
                '''.format(TABLE_SAMPLE_SEED_RAND.random())
                _executeWrapper(cur, selectCancelOrderSql)
                o_orderkey, o_orderstatus, o_custkey, o_totalprice = cur.fetchone()
                if o_orderstatus == "P":
                    updateCustomerAcctbalSql = '''
                        update customer 
                        set c_acctbal = c_acctbal + {} 
                        where c_custkey = {};
                    '''.format(o_totalprice, o_custkey)
                    _executeWrapper(cur, updateCustomerAcctbalSql)
                    cancelOrderHistorySql.append(updateCustomerAcctbalSql)
                l_orderkey = o_orderkey
                selectFLineitemPKsSql = '''
                    select l_partkey, l_suppkey, l_quantity
                    from lineitem
                    where l_orderkey = {} and l_linestatus = 'F';
                '''.format(l_orderkey)
                _executeWrapper(cur, selectFLineitemPKsSql)
                for l_partkey, l_suppkey, l_quantity in cur.fetchall():
                    updatePartsuppAvailqtySql = '''
                        update partsupp
                        set ps_availqty = ps_availqty + {}
                        where ps_partkey = {} and ps_suppkey = {};
                    '''.format(l_quantity, l_partkey, l_suppkey)
                    deleteFromLineitemSql = '''
                        delete from lineitem
                        where l_orderkey = {}
                            and l_partkey = {}
                            and l_suppkey = {};
                    '''.format(l_orderkey, l_partkey, l_suppkey)
                    _executeWrapper(cur, updatePartsuppAvailqtySql)
                    cancelOrderHistorySql.append(updatePartsuppAvailqtySql)
                    _executeWrapper(cur, deleteFromLineitemSql)
                    cancelOrderHistorySql.append(deleteFromLineitemSql)
                deleteFromOrdersSql = '''
                    delete from orders 
                    where o_orderkey = {};
                '''.format(o_orderkey)
                _executeWrapper(cur, deleteFromOrdersSql)
                cancelOrderHistorySql.append(deleteFromOrdersSql)

                conn.commit()
                historySqlFile.write("\n")
                historySqlFile.write("\n".join(cancelOrderHistorySql))
                historySqlFile.write("\n")

            # Deliver order
            elif 0.4 <= p < 0.6:
                deliverOrderHistorySqls = []

                # Here we use as the same sampling strategy as we do 
                # in "Uniformly select one order with non 'F' status"
                SelectDeliverOrderSqlTemplate = '''
                    select o_orderkey, 
                        o_orderstatus, 
                        o_totalprice, 
                        o_custkey, 
                        o_receivable_time_begin, 
                        o_receivable_time_end
                    from customer inner join (
                        select o_orderkey, 
                            o_orderstatus, 
                            o_totalprice, 
                            o_custkey, 
                            o_receivable_time_begin, 
                            o_receivable_time_end
                        from orders tablesample bernoulli (100) repeatable ({})
                        limit 1000
                    ) as sampled_orders on o_custkey = c_custkey
                    where (o_orderstatus = 'O' and c_acctbal - o_totalprice >= 0)
                        or (o_orderstatus = 'P')
                '''
                orderRow = ()
                while (len(orderRow) == 0):
                    SelectDeliverOrderSql = SelectDeliverOrderSqlTemplate.format(
                        TABLE_SAMPLE_SEED_RAND.random()
                    )
                    _executeWrapper(cur, SelectDeliverOrderSql)
                    orderRow = UNIFORM_RAND.choice(cur.fetchall())
                o_orderkey, o_orderstatus, o_totalprice, o_custkey, o_receivable_time_begin, o_receivable_time_end = orderRow
                if o_orderstatus == 'O':
                    updateCustomerAcctbalSql = '''
                        update customer
                        set c_acctbal = c_acctbal - {}
                        where c_custkey = {};
                    '''.format(o_totalprice, o_custkey)
                    updateOrderStatusSql = '''
                        update orders
                        set o_orderstatus = 'P'
                        where o_orderkey = {};
                    '''.format(o_orderkey)
                    _executeWrapper(cur, updateCustomerAcctbalSql)
                    deliverOrderHistorySqls.append(updateCustomerAcctbalSql)
                    _executeWrapper(cur, updateOrderStatusSql)
                    deliverOrderHistorySqls.append(updateOrderStatusSql)

                l_orderkey = o_orderkey
                selectFromLineitemSql = '''
                    select l_partkey, l_suppkey, l_linestatus, l_quantity
                    from lineitem
                    where l_orderkey = {};
                '''.format(l_orderkey)
                _executeWrapper(cur, selectFromLineitemSql)
                lineitems = cur.fetchall()
                isAllFStatus = True
                for l_partkey, l_suppkey, l_linestatus, l_quantity in lineitems:
                    isAllFStatus = isAllFStatus and (l_linestatus == 'F')
                    isConditionTrue = (l_linestatus == 'O')
                    if not isConditionTrue:
                        continue
                    checkQTYConditionSql = '''
                        select (ps_availqty - {}) > 0
                        from partsupp
                        where ps_partkey = {} and ps_suppkey = {};
                    '''.format(l_quantity, l_partkey, l_suppkey)
                    _executeWrapper(cur, checkQTYConditionSql)
                    isConditionTrue = isConditionTrue and (cur.fetchone())[0]
                    if not isConditionTrue:
                        continue
                    checkAvailTimeConditionSql = '''
                        select (date '{}' >= p_availablity_time_begin)
                            and (date '{}' <= p_availablity_time_end)
                        from part
                        where p_partkey = {};
                    '''.format(currentTime, currentTime, l_partkey)
                    _executeWrapper(cur, checkAvailTimeConditionSql)
                    isConditionTrue = isConditionTrue and (cur.fetchone())[0]
                    if isConditionTrue:
                        updatePartsuppSql = '''
                            update partsupp
                            set ps_availqty = ps_availqty - {}
                            where ps_partkey = {} and ps_suppkey = {};
                        '''.format(l_quantity, l_partkey, l_suppkey)
                        updatelineitemSql = '''
                            update lineitem
                            set l_linestatus = 'F',
                                l_active_time_end = date '{}'
                            where l_orderkey = {}
                                and l_partkey = {}
                                and l_suppkey = {};
                        '''.format(
                            currentTime,
                            l_orderkey,
                            l_partkey,
                            l_suppkey
                        )
                        _executeWrapper(cur, updatePartsuppSql)
                        deliverOrderHistorySqls.append(updatePartsuppSql)
                        _executeWrapper(cur, updatelineitemSql)
                        deliverOrderHistorySqls.append(updatelineitemSql)
                if (currentTime >= o_receivable_time_begin 
                    and currentTime <= o_receivable_time_end 
                    and isAllFStatus):
                    if o_receivable_time_end == MAX_DATE:
                        updateOrdersSql = '''
                            update orders
                            set o_active_time_end = date '{}',
                                o_receivable_time_end = date '{}'
                            where o_orderkey = {};
                        '''.format(currentTime, currentTime, o_orderkey)
                    else:
                        updateOrdersSql = '''
                            update orders
                            set o_active_time_end = date '{}',
                            where o_orderkey = {};
                        '''.format(currentTime, o_orderkey)
                    _executeWrapper(cur, updateOrdersSql)
                    deliverOrderHistorySqls.append(updateOrdersSql)

                conn.commit()
                historySqlFile.write("\n")
                historySqlFile.write("\n".join(deliverOrderHistorySqls))
                historySqlFile.write("\n")

            # Receive payment
            elif 0.6 <= p < 0.8:
                receivePaymentHistorySqls = []

                # Uniformly select orders still being opened in `currentTime`
                selectFromOrdersSql = '''
                    select o_orderkey, o_totalprice, o_custkey
                    from orders tablesample bernoulli (100) repeatable ({})
                    where o_receivable_time_begin <= date '{}'
                        and o_receivable_time_end >= date '{}'
                        and o_orderstatus = 'O'
                    limit 1
                '''.format(TABLE_SAMPLE_SEED_RAND.random(),
                           currentTime,
                           currentTime)
                _executeWrapper(cur, selectFromOrdersSql)
                allReceOrders = cur.fetchall()
                if len(allReceOrders) > 0:
                    o_orderkey, o_totalprice, o_custkey = allReceOrders.pop()
                    updateOrdersReceTimeSql = '''
                        update orders
                        set o_receivable_time_end = date '{}'
                        where o_orderkey = {};
                    '''.format(currentTime, o_orderkey)
                    updateCustomerSql = '''
                        update customer
                        set c_acctbal = c_acctbal + {}
                        where c_custkey = {};
                    '''.format(o_totalprice, o_custkey)
                    _executeWrapper(cur, updateOrdersReceTimeSql)
                    receivePaymentHistorySqls.append(updateOrdersReceTimeSql)
                    _executeWrapper(cur, updateCustomerSql)
                    receivePaymentHistorySqls.append(updateCustomerSql)
            
                conn.commit()
                historySqlFile.write("\n")
                historySqlFile.write("\n".join(receivePaymentHistorySqls))
                historySqlFile.write("\n")

            # Update stock
            elif 0.8 <= p < 0.85:
                # Uniformly select lineitem with 'O' status and related 
                # part is available
                selectUpdateStockSqlTemplate = '''
                    select l_partkey, l_suppkey, l_quantity
                    from (
                        select l_partkey, l_suppkey, l_quantity
                        from lineitem tablesample bernoulli (100) repeatable ({})
                        where l_linestatus = 'O'
                        limit 1000
                    ) as opened_lineitem inner join part
                        on l_partkey = p_partkey
                    where p_availablity_time_begin <= date '{}'
                        and p_availablity_time_end >= date '{}'
                '''
                updateStockLineitems = []
                while (len(updateStockLineitems) == 0):
                    selectUpdateStockSql = selectUpdateStockSqlTemplate.format(
                        TABLE_SAMPLE_SEED_RAND.random(),
                        currentTime,
                        currentTime
                    )
                    _executeWrapper(cur, selectUpdateStockSql)
                    updateStockLineitems = cur.fetchall()
                l_partkey, l_suppkey, l_quantity = UNIFORM_RAND.choice(
                    updateStockLineitems
                )
                updatePartsuppQTYSql = '''
                    update partsupp
                    set ps_availqty = ps_availqty + 2 * {}
                    where ps_partkey = {} and ps_suppkey = {};
                '''.format(l_quantity, l_partkey, l_suppkey)
                _executeWrapper(cur, updatePartsuppQTYSql)

                conn.commit()
                historySqlFile.write("\n")
                historySqlFile.write(updatePartsuppQTYSql)
                historySqlFile.write("\n")

            # Delay availablity
            elif 0.85 <= p < 0.9:
                p_partkey = UNIFORM_RAND.randint(minPartkey, maxPartkey)
                updatePartAvailTimeSql = '''
                    update part
                    set p_availablity_time_begin = date '{}',
                        p_availablity_time_end = date '{}'
                    where p_partkey = {};
                '''.format(currentTime + UNIFORM_RAND.randint(1, 14) * oneDay,
                           MAX_DATE,
                           p_partkey)
                _executeWrapper(cur, updatePartAvailTimeSql)

                conn.commit()
                historySqlFile.write("\n")
                historySqlFile.write(updatePartAvailTimeSql)
                historySqlFile.write("\n")

            # Change price by supplier
            elif 0.9 <= p < 0.95:
                selectChangePricePartsuppSqlSql = '''
                    select ps_partkey, ps_suppkey
                    from partsupp tablesample bernoulli (100) repeatable ({})
                    limit 1
                '''.format(TABLE_SAMPLE_SEED_RAND.random())
                _executeWrapper(cur, selectChangePricePartsuppSqlSql)
                ps_partkey, ps_suppkey = cur.fetchone()
                updatePartsuppSql = '''
                    update partsupp
                    set ps_supplycost = abs(ps_supplycost + ({})),
                        ps_validity_time_begin = date '{}',
                        ps_validity_time_end = date '{}'
                    where ps_partkey = {} and ps_suppkey = {};
                '''.format(
                    UNIFORM_RAND.randint(-100, 100),
                    currentTime + int(UNIFORM_RAND.gauss((-15 + 30)/2, 1.0)) * oneDay,
                    MAX_DATE,
                    ps_partkey,
                    ps_suppkey
                )
                _executeWrapper(cur, updatePartsuppSql)

                conn.commit()
                historySqlFile.write("\n")
                historySqlFile.write(updatePartsuppSql)
                historySqlFile.write("\n")

            # Update supplier
            elif 0.95 <= p < 0.999:
                s_suppkey = UNIFORM_RAND.randint(minSuppkey, maxSuppkey)
                updateSupplierAcctbalSql= '''
                    update supplier
                    set s_acctbal = abs(s_acctbal + ({}))
                    where s_suppkey = {};
                '''.format(UNIFORM_RAND.randint(-100, 100),s_suppkey)
                _executeWrapper(cur, updateSupplierAcctbalSql)

                conn.commit()
                historySqlFile.write("\n")
                historySqlFile.write(updateSupplierAcctbalSql)
                historySqlFile.write("\n")

            # Manipulate order data
            else:
                manOrderDataHistorySqls = []

                o_totalprice = 0
                selectManOrderSql = '''
                    select o_orderkey
                    from orders tablesample bernoulli (100) repeatable ({})
                    where date '{}' > o_receivable_time_end + interval '1 month'
                        and o_orderstatus = 'F'
                    limit 1
                '''.format(TABLE_SAMPLE_SEED_RAND.random(), currentTime)
                _executeWrapper(cur, selectManOrderSql)
                o_orderkey = cur.fetchone()[0]
                selectFromLineitemSql = '''
                    select l_orderkey, l_partkey, l_suppkey, l_extendedprice
                    from lineitem
                    where l_orderkey = {};
                '''.format(o_orderkey)
                _executeWrapper(cur, selectFromLineitemSql)
                lineitems = cur.fetchall()
                for l_orderkey, l_partkey, l_suppkey, l_extendedprice in lineitems:
                    l_extendedprice = l_extendedprice + UNIFORM_RAND.randint(1, 10)
                    updateLineitemExtPriceSql = '''
                        update lineitem
                        set l_extendedprice = {}
                        where l_orderkey = {}
                            and l_partkey = {}
                            and l_suppkey = {};
                    '''.format(l_extendedprice, l_orderkey, l_partkey, l_suppkey)
                    _executeWrapper(cur, updateLineitemExtPriceSql)
                    manOrderDataHistorySqls.append(updateLineitemExtPriceSql)
                    o_totalprice += l_extendedprice
                updateOrderTotPriceSql = '''
                    update orders 
                    set o_totalprice = {} 
                    where o_orderkey = {};
                '''.format(o_totalprice, o_orderkey)
                _executeWrapper(cur, updateOrderTotPriceSql)
                manOrderDataHistorySqls.append(updateOrderTotPriceSql)

                conn.commit()
                historySqlFile.write("\n")
                historySqlFile.write("\n".join(manOrderDataHistorySqls))
                historySqlFile.write("\n")

            currentUT -= 1
            totalUT += 1
        else:
            currentUT += avgUTPerDay
            currentTime += oneDay

    historySqlFile.flush()
    historySqlFile.close()

    # drop index on primary key of part, supplier, partsupp, customer, 
    # orders, lineitem
    dropPartPKSql = "drop index if exists part_pk"
    dropSupplierPKSql = "drop index if exists supplier_pk"
    dropPartsuppPKSql = "drop index if exists partsupp_pk"
    dropCustomerPKSql = "drop index if exists customer_pk"
    dropOrdersPKSql = "drop index if exists orders_pk"
    dropLineitemPKSql = "drop index if exists lineitem_pk"
    dropPKSqls = [dropPartPKSql,
                  dropSupplierPKSql, 
                  dropPartsuppPKSql, 
                  dropCustomerPKSql, 
                  dropOrdersPKSql, 
                  dropLineitemPKSql]
    for dropPKSql in dropPKSqls:
        _executeWrapper(cur, dropPKSql)

    conn.commit()

    # drop btree index on o_receivable_time
    dropReceivableTimeBTreeIndexSql = "drop index if exists o_receivable_time_btree"
    _executeWrapper(cur, dropReceivableTimeBTreeIndexSql)

    conn.commit()

    # drop btree index on p_availablity_time 
    dropAvailablityTimeBTreeIndexSql = "drop index if exists p_availablity_time_btree"
    _executeWrapper(cur, dropAvailablityTimeBTreeIndexSql)

    conn.commit()

    conn.commit()

    cur.close()
    conn.close()





if __name__ == '__main__':
    config.config()

    connStr = "dbname={} user={} password={} host={} port={}".format(
        config.dbname, 
        config.user, 
        config.password, 
        config.host, 
        config.port
    )

    LOG.info("RUN START AT {}".format(datetime.now()))

    LOG.info("Start generating v1data")
    initializeVersion1(connStr)
    LOG.info("Success! V1 data have been generated.")

    if (not config.v1Only):
        LOG.info("Start generating history")
        generataHistory(connStr)
        LOG.info("Success! History have been generated.")
