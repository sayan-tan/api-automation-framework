import sys

import psycopg2
import re
import logging as logger
from  config import read_from_config_files

# sys.path.append('Python-APIAutomation/config')


logger = logger.getLogger(__name__)
class DatabaseConnector:
    '''
    This is utility procs for database testing.
    List of utility functions -
    -------------------------------------------
    1. open_configuration_file
    2. getDBType
    3. getDatabaseConnection
    4. closeDatabaseConnection
    5. executeSQLStatementWithoutFetchingResult
    6. executeSQLStatement
    7. printResults
    8. getColumnsofTable
    9. checkTableExists
    10.checkColumnExistsInTable
    11.checkDatabaseExist
    12.checkSingleTableSize
    13.checkImpactOnSingleTableSizeAfterPost
    14.getColumnInfo
    15.checkIfEntryIsUpdated
    16.verifyColumnNamesOfTable
    17.getPrimaryKeyOfAllTables
    18.getPrimaryKeyOfTable
    19.getColumnDatatype
    20.getTablesOfPublicSchema
    21.addColumnIntoTable
    22.deleteTableFromDatabase
    23.createTable
    24.getSchemasInPostrgressServer
    25.addDataToAllColumn
    26.fetchData
    27fetchDataInnerJoin
    28.fetchDataLeftOuterJoin
    29.fetchDataRightOuterJoin
    30.fetchDataFullOuterJoin
    31.updateRowData
    32.getDataOrderby
    33.selectDistinct
    34.getMinData
    35.getMaxData
    36.getCountData
    37.getAvgData
    38.getSumData
    39.getLikeData
    40.getBetweenData
    41.getNotNullData
    42.getNullData
    43.getDataByUnion
    44.getDataByUnionAll
    45.getSqlCaseData
    46.getDataWithSqlComparisonOperator
    47.createDatabaseBackup // use this method before using any delete operation.
    48.
    49.
    50.
    '''
    Dbconfig = read_from_config_files('database.ini', 'database')
    postgresqlConfig = read_from_config_files('database.ini', 'postgresql')

    def open_configuration_file(self):
        return open("test_data")

    def getDBType(self):
        return self.Dbconfig['databasetype']

    def getDatabaseConnection(self):
        if self.getDBType()=='postgresql':
            conn = psycopg2.connect(
                host=self.postgresqlConfig['host'],
                database=self.postgresqlConfig['database'],
                user=self.postgresqlConfig['user'],
                password=self.postgresqlConfig['password'])
            return conn

    def closeDatabaseConnection(self):
        conn = self.getDatabaseConnection()
        conn.close()

    def executeSQLStatementWithoutFetchingResult(self,query):
        conn=self.getDatabaseConnection()
        cursor=conn.cursor()
        cursor.execute(query)
        conn.commit()
        return conn

    def executeSQLStatement(self,query,rows):
        conn=self.getDatabaseConnection()
        cursor=conn.cursor()
        cursor.execute(query)
        if(rows=='all'):
            results = cursor.fetchall()
            return results
        elif(rows=='one'):
            results=cursor.fetchone()
            return results
        else:
            print("specify rows value as  all or one")

    # this is for debugging purpose
    def printResults(self,query,rows):
        print(self.executeSQLStatement(query,rows))

    def getColumnsofTable(self,tableName):
        query="Select column_name from information_schema.columns where table_name='"+tableName+"'"
        results=self.executeSQLStatement(query,'all')
        return results

    def checkTableExists(self,tableName,schemaName):
        query="SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = '"+schemaName+"' AND tablename  = '"+tableName+"');"
        result=self.executeSQLStatement(query,'all')
        return result[0][0]

    def checkColumnExistsInTable(self,tableName,columnName):
        columnsList=self.getColumnsofTable(tableName)
        for column in columnsList:
            if (column[0] == columnName):
                print('Column :'+columnName +' exixts in table :'+tableName)
                return True

    def checkDatabaseExist(self,databaseName):
        '''
        :param databaseName: str
        :return: Bool

        This function checks wether particular database exist in postgress server or not
        '''
        check_database_query = "SELECT datname from pg_database where datname='"+databaseName+"';"
        check_database = self.executeSQLStatement(check_database_query,'all')
        for database in check_database[0]:
            if database == databaseName:
                return True
            else:
                return False

    def checkSingleTableSize(self,tableName):
        '''
        :param = tableName: str
        :return: int
        This function returns total db size for a particular table
        '''
        query = "SELECT count(*) FROM "+tableName+";"
        result=self.executeSQLStatement(query,'all')
        return result[0][0]

    # this needs to be modified
    def checkImpactOnSingleTableSizeAfterPost(self,tableName,API):
        '''
        :param tableName: str
        :param API:
        :return: Bool
        This function checks whether api post operation had any affect on Db size or not
        This function takes 2 parameters ( tablename and API), it will check dbsize before post operation and after post operation, if the db size increases it will return True else False
        '''
        dbsize_before_post = self.checkSingleTableSize(self,tableName)
        #api hit
        #
        dbsize_after_post = self.checkSingleTableSize(self,tableName)
        if (dbsize_after_post - dbsize_before_post) > 0:
            return True
        else:
            return False

    def getColumnInfo(self,tableName):
        '''
        :param tableName: str
        :return: list

        This function returns all attribute names in a table
        '''
        attribute_list = []
        attribute_info = self.executeSQLStatement('SELECT column_name FROM information_schema.'+'"columns"'+"WHERE"+'"table_name"='+"'"+tableName+"'",'all')
        for i in attribute_info:
            attribute_list.append(i[0])
        return attribute_list

    def checkIfEntryIsUpdated(self,entry,tableName):
        '''
        :param entry: list , entry list should be defined in sch a way that data type of each entry can be checked internally
        :param tableName: str
        :return: BOOL : if entry exist fucntion will return true else false

        This function checks wether a particular entry is present in the database or not
        '''
        table_attribute_list = self.getColumnInfo(tableName)
        check_entry_query = "Select * from "+tableName+" Where "
        for i in range(len(table_attribute_list)):
            if i != len(table_attribute_list)-1:
                if type(entry[i]) == int:
                    check_entry_query = check_entry_query + table_attribute_list[i] + "=" +str(entry[i]) + " and "
                elif type(entry[i]) == str:
                    check_entry_query = check_entry_query + table_attribute_list[i] + "=" + "'" + entry[i] + "'" + " and "
            else:
                if type(entry[i]) == int:
                    check_entry_query = check_entry_query + table_attribute_list[i] + "=" +str(entry[i])
                elif type(entry[i]) == str:
                    check_entry_query = check_entry_query + table_attribute_list[i] + "=" + "'" + entry[i] + "'"
        check_entry = self.executeSQLStatement(check_entry_query,'all')
        if(check_entry):
            return True
        else:
            return False

    def checkColumnsExistInTable(self,columnNames,tableName):
        '''
        :param columnNames: list (list of all columnNames)
        :param tableName: str
        :return: Bool or list ( if any column doesn't exist in database, function will return list of all column not in database else it will return True

        This function checks if correct coloumns are present in database or not
        '''
        database_column_name = []
        column_name_not_in_database = []
        check_column_name_query = 'Select column_name FROM information_schema."columns" WHERE "table_name"='+"'"+tableName+"'"+';'
        check_column_name = self.executeSQLStatement(check_column_name_query,'all')

        #clearing list data format
        for i in range(len(check_column_name)):
            database_column_name.append(check_column_name[i][0])
        # print(database_column_name)
        for column_name in columnNames:
            if column_name not in database_column_name:
                column_name_not_in_database.append(column_name)

        if column_name_not_in_database:
            column_name_not_in_database_comprhened = [x for x in column_name_not_in_database]
            return "Following coloums are not present inside database \n",column_name_not_in_database_comprhened
            #return "Following coloums are not present inside database",column_name_not_in_database
        else:
            return True


    def getPrimaryKeyOfAllTables(self):
        '''
        :param tableName: str
        :return: dict

        this function returns dictionary of primary key for all tables in a schema
        '''
        primary_key_tables = {} # dictionary of primary keys of tables
        pattern = "\((.*?)\)" #regex pattern to capture primary key inside brackets *()*
        primary_key_of_all_tables_in_schema_query = "SELECT conrelid::regclass AS table_name,conname AS primary_key,pg_get_constraintdef(oid) FROM   pg_constraint WHERE  contype = 'p' AND    connamespace = 'public'::regnamespace ORDER  BY conrelid::regclass::text, contype DESC;"
        primary_key_of_all_tables_in_schema = self.executeSQLStatement(primary_key_of_all_tables_in_schema_query,'all')
        for pkey in primary_key_of_all_tables_in_schema:
            if 'pkey' in pkey[1]: # validating primary key with tag pkey
                regex_result = re.findall(pattern,pkey[2])
                if len(regex_result) == 0:
                    logger.info("No Primary key for table ",pkey[0])
                elif len(regex_result) > 1:
                    logger.info("More than one primary key exist for table", pkey[0])
                else:
                    primary_key_tables[pkey[0]] = regex_result

        return primary_key_tables

    def getPrimaryKeyOfTable(self,tableName):
        '''
        :param tableName: str
        :return: list

        This function returns list of primary key for a particular table
        '''
        dict_primary_keys_of_all_tables = self.getPrimaryKeyOfAllTables()
        return dict_primary_keys_of_all_tables[tableName]

    def getColumnDatatype(self,tableName,ColumnName):
        '''
        :param tableName: str
        :Param ColumnName: str
        :return: list

        this function returns datatype for a particular column
        '''

        get_column_data_type_query = 'SELECT pg_typeof("' +ColumnName+ '"), pg_typeof(100) from ' +tableName+ ' limit 1;'
        get_column_data_type = self.executeSQLStatement(get_column_data_type_query,'all')
        if(get_column_data_type):
            return get_column_data_type[0][0]

    def getAllTablesOfPublicSchema(self):
        '''
        :return: list

        this function returns all tables present in public schema of postgresql server
        '''
        public_schema_tables = []
        get_tables_of_public_schema_query = "SELECT table_name FROM information_schema.tables WHERE table_schema='public' And table_type='BASE TABLE'"
        get_tables_of_public_schema = self.executeSQLStatement(get_tables_of_public_schema_query,'all')
        for data in get_tables_of_public_schema:
            public_schema_tables.append(data[0])
        return public_schema_tables

    def addColumnIntoTable(self,tableName,columnData):
        '''
        :param tableName: str
        :param columnData: dict
        :return: Bool

        This function adds new column to an existing table
        Column data can be of any type below -
        ** column data should be dictionary type with column name as key and other data as value and should be in same format as shown below

        1. Column name, data type, data type limit, constraint
            {'columnName':["data_type","data_type_limit","Constraint"]}

        2. Column name, data type, Constraint
            {'columnName':["data_type","Constraint"]}

        3. Column name, data type, data type limit
            {'columnName':["data_type","data_type_limit","Constraint"]}

        4. Column name, data type
            {'columnName':["data_type"]}
        '''

        insert_column_into_table_query = "ALTER Table " +tableName+ " ADD COLUMN "
        for data in columnData:
            print(data)
            column_name = data
            column_data_info = columnData[data]
            data_type = ""
            data_type_limit = ""
            data_constraint = ""
            if len(column_data_info) == 3:  # for column type containing column data type, data_type limit and Constraint i.e total 3 params
                data_type = column_data_info[0]
                data_type_limit = column_data_info[1]
                data_constraint = column_data_info[2]
            elif len(column_data_info) == 2 and type(column_data_info[1]) == str:  # for column type containing column data type, data_type limit and Constraint i.e total 2 params
                data_type = column_data_info[0]
                data_constraint = column_data_info[1]
            elif len(column_data_info) == 2:
                data_type = column_data_info[0]
                data_type_limit = column_data_info[1]
            else:
                data_type = column_data_info[0]

            if data_type and data_type_limit and data_constraint:
                if "CHAR" in data_type or "VARCHAR" in data_type or "BINARY" in data_type or "VARBINARY" in data_type or "TEXT" in data_type or "BLOB" in data_type or "SERIAL" in data_type:
                    insert_column_into_table_query = insert_column_into_table_query + column_name + " " + data_type + "(" + str(data_type_limit) + ") " + data_constraint
                elif "NUMERIC" in data_type:
                    insert_column_into_table_query = insert_column_into_table_query + column_name + " " + data_type + "(" + str(data_type_limit[0]) + "," + str(data_type_limit[1]) + ") " + data_constraint
            elif data_type and data_constraint:
                insert_column_into_table_query = insert_column_into_table_query + column_name + " " + data_type + " " + data_constraint
            elif data_type and data_type_limit:
                if "CHAR" in data_type or "VARCHAR" in data_type or "BINARY" in data_type or "VARBINARY" in data_type or "TEXT" in data_type or "BLOB" in data_type or "SERIAL" in data_type:
                    insert_column_into_table_query = insert_column_into_table_query + column_name + " " + data_type + "(" + str(data_type_limit) + ")"
                elif "NUMERIC" in data_type:
                    insert_column_into_table_query = insert_column_into_table_query + column_name + " " + data_type + "(" + str(data_type_limit[0]) + "," + str(data_type_limit[1]) + ")"
            else:
                insert_column_into_table_query = insert_column_into_table_query + column_name + " " + data_type

            insert_column_into_table = self.executeSQLStatementWithoutFetchingResult(insert_column_into_table_query)
            insert_column_into_table_query = "ALTER Table " +tableName+ " ADD COLUMN "


    def deleteTableFromDatabase(self,tableName,schemaName): #error in executing query
        '''
        :param tableName: str
        :return: Bool
        '''
        delete_table_query = "Drop table "+tableName
        try:
            delete_table = self.executeSQLStatementWithoutFetchingResult(delete_table_query)
        except:
            logger.error("Unable to delete table")

        #verify if table is deleted or not
        table_check_output = self.checkTableExists(tableName,schemaName)
        if table_check_output:
            return True
        else:
            return False

    def createTable(self,tableName,schemaName):
        '''
        :param tableName: str
        :param data_entry: dict ( dictionary_name:[[column_data_type, limit], [constraints]]
        :return: Bool

        This function creates a new table in database, return true if table is created successfully else return false
        '''
        create_table_query = "Create table "+tableName+"();"
        try:
            create_table = self.executeSQLStatementWithoutFetchingResult(create_table_query)
        except:
            logger.error("unable to create table")

        #verify if table is created or not
        table_check_output = self.checkTableExists(tableName,schemaName)
        if table_check_output:
            return True
        else:
            return False

    def getSchemasInPostrgressServer(self):
        '''
        :return: list

        this function return list of all schema present inside currently connected postgress server
        '''
        schema_data = []
        check_schema_in_server_query = "SELECT schema_name FROM information_schema.schemata"
        try:
            check_schema_in_server = self.executeSQLStatement(check_schema_in_server_query,'all')
        except:
            logger.error("Unable to retrieve schema details")
        if check_schema_in_server:
            for schema in check_schema_in_server:
                schema_data.append(schema[0])
        return schema_data

    def addDataToAllColumn(self,tableName,dataEntry):
        '''
        :param tableName: str
        :param dataEntry: list
        :return: bool
        '''
        add_data_to_column_query = "Insert into " + tableName + " values("
        for data in range(len(dataEntry)):
            if data == len(dataEntry) - 1:
                if type(dataEntry[data]) == int:
                    add_data_to_column_query = add_data_to_column_query + str(dataEntry[data]) + ")"
                elif type(dataEntry[data]) == str:
                    add_data_to_column_query = add_data_to_column_query + "'" + dataEntry[data] + "')"
            else:
                print(type(dataEntry[data]))
                if type(dataEntry[data]) == int:
                    add_data_to_column_query = add_data_to_column_query + str(dataEntry[data]) + ","
                elif type(dataEntry[data]) == str:
                    add_data_to_column_query = add_data_to_column_query + "'" + dataEntry[data] + "',"
    """
    def updateSpecificData(self,tableName,dataEntry):
        '''
        :param tableName: str
        :param dataEntry: dict {column_name:column_value,primary_key:value}
        :return:
        '''

        update_specific_data_query = "UPDATE "+tableName+" SET "
        for data in dataEntry:
            if type(dataEntry[data]) == int:
                update_specific_data_query = update_specific_data_query +data+ "=" +str(dataEntry[data]) +"where "

    """

    def fetchData(self,tableName,data):
        '''
        :param tableName: str
        :param data: dict
        :return: bool
        '''
        fetch_data_query = "Select * from " + tableName + " Where "
        for values in data:
            if (type(data[values]) == int):
                fetch_data_query = fetch_data_query + values + "=" + str(data[values]) + " and "
            elif (type(data[values]) == str):
                fetch_data_query = fetch_data_query + values + "='" + data[values] + "' and "
        fetch_data_query = fetch_data_query.split()

        fetch_data_query = " ".join(fetch_data_query)[:-3]
        fetch_data = self.executeSQLStatement(fetch_data_query,'all')
        if fetch_data:
            return True
        else:
            return False

    def fetchDataInnerJoin(self, columnData, fromTable, innerJoinTables):
        '''
        :param columnData: list inside dict
        :param fromTable: str
        :param innerJoinTables: dict inside dict inside list
        :return: list

        #example for parameters values
            columnData = {'student':['student_id','first_name'],'school':['school_name'],'city':['city_name']}
            fromTable = 'student'
            innerJoinTable = {'school':{'student_id':['student','school']},'city':{'school_id':['school','city']}}

        #example of query that is being depicted in this proc
        ----------------------
            SELECT student.student_id,student.first_name, school.school_name,city.city_name
            FROM student
            INNER JOIN school
            ON student.student_id = school.student_id
            INNER JOIN city
            ON school.school_id = city.school_id
        '''
        inner_join_query = "Select "
        # concatinating columns whose values are required in result
        for key in columnData:
            for value in columnData[key]:
                inner_join_query = inner_join_query + key + "." + value + ","
        inner_join_query = inner_join_query.split()
        inner_join_query = " ".join(inner_join_query)[:-1]  # removing ',' at the end

        # concatinating table from which data has to be fetched
        inner_join_query = inner_join_query + " FROM " + fromTable

        # concatinating inner join data
        for value in innerJoinTables:
            inner_join_query = inner_join_query + " INNER JOIN " + value + " ON "
            for dict_value in innerJoinTables[value]:
                for list_value in innerJoinTables[value][dict_value]:
                    inner_join_query = inner_join_query + list_value + "." + dict_value + "="
                inner_join_query = inner_join_query.split()
                inner_join_query = " ".join(inner_join_query)[:-1]
        print(inner_join_query)

        #executing inner join query
        inner_join = self.executeSQLStatement(inner_join_query,'all')

        return inner_join

    def fetchDataLeftOuterJoin(self,columnData, fromTable, leftouterJoinTables):
        '''
        :param columnData: list inside dict
        :param fromTable: str
        :param leftouterJoinTables: dict inside dict inside list
        :return: list

        #example for parameters values
            columnData = {'student':['student_id','first_name'],'school':['school_name'],'city':['city_name']}
            fromTable = 'student'
            leftouterJoinTables = {'school':{'student_id':['student','school']},'city':{'school_id':['school','city']}}

        #example of query that is being depicted in this proc
        ----------------------
            SELECT student.student_id,student.first_name, school.school_name,city.city_name
            FROM student
            LEFT JOIN school
            ON student.student_id = school.student_id
            LEFT JOIN city
            ON school.school_id = city.school_id
        '''
        left_join_query = "Select "
        # concatinating columns whose values are required in result
        for key in columnData:
            for value in columnData[key]:
                left_join_query = left_join_query + key + "." + value + ","
        left_join_query = left_join_query.split()
        left_join_query = " ".join(left_join_query)[:-1]  # removing ',' at the end

        # concatinating table from which data has to be fetched
        left_join_query = left_join_query + " FROM " + fromTable

        # concatinating left_join join data
        for value in leftouterJoinTables:
            left_join_query = left_join_query + " LEFT JOIN " + value + " ON "
            for dict_value in leftouterJoinTables[value]:
                for list_value in leftouterJoinTables[value][dict_value]:
                    left_join_query = left_join_query + list_value + "." + dict_value + "="
                left_join_query = left_join_query.split()
                left_join_query = " ".join(left_join_query)[:-1]
        print(left_join_query)

        #executing left_join join query
        left_join = self.executeSQLStatement(left_join_query,'all')

        return left_join

    def fetchDataRightOuterJoin(self,columnData, fromTable, rightouterJoinTables):
        '''
        :param columnData: list inside dict
        :param fromTable: str
        :param rightouterJoinTables: dict inside dict inside list
        :return: list

        #example for parameters values
            columnData = {'student':['student_id','first_name'],'school':['school_name'],'city':['city_name']}
            fromTable = 'student'
            rightouterJoinTables = {'school':{'student_id':['student','school']},'city':{'school_id':['school','city']}}

        #example of query that is being depicted in this proc
        ----------------------
            SELECT student.student_id,student.first_name, school.school_name,city.city_name
            FROM student
            RIGHT JOIN school
            ON student.student_id = school.student_id
            RIGHT JOIN city
            ON school.school_id = city.school_id
        '''
        right_join_query = "Select "
        # concatinating columns whose values are required in result
        for key in columnData:
            for value in columnData[key]:
                right_join_query = right_join_query + key + "." + value + ","
        right_join_query = right_join_query.split()
        right_join_query = " ".join(right_join_query)[:-1]  # removing ',' at the end

        # concatinating table from which data has to be fetched
        right_join_query = right_join_query + " FROM " + fromTable

        # concatinating right_join join data
        for value in rightouterJoinTables:
            right_join_query = right_join_query + " LEFT JOIN " + value + " ON "
            for dict_value in rightouterJoinTables[value]:
                for list_value in rightouterJoinTables[value][dict_value]:
                    right_join_query = right_join_query + list_value + "." + dict_value + "="
                right_join_query = right_join_query.split()
                right_join_query = " ".join(right_join_query)[:-1]
        print(right_join_query)

        #executing right_join join query
        right_join = self.executeSQLStatement(right_join_query,'all')

        return right_join


    def fetchDataFullOuterJoin(self,columnData, fromTable, fullouterJoinTables,whereClauseTable):
        '''
        :param columnData: list inside dict
        :param fromTable: str
        :param fullouterJoinTables: dict inside dict inside list
        :param whereClauseTable: dict
        :return:

        #example of query that is being depicted in this proc
        ----------------------
            SELECT student.first_name,student.student_id,school.school_name
            FROM student
            FULL OUTER JOIN school
            ON student.student_id = school.student_id
            WHERE school_name = 'MPS' and first_name = 'yash';
        '''

        full_outer_join_query = "Select "
        # concatinating columns whose values are required in result
        for key in columnData:
            for value in columnData[key]:
                full_outer_join_query = full_outer_join_query + key + "." + value + ","
        full_outer_join_query = full_outer_join_query.split()
        full_outer_join_query = " ".join(full_outer_join_query)[:-1]  # removing ',' at the end

        # concatinating table from which data has to be fetched
        full_outer_join_query = full_outer_join_query + " FROM " + fromTable

        # concatinating outer_join join data
        for value in fullouterJoinTables:
            full_outer_join_query = full_outer_join_query + " LEFT JOIN " + value + " ON "
            for dict_value in fullouterJoinTables[value]:
                for list_value in fullouterJoinTables[value][dict_value]:
                    full_outer_join_query = full_outer_join_query + list_value + "." + dict_value + "="
                full_outer_join_query = full_outer_join_query.split()
                full_outer_join_query = " ".join(full_outer_join_query)[:-1]

        full_outer_join_query = full_outer_join_query + " WHERE "
        for value in whereClauseTable:
            if type(whereClauseTable[value]) == int:
                full_outer_join_query = full_outer_join_query + value +"="+str(whereClauseTable[value]) +" and "
            elif type(whereClauseTable[value]) == str:
                full_outer_join_query = full_outer_join_query + value + "='" + whereClauseTable[value] + "' and "
        full_outer_join_query = full_outer_join_query.split()
        full_outer_join_query = " ".join(full_outer_join_query)[:-3]

        full_outer_join = self.executeSQLStatement(full_outer_join_query,'all')
        return full_outer_join

    def updateRowData(self,tableName,dataToUpdate,whereClauseData):
        '''
        :param tableName: str
        :param dataToUpdate: dict
        :param whereClauseData: dict
        :return: Bool
        '''

        update_data_query = "UPDATE "+tableName+" SET "
        for value in dataToUpdate:
            if type(dataToUpdate[value]) == int:
                update_data_query = update_data_query + value + "=" + str(dataToUpdate[value]) + ","
            elif type(dataToUpdate[value]) == str:
                update_data_query = update_data_query + value + "='" + dataToUpdate[value] + "',"
        update_data_query = update_data_query.split()
        update_data_query = " ".join(update_data_query)[:-1]

        update_data_query = update_data_query+" WHERE "
        for value in whereClauseData:
            if type(dataToUpdate[value]) == int:
                update_data_query = update_data_query + value + "=" + str(dataToUpdate[value]) + " and "
            elif type(dataToUpdate[value]) == str:
                update_data_query = update_data_query + value + "='" + dataToUpdate[value] + "' and "
        update_data_query = update_data_query.split()
        update_data_query = " ".join(update_data_query)[:-3]

        update_data = self.executeSQLStatement(update_data_query,'all')
        return update_data

    def getDataOrderby(self,tableName,selectColumns,orderbyColumns,order):
        '''
        :param tableName: str
        :param selectColumns: list
        :param orderbyColumns: list
        :param order: str ( ASC or DSC) or as empty string
        :return: Bool
        '''

        get_data_orderby_query = "SELECT "
        for value in selectColumns:
            get_data_orderby_query + value +","
        get_data_orderby_query = get_data_orderby_query.split()
        get_data_orderby_query = " ".join(get_data_orderby_query)[:-1]

        get_data_orderby_query = get_data_orderby_query + " FROM "+tableName+" ORDER BY "
        for value in orderbyColumns:
            get_data_orderby_query = get_data_orderby_query + value+","
        get_data_orderby_query = get_data_orderby_query.split()
        get_data_orderby_query = " ".join(get_data_orderby_query)[:-1]

        if(order):
            get_data_orderby_query = get_data_orderby_query+" "+order

        get_data_orderby  = self.executeSQLStatement(get_data_orderby_query,'all')
        return get_data_orderby

    def selectDistinct(self,tableName,columnData):
        '''
        :param tableName: str
        :param columnData: list
        :return: list
        '''
        select_destinct_query = "SELECT DISTINCT "
        for value in columnData:
            select_destinct_query = select_destinct_query + value + ","
        select_destinct_query = select_destinct_query.split()
        select_destinct_query = " ".join(select_destinct_query)[:-1] # Removing last ","
        select_destinct_query = select_destinct_query+" from "+tableName+";"
        select_destinct = self.executeSQLStatement(select_destinct_query,'all')

        return select_destinct

    def getMinData(self,tableName,columnName):
        '''
        :param tableName: str
        :param columnName: str
        :param whereClause: str ( sql where clause) eg. student_id > 1
        :return: list
        '''

        get_min_data_query = "SELECT MIN("+columnName+") FROM "+tableName
        get_min_data = self.executeSQLStatement(get_min_data_query,'all')
        return get_min_data

    def getMaxData(self,tableName,columnName):
        '''
        :param tableName: str
        :param columnName: str
        :param whereCondition: str ( sql where clause) eg. student_id > 1
        :return: list
        '''

        get_max_data_query = "SELECT MAX("+columnName+") FROM "+tableName
        get_max_data = self.executeSQLStatement(get_max_data_query,'all')
        return get_max_data[0][0]

    def getCountData(self,tableName,columnName,whereCondition):
        '''
        :param tableName: str
        :param columnName: str
        :param whereCondition: str ( sql where condition) eg. student_id > 1
        :return: list
        '''

        get_count_data_query = "SELECT COUNT("+columnName+") FROM "+tableName+" WHERE "+whereCondition
        get_count_data = self.executeSQLStatement(get_count_data_query,'all')
        return get_count_data

    def getAvgData(self,tableName,columnName,whereCondition):
        '''
        :param tableName: str
        :param columnName: str
        :param whereCondition: str
        :return: list
        '''
        get_avg_data_query = "SELECT AVG("+columnName+") FROM "+tableName+" WHERE "+whereCondition
        get_avg_data = self.executeSQLStatement(get_avg_data_query,'all')
        return get_avg_data

    def getSumData(self,tableName,columnName,whereCondition):
        '''
        :param tableName: str
        :param columnName: str
        :param whereCondition: str
        :return: list
        '''
        get_sum_data_query = "SELECT SUM("+columnName+") FROM "+tableName+" WHERE "+whereCondition
        get_sum_data = self.executeSQLStatement(get_sum_data_query,'all')
        return get_sum_data

    def getLikeData(self,tableName,columnData,whereColoumn,likepattern):
        '''
        :param tableName: str
        :param columnData: list
        :param whereColoumn: str
        :param likepattern: str
        :return: list
        '''
        get_like_data_query = "SELECT "
        for value in columnData:
            get_like_data_query = get_like_data_query + value + ","
        get_like_data_query = get_like_data_query.split()
        get_like_data_query = " ".join(get_like_data_query)[:-1]  # Removing last ","
        get_like_data_query = get_like_data_query+" FROM "+tableName+" WHERE "+whereColoumn+" LIKE "+likepattern

        get_like_data = self.executeSQLStatement(get_like_data_query,'all')
        return get_like_data

    def getBetweenData(self,tableName,columnData,whereColumnName,betweenValue1,betweenValue2):
        '''
        :param tableName: str
        :param columnData: list
        :param whereColumnName: str
        :param betweenValue1: int or str
        :param betweenValue2: int or str
        :return: list
        '''

        get_between_data_query = "SELECT "
        for value in columnData:
            get_between_data_query = get_between_data_query + value + ","
        get_between_data_query = get_between_data_query.split()
        get_between_data_query = " ".join(get_between_data_query)[:-1]  # Removing last ","
        if type(betweenValue1) == str and type(betweenValue2) == str:
            get_between_data_query = get_between_data_query + " FROM "+tableName+" WHERE "+whereColumnName+" BETWEEN '"+betweenValue1+"' AND '"+betweenValue2+"'"
        elif type(betweenValue1) == int and type(betweenValue2) == int:
            get_between_data_query = get_between_data_query + " FROM " + tableName + " WHERE " + whereColumnName + " BETWEEN " + str(betweenValue1) + " AND " + str(betweenValue2)
        get_between_data=self.executeSQLStatement(get_between_data_query,'all')
        return get_between_data

    def getNotNullData(self,tableName,columnNames,wherecolumn):
        '''
        :param tableName: str
        :param columnNames: list
        :param wherecolumn: str
        :return: list
        '''

        get_not_null_data_query = "SELECT "
        for value in columnNames:
            get_not_null_data_query = get_not_null_data_query + value + ","
        get_not_null_data_query = get_not_null_data_query.split()
        get_not_null_data_query = " ".join(get_not_null_data_query)[:-1]  # Removing last ","
        get_not_null_data_query = get_not_null_data_query + " FROM" + tableName + " WHERE " + wherecolumn + " IS NOT NULL"

        get_not_null_data = self.executeSQLStatement(get_not_null_data_query,'all')
        return get_not_null_data

    def getNullData(self, tableName, columnNames, wherecolumn):
        '''
        :param tableName: str
        :param columnNames: list
        :param wherecolumn: str
        :return: list
        '''

        get_null_data_query = "SELECT "
        for value in columnNames:
            get_null_data_query = get_null_data_query + value + ","
        get_null_data_query = get_null_data_query.split()
        get_null_data_query = " ".join(get_null_data_query)[:-1]  # Removing last ","
        get_null_data_query = get_null_data_query + " FROM" + tableName + " WHERE " + wherecolumn + " IS NULL"

        get_null_data = self.executeSQLStatement(get_null_data_query, 'all')
        return get_null_data

    def getDataByUnion(self,tableColumns):
        '''
        :param tableColumns: dict eg. dictionary with key as tablename and values as columns that need to be selected
        :return: list
        '''

        get_data_by_union_query = ""
        for value in tableColumns:
            tableName = value   #table name
            columns = tableColumns[value]   #same table column name
            get_data_by_union_query = get_data_by_union_query + "SELECT "
            for data in columns:
                get_data_by_union_query = get_data_by_union_query + data + ","
            get_data_by_union_query = get_data_by_union_query.split()
            get_data_by_union_query = " ".join(get_data_by_union_query)[:-1] # Removing last ","
            get_data_by_union_query = get_data_by_union_query + " FROM "+tableName+" UNION "
        get_data_by_union_query = get_data_by_union_query.split()
        get_data_by_union_query = " ".join(get_data_by_union_query)[:-6] # Removing UNION at the end of query

        get_data_by_union_query = self.executeSQLStatement(get_data_by_union_query,'all')
        return get_data_by_union_query

    def getDataByUnionAll(self,tableColumns):
        '''
        :param tableColumns: dict eg. dictionary with key as tablename and values as columns that need to be selected
        :return: list
        '''

        get_data_by_union_all_query = ""
        for value in tableColumns:
            tableName = value   #table name
            columns = tableColumns[value]   #same table column name
            get_data_by_union_all_query = get_data_by_union_all_query + "SELECT "
            for data in columns:
                get_data_by_union_all_query = get_data_by_union_all_query + data + ","
            get_data_by_union_all_query = get_data_by_union_all_query.split()
            get_data_by_union_all_query = " ".join(get_data_by_union_all_query)[:-1] # Removing last ","
            get_data_by_union_all_query = get_data_by_union_all_query + " FROM "+tableName+" UNION "
        get_data_by_union_all_query = get_data_by_union_all_query.split()
        get_data_by_union_all_query = " ".join(get_data_by_union_all_query)[:-6] # Removing UNION at the end of query

        get_data_by_union_all = self.executeSQLStatement(get_data_by_union_all_query,'all')
        return get_data_by_union_all

    def getSqlCaseData(self,tableName,selectColumnNames,whenThenCondition,elseStatement,alias):
        '''
        :param tableName: str
        :param selectColumnNames: list
        :param whenThenCondition:  dict # where When is the key and Then is value for same #{"when condition1":"then condition1","when condition2":"then condition2"}
        :param elseStatement:  str
        :param alias: str
        :return: list
        '''

        sql_case_data_query = "SELECT "
        for value in selectColumnNames:
            sql_case_data_query = sql_case_data_query + value + ","
        sql_case_data_query = sql_case_data_query +" CASE"
        for value in whenThenCondition:
            when_condition = value
            then_condition= whenThenCondition[value]
            sql_case_data_query = sql_case_data_query + " WHEN "+when_condition+" THEN '"+then_condition+"' "
        sql_case_data_query = sql_case_data_query + "ELSE '"+elseStatement+"' END AS "+alias+" FROM "+tableName+";"
        sql_case_data = self.executeSQLStatement(sql_case_data_query,'all')

        return sql_case_data

    def getDataWithSqlComparisonOperator(self,tableName,columnNames,operator,whereColumn,validationValue):
        '''
        :param tableName: str
        :param columnNames: list // if empty list given, this will return all columns
        :param operator: str // operator can be =,>,<,>=,<=,<>
        :param whereColumn: str
        :param validationValue: str or int
        :return: list
        '''
        get_data_with_sql_operator_query = "SELECT "
        if columnNames:
            for value in columnNames:
                get_data_with_sql_operator_query = get_data_with_sql_operator_query + value + " ,"
            get_data_with_sql_operator_query = get_data_with_sql_operator_query.split()
            get_data_with_sql_operator_query = " ".join(get_data_with_sql_operator_query)[:-1] # Removing last column
        else:
            get_data_with_sql_operator_query = get_data_with_sql_operator_query + "* "
        get_data_with_sql_operator_query = get_data_with_sql_operator_query+"FROM "+tableName+" WHERE "

        #adding operator based on input
        if operator == "=":
            get_data_with_sql_operator_query = get_data_with_sql_operator_query+"="
        elif operator == ">":
            get_data_with_sql_operator_query = get_data_with_sql_operator_query + ">"
        elif operator == "<":
            get_data_with_sql_operator_query = get_data_with_sql_operator_query + "<"
        elif operator == ">=":
            get_data_with_sql_operator_query = get_data_with_sql_operator_query + ">="
        elif operator == "<=":
            get_data_with_sql_operator_query = get_data_with_sql_operator_query + "<="
        elif operator == "<>":
            get_data_with_sql_operator_query = get_data_with_sql_operator_query + "<>"


        if type(validationValue) == int:
            get_data_with_sql_operator_query = get_data_with_sql_operator_query + validationValue +";"
        elif type(validationValue) == str:
            get_data_with_sql_operator_query = get_data_with_sql_operator_query + "'" + validationValue + "';"

        get_data_with_sql_operator = self.executeSQLStatement(get_data_with_sql_operator_query,'all')
        return get_data_with_sql_operator_query

    def createDatabaseBackup(self,databaseName,filepath):
        '''
        :param databaseName: str
        :param filepath: str
        :return:
        '''
        database_backup_query = "BACKUP DATABASE "+databaseName+" TO DISK ='"+filepath+"';"
        database_backup = self.executeSQLStatementWithoutFetchingResult(database_backup_query)

    def checkTablesExistInDatabase(self,tableNames,schemaName):
        '''
        :param tableNames: list (list of all table names)
        :param schemaName: name of the schema in database
        :return: Bool or list ( if any column doesn't exist in database, function will return list of all column not in database else it will return True

        This function checks if correct tables are present in database or not
        '''

        actual_table_names = []
        table_names_not_in_database = []
        check_table_name_query = "Select table_name FROM information_schema.tables WHERE table_schema='"+schemaName+"' And table_type='BASE TABLE';"
        check_table_name = self.executeSQLStatement(check_table_name_query,'all')

        #clearing list data format
        for i in range(len(check_table_name)):
            actual_table_names.append(check_table_name[i][0])
        print(actual_table_names)
        for table_name in tableNames:
            if table_name not in actual_table_names:
                table_names_not_in_database.append(table_name)

        if table_names_not_in_database:
            table_names_not_in_database_comprhened = [x for x in table_names_not_in_database]
            return "Following tables are not present in the database \n",table_names_not_in_database_comprhened
            #return "Following coloums are not present inside database",column_name_not_in_database
        else:
            return True




# conn = psycopg2.connect(
#     host="localhost",
#     database="DbTesting",
#     user="postgres",
#     password="1234")
# cursor= conn.cursor()
# # cursor.execute('select * from "Python_DB_Testing" ')
# # results=cursor.fetchall()
# # print(results)
# cursor.execute("Select column_name from information_schema.columns where table_name='Python_DB_Testing'")
# results=cursor.fetchall()
# # print(results)
# expectedColumns=('ID','Price','Name')
# print("expeccted columns tuple size: "+str(len(expectedColumns)))
# print ("results size:"+str(len(results)))
# for i in results:
#     for j in range(0, len(expectedColumns)):
#         if (i[0] == expectedColumns[j]):
#             print(expectedColumns[j]+' value exists')



