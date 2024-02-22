#from database_testing import DBConnect
import logging as logger
import pytest

#from database_testing.DBConnect import DatabaseConnector as dbconnect
# from database_testing.DBConnect import DatabaseConnector as dbconnect
from database_testing.database_testing_utilities import DatabaseConnector as dbconnect
from config import read_from_config_files
logger = logger.getLogger(__name__)

class TestDbTest(dbconnect):
    '''
        List of testcase -
        1. test_connect_to_database
        2. test_create_table_in_databse
        3. test_add_column_in_table
        4.

    '''

    # def test_connect_to_database(self):
    #     database_connect_obj = dbconnect()
    #     database_connect = database_connect_obj.getDatabaseConnection()
    #
    # to test whether a database exists in the postgres server
    def test_check_db_exists(self):
        dbconnect.checkDatabaseExist(self,'SampleDatabase')

    # to test whether actor table exists in the database
    def test_actor_table_exist(self):
        dbconnect.checkTableExists(self,'actor','public')

    # to test whether the list of tables exist in the database
    def test_all_tables_exist_in_database(self):
        expectedTablesList=['actor', 'store', 'address', 'category', 'city', 'country', 'customer', 'film_actor', 'film_category', 'inventory', 'language', 'rental', 'staff', 'payment', 'film']
        res=dbconnect.checkTablesExistInDatabase(self,expectedTablesList,'public')
        assert res == True, "" + str(res[0]) + str(res[1])

    # to test whether actor_id column type is integer
    def test_actor_id_column_type_in_actor_table(self):
        res=dbconnect.getColumnDatatype(self,'actor','actor_id')
        assert  res=='integer'

    # to test whether actor table has any data
    def test_actor_table_has_data(self):
        res=dbconnect.checkSingleTableSize(self,'actor')
        assert res>0

    # to test whether actor table has primary key constraint
    def test_actor_table_has_primary_key(self):
        res=dbconnect.getPrimaryKeyOfTable(self,'actor')
        assert res is not None

    # def test_actor_table_after_post_request(self):
    #     print('test')
    #
    # def test_actor_table_after_put_request(self):
    #     print('test')
    #
    # def test_actor_table_after_delete_request(self):
    #     print('test')

    # to test whether inserting data into actor table works as expected
    def test_actor_table_after_inserting_data(self):
        max_id=dbconnect.getMaxData(self,'actor','actor_id')
        id=max_id+1
        fname='auto_firstname_'+str(id)
        lname='auto_lastname_'+str(id)
        before_insert_count=dbconnect.checkSingleTableSize(self,'actor')
        res=dbconnect.executeSQLStatementWithoutFetchingResult(self,"insert into actor values("+str(id)+",'"+fname+"','"+lname+"',Now());")
        after_insert_count = dbconnect.checkSingleTableSize(self, 'actor')
        assert after_insert_count==before_insert_count+1

    # to test whether updating data in actor table works as expected
    def test_actor_table_after_updating_data(self):
        max_id = dbconnect.getMaxData(self, 'actor', 'actor_id')
        id = max_id + 1
        fname = 'auto_firstname_' + str(id)
        lname = 'auto_lastname_' + str(id)
        res = dbconnect.executeSQLStatementWithoutFetchingResult(self, "insert into actor values(" + str(id) + ",'" + fname + "','" + lname + "',Now());")
        update_first_name = fname+'_updated'
        res=dbconnect.executeSQLStatementWithoutFetchingResult(self,"update actor set first_name='"+update_first_name+"' where actor_id="+str(id)+"")
        res=dbconnect.executeSQLStatement(self,"select first_name from actor where actor_id="+str(id)+"",'one')
        assert res[0]==update_first_name

    # to test whether deleting data from actor table works as expected
    def test_actor_table_after_deleting_data(self):
        max_id = dbconnect.getMaxData(self, 'actor', 'actor_id')
        id = max_id + 1
        fname = 'auto_firstname_' + str(id)
        lname = 'auto_lastname_' + str(id)
        res = dbconnect.executeSQLStatementWithoutFetchingResult(self, "insert into actor values(" + str(id) + ",'" + fname + "','" + lname + "',Now());")
        before_delete_count = dbconnect.checkSingleTableSize(self, 'actor')
        res = dbconnect.executeSQLStatementWithoutFetchingResult(self,"delete from actor where actor_id=" + str(id) + "")
        after_delete_count = dbconnect.checkSingleTableSize(self, 'actor')
        assert before_delete_count==after_delete_count+1

    # to test read data from actor table works as expected
    def test_read_actor_table_data(self):
        res=dbconnect.executeSQLStatement(self,'select * from actor','all')
        assert res is not None

    # to test the integrity of actor table
    def test_integrity_of_the_actor_table(self):
        # try deleting a row which is being referenced in other child tables
        max_id = dbconnect.getMaxData(self, 'actor', 'actor_id')
        id = max_id + 1
        fname = 'auto_firstname_' + str(id)
        lname = 'auto_lastname_' + str(id)
        res1 = dbconnect.executeSQLStatementWithoutFetchingResult(self, "insert into actor values(" + str(id) + ",'" + fname + "','" + lname + "',Now());")
        res2 = dbconnect.executeSQLStatementWithoutFetchingResult(self, "insert into film_actor values(" + str(id) + "," + str(1)  +", Now());")
        try:
            res = dbconnect.executeSQLStatementWithoutFetchingResult(self,"delete from actor where actor_id=" + str(id) + "")
        except Exception:
            print('Delete was not executed as expected')
            assert True

    # to test all the columns exist in the actor table
    def test_check_columns_exists_in_actor_table(self):
        expectedColumns={'actor_id','first_name','last_name','last_update'}
        res=dbconnect.checkColumnsExistInTable(self,expectedColumns,'actor')
        assert  res==True,""+str(res[0])+str(res[1])

    # no need to add these tests
    # def test_add_column_in_table(self):
    #     column_data = {'id':["NUMERIC", [30,30]],'dob':["NUMERIC", [30,30]]}
    #

    # def test_create_table_in_databse(self):
    #     dbconnect.createTable('Sample','public')
    #

"""
from database_testing import DBConnect
from database_testing.DBConnect import DatabaseConnector
query="Select column_name from information_schema.columns where table_name='Python_DB_Testing'"
query2='Select * from "Python_DB_Testing"'
obj= DatabaseConnector()
# obj.printResults(query2)
# obj.getColumnsofTable('Python_DB_Testing')
# print(obj.checkTableExists('Python_DB_Testing','public')[0][0])
obj.checkColumnExistsInTable('Python_DB_Testing','Name')

################################################
columnData = {'student': ['student_id', 'first_name'], 'school': ['school_name'], 'city': ['city_name']}
fromTable = 'student'
innerJoinTable = {'school': {'student_id': ['student', 'school']}, 'city': {'school_id': ['school', 'city']}}
whereClauseTable = {'school_name':'MPS'}
#print(obj.fetchDataInnerJoin(columnData,fromTable,innerJoinTable))
#print(obj.fetchDataFullOuterJoin(columnData,fromTable,innerJoinTable,whereClauseTable))
#print(obj.updateRowData('city',{'city_id':1007,'city_name':'REWARI'},{'city_id':1007,'city_name':'REWARI'}))
#print(obj.selectDistinct("student",['first_name','last_name']))
#print(obj.getDataByUnion({"table1":['col1','col2'],"table2":['col1','col2'],"table3":['col1','col2']}))
print(obj.getSqlCaseData('student',['col1','col2'],{"when condition1":"then condition1","when condition2":"then condition2"},"this is else statement","alias name"))
"""
