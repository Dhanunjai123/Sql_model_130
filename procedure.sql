/*
Change log: <Rajasundar - 20-06-23> Removed statically setting timestamp column part
 */

USE SCHEMA DEVELOPMENT_DB_JMAN.KENNAMETAL_STAGING
;

CREATE OR REPLACE PROCEDURE AUTOMATE_120(database_name STRING, schema_name STRING, client STRING)
  RETURNS STRING
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.8'
  PACKAGES = ('snowflake-snowpark-python')
  HANDLER = 'main'
AS
$$ 
import re


def run(session, database_name, schema_name, table_name):
    """
    To find out the each *untyped table's column datatype 

    Parameters :
        database_name : Name of the database which is having the *untyped tables
        schema_name   : Name of the schema name from the above database name which is having the *untyped tables
        table_name    : Name of the *untyped table name

    Returns : 
        response_dict : A dictionary which is having the column name as key and it's data type as value
    """

    table_column_names = session.sql(
        f"SELECT column_name FROM \"{database_name}\".INFORMATION_SCHEMA.COLUMNS WHERE table_schema = '{schema_name}' AND table_name = '{table_name}'"
    ).collect()

    response_list = []

    def find_int_float(session, process_column, database_name, schema_name, table_name):
        """
        To find out the integer/float data columns

        Parameters :
            process_column : Name of the column that needs to be find datatype
            database_name  : Name of the database 
            schema_name    : Name of the schema
            table          : Name of table table
        Returns : 
            Returns datatype if the column is NUMBER or FLOAT column
        """

        sql_response = session.sql(
            f"SELECT \"{process_column}\"::INT FROM {database_name}.{schema_name}.{table_name}"
        ).collect()

        try:
            sql_response_float = session.sql(
                f"SELECT \"{process_column}\" FROM {database_name}.{schema_name}.{table_name} WHERE \"{process_column}\"::VARCHAR LIKE '%.%'"
            ).collect()

            if len(sql_response_float) > 1:
                return "FLOAT"
            else:
                return "NUMBER"
        except:
            return "NUMBER"

    for loop_iter in table_column_names:
        process_column = loop_iter.COLUMN_NAME
        response_list.append(process_column)

        # To find if the column is null
        sql_response_null = session.sql(
            f"SELECT \"{process_column}\" AS data_value FROM {database_name}.{schema_name}.{table_name} WHERE \"{process_column}\" IS NOT NULL"
        ).collect()

        if len(sql_response_null) == 0:
            response_list.append('VARCHAR')
        else:
            try:
                # To find out the boolean data columns
                sql_response = session.sql(
                    f"SELECT \"{process_column}\"::BOOLEAN FROM {database_name}.{schema_name}.{table_name}"
                ).collect()

                sql_response = session.sql(
                    f"SELECT \"{process_column}\" AS data_value FROM {database_name}.{schema_name}.{table_name} WHERE \"{process_column}\" IS NOT NULL"
                ).collect()

                sample_value = sql_response[0].DATA_VALUE

                # To check the sample data from the selected column
                boolean_responce = ['TRUE', 'FALSE', 'True', 'False', 'true', 'false']

                if str(sample_value) in boolean_responce:
                    response_list.append("BOOLEAN")
                else:
                    # To find out the integer/float data columns
                    int_float_result = find_int_float(
                        session, process_column, database_name, schema_name, table_name)
                    response_list.append(int_float_result)

            except:

                # To find out the integer/float data columns
                try:
                    int_float_result = find_int_float(
                        session, process_column, database_name, schema_name, table_name)
                    response_list.append(int_float_result)
                except:

                    # To find out the date/timestamp columns
                    try:
                        sql_response = session.sql(
                            f"SELECT \"{process_column}\"::TIMESTAMP FROM {database_name}.{schema_name}.{table_name}").collect()

                        try:
                            sql_response_timestamp = session.sql(
                                f"SELECT \"{process_column}\" FROM {database_name}.{schema_name}.{table_name} WHERE \"{process_column}\"::VARCHAR LIKE '%:%'"
                            ).collect()

                            if len(sql_response_timestamp) > 1:
                                response_list.append('TIMESTAMP')
                            else:
                                response_list.append('DATE')
                        except:
                            response_list.append('DATE')
                    except:
                        sql_response_timestamp = session.sql(
                                f"SELECT \"{process_column}\" FROM {database_name}.{schema_name}.{table_name} WHERE \"{process_column}\"::VARCHAR ILIKE '%:%:% AM' OR \"{process_column}\"::VARCHAR ILIKE '%:%:% PM'"
                            ).collect()

                        if len(sql_response_timestamp) >= 1:
                            try:
                                sql_response = session.sql(
                                    f"SELECT TO_TIMESTAMP(\"{process_column}\", 'mm/dd/yyyy HH12:MI:SS AM') FROM {database_name}.{schema_name}.{table_name}").collect()
                                response_list.append('TIMESTAMP_TYPE2')
                            except:
                                # process_column_split = process_column.split(' ')
                                # process_column_split_lower = [
                                #     i.lower() for i in process_column_split]

                                # if len(process_column_split_lower) > 1 and 'date' in process_column_split_lower:
                                #     response_list.append('TIMESTAMP')
                                # else:
                                response_list.append('VARCHAR')
                        else:
                            # removing this part of code since it's caused issue in one of the implementation
                            # process_column_split = process_column.split(' ')
                            # process_column_split_lower = [
                            #    i.lower() for i in process_column_split]

                            # if len(process_column_split_lower) > 1 and 'date' in process_column_split_lower:
                            #    response_list.append('TIMESTAMP')
                            # else:
                            response_list.append('VARCHAR')

    response_dict = {
        response_list[i]: response_list[i + 1] for i in range(0, len(response_list), 2)
    }

    return response_dict


def drop_table(database_name, schema_name, table):
    """
    To produce the drop commands for *typed table

    Parameters :
        database_name : Name of the db
        schema_name   : Name of the schema name that the table needs to be dropped
        table         : Name of table table name that needs to droped

    Returns : 
        drop_cmd : A list of typed tables drop command in string datatype
    """

    drop_cmd = ''
    table = table.replace('UNTYPED', 'TYPED').lower()
    drop_cmd = f'\nDROP TABLE IF EXISTS {schema_name}.{table}\n;'

    return drop_cmd


def create_table(database_name, schema_name, table, dtype_dict):
    """
    To produce the create table commands for typed tables

    Parameters :
        database_name : Name of the db
        schema_name   : Name of the schema name where the table needs to be created
        table         : Name of table name that needs to be create
        dtype_dict    : Dictionary that contains column names and it's data type

    Returns : 
        create_cmd : A list of typed tables create table command in string datatype
    """

    table = table.replace('UNTYPED', 'TYPED').lower()
    create_cmd = ''
    create_cmd = f'\nCREATE TABLE {schema_name}.{table} (\n'

    for key in dtype_dict:
        data_type = dtype_dict[key]
        
        # To find abbreviation string from column name
        caps_result = re.findall('[A-Z][A-Z]+', key)

        for i in caps_result:
            i1 = i.lower()
            i1 = '_' + i1 + '_'
            key = key.replace(i, i1)

        # To split out the column name by caps strings
        key_list = re.findall('[a-zA-Z][^A-Z]*', key)
        key_string = '_'.join(key_list)
        key = key_string.lower()
        key = key.replace('%', '_pc_')
        key = key.replace('&','_and_')
        key = key.replace('@','_at_')

        if '# of' in key:
            key = key.replace('# of', 'number_of_')
        elif '#' in key:
            key = key.replace('#', 'number')  # updated 14-06-23

        # key = key.replace('#')
        # To replace with null
        replace_null = ['.', "'", ':', ')', ']', ';', '*']

        for i in replace_null:
            key = key.replace(i, '')

        # To replace with underscore
        replace_underscore = [' ', '/', '-', '(', '[', '____', '___', '__']

        for i in replace_underscore:
            key = key.replace(i, '_')
            

        # To avoid last underscore of the string
        if key[-1] == '_':
            key = key[:-1]

        if data_type == 'VARCHAR':
            data_type = 'VARCHAR(32767)'
        elif data_type == 'TIMESTAMP_TYPE2':
            data_type = 'TIMESTAMP'

        result = f'{key} {data_type} NULL,\n'
        create_cmd = create_cmd + result

    create_cmd = create_cmd + 'global_nickname VARCHAR(255) NULL )\n;'

    return create_cmd


def insert_table(database_name, schema_name, table, dtype_dict, client):
    """
    To produce the insert tabls commands for typed tables

    Parameters :
        database_name : Name of the db
        schema_name   : Name of the schema name that the table needs to be created
        table         : Name of *typed table name 
        dtype_dict    : Dictionary that contains column names and it's data type
        client        : Client name used for different client

    Returns :
        insert_cmd : A list of typed table's insert into command in string datatype
    """

    new_table = table.replace('UNTYPED', 'TYPED').lower()
    table = table.lower()
    insert_cmd = f'\nINSERT INTO {schema_name}.{new_table}\nSELECT\n'

    for key in dtype_dict:
        data_type = dtype_dict[key]
        result = ''

        if data_type == 'TIMESTAMP':
            result = f'COALESCE(TRY_TO_TIMESTAMP("{key}"), TRY_TO_TIMESTAMP("{key}",\'mm/dd/yyyy HH:MI\')),\n'
        elif data_type == 'TIMESTAMP_TYPE2':
            result = f'COALESCE(TRY_TO_TIMESTAMP("{key}"), TO_TIMESTAMP("{key}",\'mm/dd/yyyy HH12:MI:SS PM\')),\n'
        else:
            result = f'"{key}",\n'

        insert_cmd = insert_cmd + result

    insert_cmd = insert_cmd + f'$global_nickname FROM {schema_name}.{table}\n;'

    return insert_cmd


def main(session, database_name, schema_name, client):
    """
    This would act as main funtion and it's used to call other funtion to create a final structured query for 120 model

    Parameters :
        database_name : Name of the database
        schema_name   : Name of the schema name where *_untyped tables sits there
        client        : Name of the Client, Ex. CCHMC, Sanofi

    Returns :
        Return success after all the *_typed table are created
    """

    list_untyped_tables = f'SELECT table_name FROM information_schema.tables WHERE table_schema = \'{schema_name}\' AND table_name ILIKE \'%UNTYPED\''
    sql_res = session.sql(list_untyped_tables).collect()
    d_type = []
    table_names = []
    
    final_query = f'SET global_nickname = \'{client}\';\n'

    for row in sql_res:
        row = row.TABLE_NAME
        data_type = run(session, database_name, schema_name, row)
        table_names.append(row)
        d_type.append(data_type)

    for i in range(len(table_names)):
        table_name = table_names[i]
        dtype_dict = d_type[i]

        drop_res = drop_table(database_name, schema_name, table_name)
        final_query = final_query + drop_res

        create_res = create_table(database_name, schema_name, table_name, dtype_dict)
        final_query = final_query + create_res

        insert_res = insert_table(database_name, schema_name, table_name, dtype_dict, client)
        final_query = final_query + insert_res

    return final_query
#    try:
#        split_query = final_query.split(';')

#        for i in range(len(split_query)-1):
#            sql_query = split_query[i]
#            result = session.sql(sql_query).collect()

#        return 'Success'

#    except Exception as e:
#        return sql_query

$$
;


CALL AUTOMATE_120('DEVELOPMENT_DB_JMAN', 'KENNAMETAL_STAGING', 'KENNAMETAL')
;
