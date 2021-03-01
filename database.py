from __future__ import annotations
import pickle
from table import Table
from time import sleep, localtime, strftime
import os
from btree import Btree
import shutil
from misc import split_condition,get_op
from tabulate import tabulate
from p2pnetwork.node import Node

class Database(Node):
    '''
    Database class contains tables.
    '''
    nodes = [["127.0.0.1", 8002], ["127.0.0.1", 8003]]
    def __init__(self, name, load=True, distributed=False):
        host = "127.0.0.1"
        port = 8001
        super(Database, self).__init__(host, port, None)
        self.tables = {}
        self._name = name
        self.distributed = distributed
        self.savedir = f'dbdata/{name}_db'
        if self.distributed:
            self.start()
            for n in self.nodes:
                self.connect_with_node(n[0], n[1])
        if load:
            try:
                self.load(self.savedir)
                print(f'Loaded "{name}".')
                return
            except:
                print(f'"{name}" db does not exist, creating new.')

        # create dbdata directory if it doesnt exist
        if not os.path.exists('dbdata'):
            os.mkdir('dbdata')

        # create new dbs save directory
        try:
            os.mkdir(self.savedir)
        except:
            pass

        # create all the meta tables
        self.create_table('meta_length',  ['table_name', 'no_of_rows'], [str, int])
        self.create_table('meta_locks',  ['table_name', 'locked'], [str, bool])
        self.create_table('meta_insert_stack',  ['table_name', 'indexes'], [str, list])
        self.create_table('meta_indexes',  ['table_name', 'index_name'], [str, str])
        self.save()

    # all the methods below are called when things happen in the network.
    # implement your network node behavior to create the required functionality.

    def outbound_node_connected(self, node):
        print("outbound_node_connected: " + str(node.port))

    def inbound_node_connected(self, node):
        print("inbound_node_connected: " + str(node.port))

    def inbound_node_disconnected(self, node):
        print("inbound_node_disconnected: " + str(node.port))

    def outbound_node_disconnected(self, node):
        print("outbound_node_disconnected: " + str(node.port))

    def node_message(self, node, data):
        message = data
        if ("Data" in message.keys()):
            self.DataHandler(message)
        else:
            if (message["action"] == "select"):
                self.select_get(message,node)
            elif (message["action"] == "update"):
                self.update_get(message, node)
            elif (message["action"] == "delete"):
                self.delete_get(message, node)
            elif (message["action"] == "insert"):
                self.insert_get(message,node)
            else:
                print("Invalid Message")
        print("node_message from " + str(node.port) + ": " + str(data))

    def node_disconnect_with_outbound_node(self, node):
        print("node wants to disconnect with other outbound node: " + str(node.port))

    def node_request_to_stop(self):
        print("node is requested to stop!")

    def DataHandler(self,message):
        if message["action"]=="select":
            if message["Data"]!=None:
                message["Data"].show()


    def select_post(self,table_name,columns,condition,order_by,asc,top_k):
        message = {
            "action": "select",
            "table": table_name,
            "columns": columns,
            "select_condition" : condition,
            "order_by": order_by,
            "asc": asc,
            "top_k": top_k
        }
        self.send_to_nodes(message)

    def select_get(self,message,node):
        flag= False
        if message["table"] in self.tables:
            table=[]
            condition_column, condition_operator, condition_value = self.tables[message["table"]]._parse_condition(message["select_condition"])
            distributed_key_name, distributed_key_operator, distributed_key_value = self.tables[message["table"]]._parse_condition(self.tables[message["table"]].distributed_key)
            if condition_operator=="==" or distributed_key_operator=="==":
                if condition_operator=="==":
                    if get_op(distributed_key_operator,distributed_key_value,condition_value):
                        table=self.select(message["table"],message["columns"],message["select_condition"],message["order_by"],message["asc"],message["top_k"],None,True,True)
                        flag=True
                else:
                    if get_op(condition_operator,condition_value,distributed_key_value):
                        table=self.select(message["table"],message["columns"],message["select_condition"],message["order_by"],message["asc"],message["top_k"],None,True,True)
                        flag=True
            else:
                if get_op(condition_operator,condition_value,distributed_key_value):
                    table=self.select(message["table"],message["columns"],message["select_condition"],message["order_by"],message["asc"],message["top_k"],None,True,True)
                    flag=True
            if flag:
                response = {
                  "Data":table,
                  "action":"select",
                  "table": message["table"],
                  "columns": message["columns"],
                  "select_condition" : message["condition"],
                  "order_by": message["order_by"],
                  "asc": message["asc"],
                  "top_k": message["top_k"]
                }
                self.send_to_node(node, response)
            else:
                response = {
                    "Data":None,
                    "action":"select"
                }
                self.send_to_node(node, response)
        else:
            response = {
                "Data": self.host + " " + str(self.port) + " :" + " No work needs to be done from here"
            }
            self.send_to_node(node, response)

    def insert_post(self, table_name, row):
        message = {
            "action": "insert",
            "table": table_name,
            "row": row
        }
        self.send_to_nodes(message)

    def insert_get(self, message,node):
        if message["table"] in self.tables:
            column_name, operator, value = self.tables[message["table"]]._parse_condition(self.tables[message["table"]].distributed_key)
            if get_op(operator, message['row'][self.tables[message["table"]].column_names.index(column_name)], value):
                self.insert(message["table"], message["row"], True,True)
                response = {
                  "Data": self.host + " " + str(self.port) + " :" + " Done"
                }
                self.send_to_node(node, response)
            else:
                response = {
                    "Data": self.host + " " + str(self.port) + " :" + " Insert redirected"
                }
                self.send_to_node(node, response)
        else:
            response = {
                "Data": self.host + " " + str(self.port) + " :" + " No work needs to be done from here"
            }
            self.send_to_node(node, response)

    def delete_post(self, table_name, condition):
        message = {
            "action": "delete",
            "table": table_name,
            "condition": condition
        }

        self.send_to_nodes(message)



    def delete_get(self, message, node):
        if message["table"] in self.tables:
            self.delete(message["table"], message["condition"],True)
            response = {
                "Data": self.host + " " + str(self.port) + " :" + " Done"
            }
            self.send_to_node(node, response)
        else:
            response = {
                 "Data": self.host + " " + str(self.port) + " :" + " No work needs to be done from here"
            }
            self.send_to_node(node, response)

    def update_post(self, table_name, set_value, set_column, condition):
        message = {
            "action": "update",
            "table": table_name,
            "set_value": set_value,
            "set_column": set_column,
            "condition": condition
        }
        self.send_to_nodes(message)

    def update_get(self, message, node):
        if message["table"] in self.tables:
            self.update(message["table"], message["set_value"], message["set_column"], message["condition"],True)
            response = {
                "Data": self.host + " " + str(self.port) + " :" + " Done"
            }
            self.send_to_node(node, response)
        else:
            response = {
                "Data": self.host + " " + str(self.port) + " :" + " No work needs to be done from here"
            }
            self.send_to_node(node, response)
    def save(self):
        '''
        Save db as a pkl file. This method saves the db object, ie all the tables and attributes.
        '''
        for name, table in self.tables.items():
            with open(f'{self.savedir}/{name}.pkl', 'wb') as f:
                pickle.dump(table, f)

    def _save_locks(self):
        '''
        Save db as a pkl file. This method saves the db object, ie all the tables and attributes.
        '''
        with open(f'{self.savedir}/meta_locks.pkl', 'wb') as f:
            pickle.dump(self.tables['meta_locks'], f)

    def load(self, path):
        '''
        Load all the tables that are part of the db (indexs are noted loaded here)
        '''
        for file in os.listdir(path):

            if file[-3:]!='pkl': # if used to load only pkl files
                continue
            f = open(path+'/'+file, 'rb')
            tmp_dict = pickle.load(f)
            f.close()
            name = f'{file.split(".")[0]}'
            self.tables.update({name: tmp_dict},dcheck=True)
            setattr(self, name, self.tables[name])

    def drop_db(self):
        shutil.rmtree(self.savedir)

    #### IO ####

    def _update(self):
        '''
        Update all the meta tables.
        '''
        self._update_meta_length()
        self._update_meta_locks()
        self._update_meta_insert_stack()

    def inheritance(self,name=None, column_names=None, column_types=None, primary_key=None, inherited_tables=None,distributed_key = None,load=None):
        '''
        Creation of 2 temporary lists.
        Temp_cols contains the columns of the new table.
        Temp_types contains the column types of the new table.
        Creation of a table object,which we will return to the create_table function.
        '''
        temp_cols=[]
        temp_types=[]
        for inherits in inherited_tables:#Loop for searching the tables which will be inherited by the new table.
            for col,colt in zip(self.tables[inherits].column_names,self.tables[inherits].column_types):
                if col not in temp_cols:#If the column name does not exists in the temp_cols,the column name and the column type will be appended in the temp lists.
                    temp_cols.append(col)
                    temp_types.append(colt)
                else:#if the column name exists,then we check if the columns with the same name also have the same column type.
                    tindex=temp_cols.index(col)
                    if temp_types[tindex]!=colt:#if they do not have the same type,we raise an error because the two tables can not be merged!
                        raise ValueError(f"Column {col} has a type conflict when trying to merge!")
                        #If the columns have the same type,they will be merged.
            self.tables[inherits].kids_tables.append(name)
        for col,colt in zip(column_names,column_types):#Insert to table the columns which are not inherited from other tables.
            if col not in temp_cols:
                temp_cols.append(col)
                temp_types.append(colt)
            else:
                tindex=temp_cols.index(col)
                if temp_types[tindex]!=colt:
                    raise ValueError(f"Column {col} has a type conflict when trying to merge!")
        return Table(name=name, column_names=temp_cols, column_types=temp_types, primary_key=primary_key,inherited_tables=inherited_tables,kids_tables=[], distributed_key = distributed_key,load=load)

    def create_table(self, name=None, column_names=None, column_types=None, primary_key=None, inherited_tables=None,distributed_key = None, load=None):
        '''
        This method create a new table. This table is saved and can be accessed by
        db_object.tables['table_name']
        or
        db_object.table_name
        '''
        if inherited_tables==None:
            new_table=Table(name=name, column_names=column_names, column_types=column_types, primary_key=primary_key,kids_tables=[], distributed_key = distributed_key,load=load)
        else:
            new_table=self.inheritance(name,column_names,column_types,primary_key,inherited_tables,[],distributed_key,load)
        self.tables.update({name: new_table},dcheck=True)
        # self._name = Table(name=name, column_names=column_names, column_types=column_types, load=load)
        # check that new dynamic var doesnt exist already
        if name not in self.__dir__():
            setattr(self, name, self.tables[name])
        else:
            raise Exception(f'Attribute "{name}" already exists in class "{self.__class__.__name__}".')
        # self.no_of_tables += 1
        print(f'New table "{name}"')
        self._update()
        self.save()

    def partition(self, table_name, partition_key):
        '''
            This method partition the table.
            So it gets the property partition_key with the
            name of the column we use to partition
        '''
        #Checks if partition key is a valid column
        if (partition_key in self.tables[table_name].column_names):
            self.tables[table_name].partition_key = partition_key
            print('Partition successfully created!')
        else:
            print("This partition key does not exist in table columns")
        self._update()
        self.save()
    def create_partition(self, table_name, master_table_name, partition_key_value):
        '''
            This method create a partition(table_name) for the master_table_name.
            So it gets the property partition_key_value which is the value that is based the new partition
            and table_name is added on property partitions of master_table
        '''
        if(self.tables[master_table_name].partition_key == None):
            print("You must partition the table ", master_table_name, " first")
            return
        for partition in self.tables[master_table_name].partitions:
            if(self.tables[partition].partition_key_value == partition_key_value):
                print("There is already a partition with this partition key value")
                return
        given_key_type = type(partition_key_value)
        existed_key_type = self.tables[master_table_name].column_types[self.tables[master_table_name].column_names.index(self.tables[master_table_name].partition_key)]
        if( given_key_type != existed_key_type):
            print("Partition value not equal to partition key type")
            return
        try:
            self.create_table(table_name, [], [], None, [master_table_name])
            self.tables[table_name].partition_key = self.tables[master_table_name].partition_key
            self.tables[table_name].partition_key_value = partition_key_value
            self.tables[table_name].master = master_table_name
            self.tables[master_table_name].partitions.append(table_name)
            self._update()
            self.save()
        except Exception as e:
            print(e)
            print("An error occured,Creation failed")

    def search_partition_table(self,master_table,partition_key_value,operator):
        '''
            This method search for the targeted partition of master_table.
            So it seeks in list partitions for the same partition_key_value
        '''
        tables_list=[]
        for partition in self.tables[master_table].partitions:
            if get_op(operator,self.tables[partition].partition_key_value,partition_key_value):
                tables_list.append(partition)
        return tables_list


    def drop_table(self, table_name):
        '''
        Drop table with name 'table_name' from current db
        '''
        self.load(self.savedir)
        if self.is_locked(table_name):
            return
        if self.tables[table_name].kids_tables!=[]:
            print (f"'{table_name}' table can't be deleted because it's been inherited from other tables!")
        else:
            if self.tables[table_name].inherited_tables!=None:
                for parent in self.tables[table_name].inherited_tables:
                    self.tables[parent].kids_tables.pop(self.tables[parent].kids_tables.index(table_name))
                self._update()
                self.save()
            if self.tables[table_name].kids_tables != []:
                for kid in self.tables[table_name].kids_tables:
                    self.tables[kid].inherited_tables.pop(self.tables[kid].inherited_tables.index(table_name))
                self._update()
                self.save()
            self.tables.pop(table_name)
            delattr(self, table_name)
            if os.path.isfile(f'{self.savedir}/{table_name}.pkl'):
                os.remove(f'{self.savedir}/{table_name}.pkl')
            else:
                print(f'"{self.savedir}/{table_name}.pkl" does not exist.')
            self.delete('meta_locks', f'table_name=={table_name}',True)
            self.delete('meta_length', f'table_name=={table_name}',True)
            self.delete('meta_insert_stack', f'table_name=={table_name}',True)

            self._update()
            self.save()


    def table_from_csv(self, filename, name=None, column_types=None, primary_key=None):
        '''
        Create a table from a csv file.
        If name is not specified, filename's name is used
        If column types are not specified, all are regarded to be of type str
        '''
        if name is None:
            name=filename.split('.')[:-1][0]


        file = open(filename, 'r')

        first_line=True
        for line in file.readlines():
            if first_line:
                colnames = line.strip('\n').split(',')
                if column_types is None:
                    column_types = [str for _ in colnames]
                self.create_table(name=name, column_names=colnames, column_types=column_types, primary_key=primary_key)
                self.lockX_table(name)
                first_line = False
                continue
            self.tables[name]._insert(line.strip('\n').split(','))

        self.unlock_table(name)
        self._update()
        self.save()


    def table_to_csv(self, table_name, filename=None):
        res = ''
        for row in [self.tables[table_name].column_names]+self.tables[table_name].data:
            res+=str(row)[1:-1].replace('\'', '').replace('"','').replace(' ','')+'\n'

        if filename is None:
            filename = f'{table_name}.csv'

        with open(filename, 'w') as file:
           file.write(res)

    def table_from_object(self, new_table):
        '''
        Add table obj to database.
        '''

        self.tables.update({new_table._name: new_table},dcheck=True)
        if new_table._name not in self.__dir__():
            setattr(self, new_table._name, new_table)
        else:
            raise Exception(f'"{new_table._name}" attribute already exists in class "{self.__class__.__name__}".')
        self._update()
        self.save()



    ##### table functions #####

    # In every table function a load command is executed to fetch the most recent table.
    # In every table function, we first check whether the table is locked. Since we have implemented
    # only the X lock, if the tables is locked we always abort.
    # After every table function, we update and save. Update updates all the meta tables and save saves all
    # tables.

    # these function calls are named close to the ones in postgres

    def cast_column(self, table_name, column_name, cast_type):
        '''
        Change the type of the specified column and cast all the prexisting values.
        Basically executes type(value) for every value in column and saves

        table_name -> table's name (needs to exist in database)
        column_name -> the column that will be casted (needs to exist in table)
        cast_type -> needs to be a python type like str int etc. NOT in ''
        '''
        self.load(self.savedir)
        if self.is_locked(table_name):
            return
        self.lockX_table(table_name)
        self.tables[table_name]._cast_column(column_name, cast_type)
        self.unlock_table(table_name)
        self._update()
        self.save()

    def inherited_insert(self,table_name,row):
        '''
        This function takes as arguments the table name and the row that user wants to insert to the table.
        It iterates through the table's list which contains the inherited tables and checks if every
        column name exists in the inherited tables. If it exists, the row will be appended and after that execution
        it will append the table name and the row to a list which will be used to insert the rows into the tables.
        '''
        executions=[]
        for inh in self.tables[table_name].inherited_tables:
            info=[inh]
            inherited_row=[]
            for col in self.tables[inh].column_names:
                if col in self.tables[table_name].column_names:
                    #We search for the index of the column,so we can copy it from the row to the inherited_row.
                    tindex=self.tables[table_name].column_names.index(col)
                    inherited_row.append(row[tindex])
            info.append(inherited_row)
            executions.append(info)
            if self.tables[inh].inherited_tables!=None:
                self.inherited_insert(inh,inherited_row)

        try:#We use this try_except command so if an insert fails,then nothing happens and an exception is raised.
            for exe in executions:
                if self.is_locked(exe[0]):
                    return
                self.lockX_table(exe[0])
                insert_stack=self._get_insert_stack_for_table(exe[0])
                try:
                    self.tables[exe[0]]._insert(exe[1],insert_stack)
                except Exception as e:
                    print(e)
                    print(f'A problem occured with the "{exe[0]}" table')
                self._update_meta_insert_stack_for_tb(exe[0], insert_stack[:-1])
                self.unlock_table(exe[0])
                self._update()
                self.save()
        except Exception as e:
            print (e)
            print ('Abort the mission!')


    def insert(self, table_name, row, lock_load_save=True, dcheck = False):
        '''
        Inserts into table

        table_name -> table's name (needs to exist in database)
        row -> a list of the values that are going to be inserted (will be automatically casted to predifined type)
        lock_load_save -> If false, user need to load, lock and save the states of the database (CAUTION). Usefull for bulk loading
        '''
        if self.distributed and not(dcheck):
            self.insert_post(table_name,row)
            column_name,operator,value = self.tables[table_name]._parse_condition(self.tables[table_name].distributed_key)
            if not(get_op(operator,row[self.tables[table_name].column_names.index(column_name)],value)):
                return;
        if self.tables[table_name].partition_key_value != None:
            print("This is a table partition! You need to insert to master table:"+self.tables[table_name].master)
            return
        if self.tables[table_name].partition_key != None:
            self.insert_partition(table_name, row)
        else:
            if lock_load_save:
                self.load(self.savedir)
                if self.is_locked(table_name):
                    return
                # fetch the insert_stack. For more info on the insert_stack
                # check the insert_stack meta table
                self.lockX_table(table_name)
            #If the tabled has inherited other tables, function inherited_insert will be called and returns a boolean if it succeded.
            if self.tables[table_name].inherited_tables!=None:
                self.inherited_insert(table_name,row)
            insert_stack = self._get_insert_stack_for_table(table_name)
            try:
                self.tables[table_name]._insert(row, insert_stack)
            except Exception as e:
                print(e)
                print('ABORTED')
                # sleep(2)
            self._update_meta_insert_stack_for_tb(table_name, insert_stack[:-1])
            if lock_load_save:
                self.unlock_table(table_name)
                self._update()
                self.save()
    def insert_partition(self,table_name, row, lock_load_save=True):
        '''
            This method insert to a partition.
            So it tries to find a partition with same partition_key_value as the inser row key
            and then insert it
        '''
        part_table_name = ""
        for part_name in self.tables[table_name].partitions:
            if self.tables[part_name].partition_key_value == row[self.tables[table_name].column_names.index(self.tables[table_name].partition_key)]:
                part_table_name = part_name
                break
        if part_table_name == "":
            print("There is no partition for such data to "+ table_name)
            return
        if lock_load_save:
            self.load(self.savedir)
            if self.is_locked(part_table_name):
                return
            # fetch the insert_stack. For more info on the insert_stack
            # check the insert_stack meta table
            self.lockX_table(part_table_name)
        insert_stack = self._get_insert_stack_for_table(part_table_name)
        try:
            self.tables[part_table_name]._insert(row, insert_stack)
        except Exception as e:
            print(e)
            print('ABORTED')
        # sleep(2)
        self._update_meta_insert_stack_for_tb(part_table_name, insert_stack[:-1])
        if lock_load_save:
            self.unlock_table(part_table_name)
            self._update()
            self.save()
    def update_partition(self,table_name,set_value,set_column,condition):
        column_name,operator,value=self.tables[table_name]._parse_condition(condition)
        tables_list=[]
        #If the column_name of the condition is the same with partition key column of the table,we use search_partition function
        #to find the tables we have to make changes!
        if column_name==self.tables[table_name].partition_key:
            tables_list=self.search_partition_table(table_name,value,operator)
            self.load(self.savedir)
        #else we have to iterate all the tables of the partition
        else:
            tables_list=self.tables[table_name].partitions
        #For every table of the tables_list, we update the table using _update_row function in Table class.
        for table in tables_list:
            if self.is_locked(table):
                return
            self.lockX_table(table)
            self.tables[table]._update_row(set_value, set_column, condition)
            self.unlock_table(table)
            self._update()
            self.save()

    def update(self, table_name, set_value, set_column, condition,dcheck=False):
        '''
        Update the value of a column where condition is met.

        table_name -> table's name (needs to exist in database)
        set_value -> the new value of the predifined column_name
        set_column -> the column that will be altered
        condition -> a condition using the following format :
                    'column[<,<=,==,>=,>]value' or
                    'value[<,<=,==,>=,>]column'.

                    operatores supported -> (<,<=,==,>=,>)
        '''
        #If the table is partitioned,we call the update_partition function.
        if self.tables[table_name].partitions!=[]:
            try:
                self.lockX_table(table_name)
                self.update_partition(table_name,set_value,set_column,condition)
                self.unlock_table(table_name)
            except Exception as e:
                print(e)
                print('Problem occured while trying to update!')
        else:
            rows = []
            if (not(self.tables[table_name].inherited_tables == None and self.tables[table_name].kids_tables == [])):
                self.load(self.savedir)
                if self.is_locked(table_name):
                    return
                self.lockX_table(table_name)
                con = []
                con.append(condition)
                rows.append(self.tables[table_name]._update_row_inh(set_value, set_column, con))
                self.unlock_table(table_name)
                self._update()
                self.save()
                condition = []
                if(rows != [[]]):
                    self.update_inherited_tables(table_name, set_value, set_column, condition,rows)
                else:
                    print("0 rows affected")
            else:
                self.load(self.savedir)
                if self.is_locked(table_name):
                    return
                self.lockX_table(table_name)
                self.tables[table_name]._update_row(set_value, set_column, condition)
                if self.distributed and not(dcheck):
                    self.update_post(table_name,set_value, set_column, condition)
                self.unlock_table(table_name)
                self._update()
                self.save()


    def update_inherited_tables(self, table_name, set_value, set_column, condition, rows, check_kids = True, check_parents = True):
        '''
                    This method update to an inherited table.
                    So it updates the table_name and after update kids and parents where it needs
        '''
        if(self.tables[table_name].inherited_tables != None and check_parents):
            for parent in self.tables[table_name].inherited_tables:
                i = 0
                for row in rows:
                    j = 0
                    condition.clear()
                    for r in row[i]:
                        for t in self.tables[parent].column_names:
                            if (r[0] == t):
                                condition.append(r[0] + " == " + str(r[1]))
                        j += 1

                    self.load(self.savedir)
                    if self.is_locked(parent):
                        return
                    self.lockX_table(parent)
                    self.tables[parent]._update_row_inh(set_value, set_column, condition, 1)
                    self.unlock_table(parent)
                    self._update()
                    self.save()
                    i += 1
                if (rows != [[]]):
                    self.update_inherited_tables(parent, set_value, set_column, condition, rows,False)
                else:
                    print("0 rows affected")
        if(self.tables[table_name].kids_tables != [] and check_kids):
            for kid in self.tables[table_name].kids_tables:
                i = 0;
                for row in rows:
                    j = 0
                    condition.clear()
                    for r in row[i]:
                        for t in self.tables[kid].column_names:
                            if (r[0] == t):
                                condition.append(r[0] + " == " + str(r[1]))
                        j += 1

                    self.load(self.savedir)
                    if self.is_locked(kid):
                        return
                    self.lockX_table(kid)
                    self.tables[kid]._update_row_inh(set_value, set_column, condition, 1)
                    self.unlock_table(kid)
                    self._update()
                    self.save()
                    if (rows != [[]]):
                        self.update_inherited_tables(kid, set_value, set_column, condition, rows, True, True)
                    else:
                        print("0 rows affected")
                    i += 1
    def delete_inherited_parents(self,table_name,condition,rows_to_del,already_checked=None):
        if rows_to_del==[]:
            column_name, operator, value = self.tables[table_name]._parse_condition(condition)
            indexes_to_del = []

            column = self.tables[table_name].columns[self.tables[table_name].column_names.index(column_name)]
            for index, row_value in enumerate(column):
                if get_op(operator, row_value, value):
                    indexes_to_del.append(index)
                    rows_to_del.append(self.tables[table_name].data[index])
#-----------------------------------------------------------------------------------------------------------------
        deleted_rows=[]
        if self.tables[table_name].inherited_tables!=None:
            for parent in self.tables[table_name].inherited_tables:
                if not parent==already_checked:
                    self.lockX_table(parent)
                    conditions=[]
                    for row in rows_to_del:
                        i=0
                        for col_row in row:
                            if self.tables[table_name].column_names[i] in self.tables[parent].column_names:
                                conditions.append(self.tables[table_name].column_names[i]+"=="+str(col_row))
                            i+=1
                        deleted_rows.append(self.tables[parent]._delete_where_inherited(conditions))
                    self.unlock_table(parent)
                    self._update()
                    self.save()
                    if self.tables[parent].inherited_tables!=None:
                        self.delete_inherited_parents(parent,condition,deleted_rows)


    def delete_inherited_kids(self,table_name,condition,rows_to_del):
        if rows_to_del==[]:
            column_name, operator, value = self.tables[table_name]._parse_condition(condition)
            indexes_to_del = []

            column = self.tables[table_name].columns[self.tables[table_name].column_names.index(column_name)]
            for index, row_value in enumerate(column):
                if get_op(operator, row_value, value):
                    indexes_to_del.append(index)
                    rows_to_del.append(self.tables[table_name].data[index])
#------------------------------------------------------------------------------------------------------------
        deleted_rows=[]
        if self.tables[table_name].kids_tables!=[]:
            for kid in self.tables[table_name].kids_tables:
                self.lockX_table(kid)
                for row in rows_to_del:
                    conditions=[]
                    i=0
                    for col_row in row:
                        if self.tables[table_name].column_names[i] in self.tables[kid].column_names:
                            conditions.append(self.tables[table_name].column_names[i]+"=="+str(col_row))
                        i+=1
                    deleted_rows.append(self.tables[kid]._delete_where_inherited(conditions))
                self.unlock_table(kid)
                self._update()
                self.save()
                if self.tables[kid].kids_tables!=[]:
                    self.delete_inherited_kids(kid,condition,deleted_rows)

                if len(self.tables[kid].inherited_tables)>1:
                    self.delete_inherited_parents(kid,condition,deleted_rows,table_name)


    def delete(self, table_name, condition,dcheck=False):
        '''
        Delete rows of a table where condition is met.

        table_name -> table's name (needs to exist in database)
        condition -> a condition using the following format :
                    'column[<,<=,==,>=,>]value' or
                    'value[<,<=,==,>=,>]column'.

                    operatores supported -> (<,<=,==,>=,>)
        '''
        if table_name != "meta_indexes" and table_name != "meta_insert_stack" and table_name != "meta_length" and table_name != "meta_locks":
            if self.tables[table_name].partition_key_value != None:
                print("This is a table partition! You need to delete to master table:"+self.tables[table_name].master)
                return
            if self.tables[table_name].partition_key != None:
                self.delete_partition(table_name, condition)
                return;
        else:
            self.load(self.savedir)
            if self.is_locked(table_name):
                return
            self.lockX_table(table_name)
            try:
                if self.tables[table_name].inherited_tables!=None:
                    self.delete_inherited_parents(table_name,condition,[])
                if self.tables[table_name].kids_tables!=[]:
                    self.delete_inherited_kids(table_name,condition,[])
                deleted = self.tables[table_name]._delete_where(condition)
                if self.distributed and (not dcheck):
                    self.delete_post(table_name,condition)
            except Exception as e:
                print (e)
                print("An error occured,no changes made to the database's tables!")
            self.unlock_table(table_name)
            self._update()
            self.save()
            # we need the save above to avoid loading the old database that still contains the deleted elements
            if table_name[:4]!='meta':
                self._add_to_insert_stack(table_name, deleted)
            self.save()

    def delete_partition(self,table_name, condition):
        part_table_name = []
        column_name, operator, value = self.tables[table_name]._parse_condition(condition)
        if(column_name != self.tables[table_name].partition_key):
            for partition in self.tables[table_name].partitions:
                self.load(self.savedir)
                if self.is_locked(partition):
                    return
                self.lockX_table(partition)
                deleted = self.tables[partition]._delete_where(condition)
                self.delete_post(partition, condition)
                self.unlock_table(partition)
                self._update()
                self.save()
                # we need the save above to avoid loading the old database that still contains the deleted elements
                if table_name[:4] != 'meta':
                    self._add_to_insert_stack(partition, deleted)
                self.save()
            return
        for part_name in self.tables[table_name].partitions:
            if get_op(operator,self.tables[part_name].partition_key_value,value):
                part_table_name.append(part_name)
                break
        if part_table_name == "":
            print("There is no partition with such data to delete ")
            return
        self.load(self.savedir)
        if self.is_locked(part_name):
            return
        self.lockX_table(part_name)
        deleted = self.tables[part_name]._delete_where(condition)
        self.delete_post(part_name, condition)
        self.unlock_table(part_name)
        self._update()
        self.save()
        # we need the save above to avoid loading the old database that still contains the deleted elements
        if table_name[:4] != 'meta':
            self._add_to_insert_stack(part_name, deleted)
        self.save()


    def select_partition(self,table_name,columns,condition,order_by,asc,top_k,save_as,return_object):
        column_name=None
        #If the condition is not none, we parse condition so we can use it later.
        if condition is not None:
            column_name,operator,value=self.tables[table_name]._parse_condition(condition)
            partitions_list=[]
            #If the condition column is the same with partition key column, we will call the search_partition function
            #it returns a list of tables according to the condition
            if self.tables[table_name].partition_key==column_name:
                partitions_list=self.search_partition_table(table_name,value,operator)
            #else we have to search all the partition tables.
            else:
                partitions_list=self.tables[table_name].partitions
        else:
            partitions_list=self.tables[table_name].partitions
        tables=[]
        #for every table in partition list,we select the rows we want with the condition and we append a table object to the tables list.
        for table in partitions_list:
            self.lockX_table(table)
            if self._has_index(table) and column_name==self.tables[table].column_names[self.tables[table].pk_idx]:
                index_name = self.select('meta_indexes', '*', f'table_name=={table}', return_object=True,dcheck=True).index_name[0]
                bt = self._load_idx(index_name)
                tables.append(self.tables[table]._select_where_with_btree(columns, bt, condition, order_by, asc, top_k))
            else:
                tables.append(self.tables[table]._select_where(columns, condition, order_by, asc, top_k))
            self.unlock_table(table)

        print(f"\n## {self.tables[table_name]._name} ##")
        #We create the headers of the table we will show
        headers = [f'{col} ({tp.__name__})' for col, tp in zip(self.tables[table_name].column_names, self.tables[table_name].column_types)]
        if self.tables[table_name].pk_idx is not None:
            headers[self.tables[table_name].pk_idx] = headers[self.tables[table_name].pk_idx]+' #PK#'
        non_none_rows=[]
        #for every table in tables list,we select the non_none_rows and print them.
        for table in tables:
            for row in table.data:
                if any(row):
                    non_none_rows.append(row)
        if order_by!=None:
            sort_col=self.tables[table_name].column_names.index(order_by)
            non_none_rows=sorted(non_none_rows,key=lambda l:l[sort_col], reverse=asc)
        print(tabulate(non_none_rows, headers=headers)+'\n')



    def select(self, table_name, columns, condition=None, order_by=None, asc=False,
               top_k=None, save_as=None, return_object=False,dcheck=False):
        '''
        Selects and outputs a table's data where condtion is met.

        table_name -> table's name (needs to exist in database)
        columns -> The columns that will be part of the output table (use '*' to select all the available columns)
        condition -> a condition using the following format :
                    'column[<,<=,==,>=,>]value' or
                    'value[<,<=,==,>=,>]column'.

                    operatores supported -> (<,<=,==,>=,>)
        order_by -> A column name that signals that the resulting table should be ordered based on it. Def: None (no ordering)
        asc -> If True order by will return results using an ascending order. Def: False
        top_k -> A number (int) that defines the number of rows that will be returned. Def: None (all rows)
        save_as -> The name that will be used to save the resulting table in the database. Def: None (no save)
        return_object -> If true, the result will be a table object (usefull for internal usage). Def: False (the result will be printed)

        '''
        self.load(self.savedir)
        if self.is_locked(table_name):
            return
        self.lockX_table(table_name)

        if self.distributed and not(dcheck) and table_name != "meta_indexes" and table_name != "meta_insert_stack" and table_name != "meta_length" and table_name != "meta_locks":
            condition_column,_,_=self.tables[table_name]._parse_condition(condition)
            distributed_key_column,_,_=self.tables[table_name]._parse_condition(self.tables[table_name].distributed_key)
            if condition_column==distributed_key_column:
                self.select_post(table_name,columns,condition,order_by,asc,top_k)

        #If table's partiotions list is not empty, it means that this table is partitioned,so we have to call select_partition function!
        if table_name != "meta_indexes" and table_name != "meta_insert_stack" and table_name != "meta_length" and table_name != "meta_locks":
            if self.tables[table_name].partitions!=[]:
                self.select_partition(table_name,columns,condition,order_by,asc,top_k,save_as,return_object)
                self.unlock_table(table_name)
        else:
            if condition is not None:
                condition_column = split_condition(condition)[0]
            if self._has_index(table_name) and condition_column==self.tables[table_name].column_names[self.tables[table_name].pk_idx]:
                index_name = self.select('meta_indexes', '*', f'table_name=={table_name}', return_object=True, dcheck = True).index_name[0]
                bt = self._load_idx(index_name)
                table = self.tables[table_name]._select_where_with_btree(columns, bt, condition, order_by, asc, top_k)
            else:
                table = self.tables[table_name]._select_where(columns, condition, order_by, asc, top_k)
            self.unlock_table(table_name)
            if save_as is not None:
                table._name = save_as
                self.table_from_object(table)
            else:
                if return_object:
                    return table
                else:
                    table.show()

    def show_table(self, table_name, no_of_rows=None):
        '''
        Print a table using a nice tabular design (tabulate)

        table_name -> table's name (needs to exist in database)
        '''
        self.load(self.savedir)
        if self.is_locked(table_name):
            return
        self.tables[table_name].show(no_of_rows, self.is_locked(table_name))

    def sort(self, table_name, column_name, asc=False):
        '''
        Sorts a table based on a column

        table_name -> table's name (needs to exist in database)
        column_name -> the column that will be used to sort
        asc -> If True sort will return results using an ascending order. Def: False
        '''

        self.load(self.savedir)
        if self.is_locked(table_name):
            return
        self.lockX_table(table_name)
        self.tables[table_name]._sort(column_name, asc=asc)
        self.unlock_table(table_name)
        self._update()
        self.save()

    def inner_join(self, left_table_name, right_table_name, condition, save_as=None, return_object=False):
        '''
        Join two tables that are part of the database where condition is met.
        left_table_name -> left table's name (needs to exist in database)
        right_table_name -> right table's name (needs to exist in database)
        condition -> a condition using the following format :
                    'column[<,<=,==,>=,>]value' or
                    'value[<,<=,==,>=,>]column'.

                    operatores supported -> (<,<=,==,>=,>)
        save_as -> The name that will be used to save the resulting table in the database. Def: None (no save)
        return_object -> If true, the result will be a table object (usefull for internal usage). Def: False (the result will be printed)
        '''
        self.load(self.savedir)
        if self.is_locked(left_table_name) or self.is_locked(right_table_name):
            print(f'Table/Tables are currently locked')
            return

        res = self.tables[left_table_name]._inner_join(self.tables[right_table_name], condition)
        if save_as is not None:
            res._name = save_as
            self.table_from_object(res)
        else:
            if return_object:
                return res
            else:
                res.show()

    def lockX_table(self, table_name):
        '''
        Locks the specified table using the exclusive lock (X)

        table_name -> table's name (needs to exist in database)
        '''
        if table_name[:4]=='meta':
            return

        self.tables['meta_locks']._update_row(True, 'locked', f'table_name=={table_name}')
        self._save_locks()
        # print(f'Locking table "{table_name}"')

    def unlock_table(self, table_name):
        '''
        Unlocks the specified table that is exclusivelly locked (X)

        table_name -> table's name (needs to exist in database)
        '''
        self.tables['meta_locks']._update_row(False, 'locked', f'table_name=={table_name}')
        self._save_locks()
        # print(f'Unlocking table "{table_name}"')

    def is_locked(self, table_name):
        '''
        Check whether the specified table is exclusivelly locked (X)

        table_name -> table's name (needs to exist in database)
        '''
        if table_name[:4]=='meta':  # meta tables will never be locked (they are internal)
            return False

        with open(f'{self.savedir}/meta_locks.pkl', 'rb') as f:
            self.tables.update({'meta_locks': pickle.load(f)},dcheck=True)
            self.meta_locks = self.tables['meta_locks']

        try:
            res = self.select('meta_locks', ['locked'], f'table_name=={table_name}', return_object=True, dcheck=True).locked[0]
            if res:
                print(f'Table "{table_name}" is currently locked.')
            return res

        except IndexError:
            return

    #### META ####

    # The following functions are used to update, alter, load and save the meta tables.
    # Important: Meta tables contain info regarding the NON meta tables ONLY.
    # i.e. meta_length will not show the number of rows in meta_locks etc.

    def _update_meta_length(self):
        '''
        updates the meta_length table.
        '''
        for table in self.tables.values():
            if table._name[:4]=='meta': #skip meta tables
                continue
            if table._name not in self.meta_length.table_name: # if new table, add record with 0 no. of rows
                self.tables['meta_length']._insert([table._name, 0])

            # the result needs to represent the rows that contain data. Since we use an insert_stack
            # some rows are filled with Nones. We skip these rows.
            non_none_rows = len([row for row in table.data if any(row)])
            self.tables['meta_length']._update_row(non_none_rows, 'no_of_rows', f'table_name=={table._name}')
            # self.update_row('meta_length', len(table.data), 'no_of_rows', 'table_name', '==', table._name)

    def _update_meta_locks(self):
        '''
        updates the meta_locks table
        '''
        for table in self.tables.values():
            if table._name[:4]=='meta': #skip meta tables
                continue
            if table._name not in self.meta_locks.table_name:

                self.tables['meta_locks']._insert([table._name, False])
                # self.insert('meta_locks', [table._name, False])

    def _update_meta_insert_stack(self):
        '''
        updates the meta_insert_stack table
        '''
        for table in self.tables.values():
            if table._name[:4]=='meta': #skip meta tables
                continue
            if table._name not in self.meta_insert_stack.table_name:
                self.tables['meta_insert_stack']._insert([table._name, []])


    def _add_to_insert_stack(self, table_name, indexes):
        '''
        Added the supplied indexes to the insert stack of the specified table

        table_name -> table's name (needs to exist in database)
        indexes -> The list of indexes that will be added to the insert stack (the indexes of the newly deleted elements)
        '''
        old_lst = self._get_insert_stack_for_table(table_name)
        self._update_meta_insert_stack_for_tb(table_name, old_lst+indexes)

    def _get_insert_stack_for_table(self, table_name):
        '''
        Return the insert stack of the specified table

        table_name -> table's name (needs to exist in database)
        '''
        return self.tables['meta_insert_stack']._select_where('*', f'table_name=={table_name}').indexes[0]
        # res = self.select('meta_insert_stack', '*', f'table_name=={table_name}', return_object=True).indexes[0]
        # return res

    def _update_meta_insert_stack_for_tb(self, table_name, new_stack):
        '''
        Replaces the insert stack of a table with the one that will be supplied by the user

        table_name -> table's name (needs to exist in database)
        new_stack -> the stack that will be used to replace the existing one.
        '''
        self.tables['meta_insert_stack']._update_row(new_stack, 'indexes', f'table_name=={table_name}')


    # indexes
    def create_index(self, table_name, index_name, index_type='Btree'):
        '''
        Create an index on a specified table with a given name.
        Important: An index can only be created on a primary key. Thus the user does not specify the column

        table_name -> table's name (needs to exist in database)
        index_name -> name of the created index
        '''
        if self.tables[table_name].pk_idx is None: # if no primary key, no index
            print('## ERROR - Cant create index. Table has no primary key.')
            return
        if index_name not in self.tables['meta_indexes'].index_name:
            # currently only btree is supported. This can be changed by adding another if.
            if index_type=='Btree':
                print('Creating Btree index.')
                # insert a record with the name of the index and the table on which it's created to the meta_indexes table
                self.tables['meta_indexes']._insert([table_name, index_name])
                # crate the actual index
                self._construct_index(table_name, index_name)
                self.save()
        else:
            print('## ERROR - Cant create index. Another index with the same name already exists.')
            return

    def _construct_index(self, table_name, index_name):
        '''
        Construct a btree on a table and save.

        table_name -> table's name (needs to exist in database)
        index_name -> name of the created index
        '''
        bt = Btree(3) # 3 is arbitrary

        # for each record in the primary key of the table, insert its value and index to the btree
        for idx, key in enumerate(self.tables[table_name].columns[self.tables[table_name].pk_idx]):
            bt.insert(key, idx)
        # save the btree
        self._save_index(index_name, bt)


    def _has_index(self, table_name):
        '''
        Check whether the specified table's primary key column is indexed

        table_name -> table's name (needs to exist in database)
        '''
        return table_name in self.tables['meta_indexes'].table_name

    def _save_index(self, index_name, index):
        '''
        Save the index object

        index_name -> name of the created index
        index -> the actual index object (btree object)
        '''
        try:
            os.mkdir(f'{self.savedir}/indexes')
        except:
            pass

        with open(f'{self.savedir}/indexes/meta_{index_name}_index.pkl', 'wb') as f:
            pickle.dump(index, f)

    def _load_idx(self, index_name):
        '''
        load and return the specified index

        index_name -> name of the created index
        '''
        f = open(f'{self.savedir}/indexes/meta_{index_name}_index.pkl', 'rb')
        index = pickle.load(f)
        f.close()
        return index
