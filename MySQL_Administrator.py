# -*- coding: utf-8 -*-

import mysql.connector
import mysql
import os


class MySQLAdministrator:
    """Object used for manage database"""

    def __init__(self):

        self.mydb = None
        self.myCursor = None
        self.dataBaseList = []
        self.tableList = []

        self.getHelper()

    def error(self):
        return mysql.connector

    def getHelper(self):

        if "TypeMySQL_Col.csv" not in os.listdir():
            os.system("python MySQL_f_helper.py")

        self.MySQL_helper = {}
        with open("TypeMySQL_Col.csv", "r", encoding="utf-8") as file:
            file = file.read()
            MySQL_helper = file.split("\n")

        self.MySQL_helper[MySQL_helper[0]] = [MySQL_helper[i].split(";") for i in range(1, 11)]
        self.MySQL_helper[MySQL_helper[12]] = [MySQL_helper[i].split(";") for i in range(13, 17)]
        self.MySQL_helper[MySQL_helper[18]] = [MySQL_helper[i].split(";") for i in range(19, 33)]

    def connection(self, host="localhost", user="", password="", db=None):
        """Method used to connect to the database."""

        self.mydb = mysql.connector.connect(host=host,
                                            user=user,
                                            passwd=password,
                                            database=db)
        self.myCursor = self.mydb.cursor()

# ------------------------------------------------------------------------------

    def createDB(self, namedb):
        """Method used to create a new database."""

        self.myCursor.execute(f"CREATE DATABASE {namedb};")

    def checkDB(self):
        """Method used to check existing database."""

        self.myCursor.execute("SHOW DATABASES;")

        for dataBase in self.myCursor:
            if self.dataBaseList.count(dataBase) == 0:
                self.dataBaseList.append(dataBase[0])

        return self.dataBaseList

    def useDB(self, db_name):
        self.myCursor.execute(f"USE {db_name};")
# ------------------------------------------------------------------------------

    def createTable(self, table_name, primaryKey=True):
        """Method used to create a new tab."""

        if primaryKey is True:
            self.myCursor.execute(f"CREATE TABLE {table_name} "
                                  "(id INT AUTO_INCREMENT PRIMARY KEY);")

        else:
            self.myCursor.execute(f"CREATE TABLE {table_name};")

    def dropTable(self, table_name):
        """Method used to drop table."""

        self.myCursor.execute(f"DROP TABLE {table_name};")

    def checkTable(self):
        """Method used to check existing tables."""

        self.tableList = []

        self.myCursor.execute("SHOW TABLES")

        for table in self.myCursor:
            if self.tableList.count(table) == 0:
                self.tableList.append(table[0])

        return self.tableList

# ------------------------------------------------------------------------------

    def createCol(self, table_name="", column_name="", type_column="help"):
        """Method used for create column in table, Set type_column to help for help."""

        from pprint import pprint

        if type_column == "help":
            for keys, values in self.MySQL_helper.items():
                print("\n", keys, end="\n\n")
                for items in values:
                    pprint(items)
        else:

            self.myCursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {type_column};")

    def dropCol(self, table_name, column_name):
        """Method used for drop table"""
        self.myCursor.execute(f"ALTER TABLE {table_name} DROP {column_name};")

    def checkColName(self, table=None):
        """Return column name in db or table"""

        columnDict = {}

        if table is not None:
            # If table is specified.
            self.myCursor.execute(f"DESCRIBE {table};")
            result = self.myCursor.fetchall()
            columnList = []
            for item in result:
                columnList.append(item[0])
            return columnList

        else:
            for table in self.checkTable():
                # If table is specified.
                self.myCursor.execute(f"DESCRIBE {table};")
                result = self.myCursor.fetchall()
                columnList = []
                for item in result:
                    columnList.append(item[0])
                columnDict[table] = columnList
            return columnDict

# ------------------------------------------------------------------------------

    def checkRow(self, table_name, column_name=""):

        print(table_name)
        self.myCursor.execute(f"SELECT * FROM {table_name}")
        rows = self.myCursor.fetchall()
        if rows == []:
            rows = None
        print(rows)
        return rows

    def insert(self, table_name, value, column_name=""):
        """Method used to insert a or many records in the table."""

        if isinstance(value, list):
            if len(column_name) == len(value[0]):
                column_name = "`, `".join(column_name)
                column_name = "`" + column_name + "`"
                print(column_name)
                value_cmd = "%s" + ", %s" * (len(value[0]) - 1)

            else:
                input("Le nombre de valeurs insérées par ligne n'est pas egal "
                      "au nombre de colonne.")

            cmd = f"INSERT INTO {table_name} ({column_name}) VALUES ({value_cmd});"
            print(cmd)
            self.myCursor.executemany(cmd, value)

        elif column_name == "":
            column_name = self.checkColName(table_name)[1]
            cmd = f"INSERT INTO {table_name} (`{column_name}`) VALUE ('{value}');"
            self.myCursor.execute(cmd)

            self.mydb.commit()
            print(self.myCursor.rowcount, "row(s) was inserted")

        else:
            cmd = f"INSERT INTO {table_name} (`{column_name}`) VALUE ('{value}');"
            self.myCursor.execute(cmd)

            self.mydb.commit()
            print(self.myCursor.rowcount, "row(s) was inserted")

    def update(self, table_name, column_name, value, id):
        """Method used to update one value in one column"""
        cmd = f"UPDATE {table_name} SET {column_name} = {value} WHERE `id`={id}"
        self.myCursor.execute(cmd)
        self.mydb.commit()
        print(self.myCursor.rowcount, "row(s) was modified")

# ------------------------------------------------------------------------------
    def linkKey(self, primaryTable, childTable, value, workKey):
        """Method used for add f_key number in table with:
        -primaryTable: table need to add Key
        -childTable: table where they are value searched and this key
        -value: value who want to link
        -workKey: key of actual working row in primaryTable"""
        # Il faut update la ligne 1 de nom
        # Il faut donc creer une méthode pour modifier un ligne.
        rows = self.checkRow(childTable)
        if rows is not None:
            # création d'un dictionnaire {value: key}
            rowdict = {value: key for key, value in rows}
            if value in rowdict:
                # Récupération de la clé
                id = int(rowdict[value])

                print(primaryTable, f"{childTable}_id", id, value)

                self.updateKey(primaryTable, f"{childTable}_id", id, workKey)

        else:
            print("Aucune ligne à lier")

    def updateKey(self, primary_table, column_name, workKey, id_fKey):
        """Method used to update one value in one column"""
        cmd = f"UPDATE {primary_table} SET {column_name} = {id_fKey} WHERE `id`={workKey}"
        self.myCursor.execute(cmd)
        self.mydb.commit()
        print(self.myCursor.rowcount, "row(s) was modified")

    def getKey(self, table_name, value):
        """Method used to get key of value"""
        rows = self.checkRow(table_name)
        for tuples in rows:
            if value in tuples:
                key = tuples[0]
        print("---------------------", rows, value)
        print("---------------------", key)

        return key
