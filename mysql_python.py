#!/usr/bin/env python
# coding=utf-8

import MySQLdb, sys
from collections import OrderedDict

# Source: https://github.com/nestordeharo/mysql-python-class

class MysqlPython(object):
    """
        Python Class for connecting  with MySQL server and accelerate development project using MySQL
        Extremely easy to learn and use, friendly construction.
    """

    __instance   = None
    __host       = None
    __user       = None
    __password   = None
    __database   = None
    __session    = None
    __connection = None

    def __new__(cls, *args, **kwargs):
        if not cls.__instance or not cls.__database:
             cls.__instance = super(MysqlPython, cls).__new__(cls,*args,**kwargs)
        return cls.__instance
    ## End def __new__

    def __init__(self, host='localhost', user='root', password='', database=''):
        self.__host     = host
        self.__user     = user
        self.__password = password
        self.__database = database
        self.__open()
    # End def __init__

    def __open(self):
        try:
            cnx = MySQLdb.connect(self.__host, self.__user, self.__password, self.__database)
            self.__connection = cnx
            self.__session    = cnx.cursor()
        except MySQLdb.Error as e:
            print "Error %d: %s" % (e.args[0],e.args[1])
    ## End def __open

    def __close(self):
        self.__session.close()
        self.__connection.close()


    # End def __close
    def close(self):
        self.__close()

    def select(self, sql):
        try:
            #self.__open()
            self.__session.execute(sql)
            number_rows = self.__session.rowcount
            number_columns = len(self.__session.description)

            if number_rows >= 1 and number_columns > 1:
                result = [item for item in self.__session.fetchall()]
            else:
                result = [item[0] for item in self.__session.fetchall()]
            #self.__close()

            if len(result) == 1:
                return result[0]
            return result

        except Exception as err:
            return("Something went wrong: {}".format(err))
    # End def select

    def selectone(self, sql, err='#--'):
        return self.select("SELECT IFNULL((" + sql + "), '" + str(err) + "');")

    def update(self, sql):

        self.__open()
        self.__session.execute(sql)
        self.__connection.commit()

        # Obtain rows affected
        update_rows = self.__session.rowcount
        self.__close()

        return update_rows
    # End function update

    def insert(self, sql):

        self.__open()
        self.__session.execute(sql)
        self.__connection.commit()

        # Obtain rows affected
        insert_rows = self.__session.rowcount
        self.__close()

        return insert_rows
    # End def insert

    def delete(self, sql):

        self.__open()
        self.__session.execute(sql)
        self.__connection.commit()

        # Obtain rows affected
        insert_rows = self.__session.rowcount
        self.__close()

        return insert_rows
    # End def delete

# End class

