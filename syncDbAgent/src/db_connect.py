import pymssql
import pymysql


class MsDbConn(object):

    def __init__(self, host, username, password, database):
        self.conn = pymssql.connect(host=host, user=username, password=password, database=database)

    def query_all(self, sql):
        cursor = self.conn.cursor()
        cursor.execute(sql)
        return cursor.fetchall()

    def close(self):
        self.conn.close()


class MySQLDbConn(object):

    def __init__(self, host, username, password, database):
        self.conn = pymysql.connect(host=host, user=username, password=password, database=database)

    def query_all(self, sql):
        cursor = self.conn.cursor()
        cursor.execute(sql)
        return cursor.fetchall()

    def close(self):
        self.conn.close()
