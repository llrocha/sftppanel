#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import re
import md5
import sys
import glob
import logging
import sqlite3
import traceback
import unicodedata
from datetime import datetime, timedelta


def datPath(geo):
    return '/{0}/promax/dat'.format(geo)

def logPath(geo):
    return '/{0}/promax/int/log/promax_all'.format(geo)

def promaxLogPath(geo):
    return '/{0}/promax/int/dvs'.format(geo)

def twsPath(geo):
  command = 'ps -ef|grep netman|grep "/{0}/"|grep -v grep'.format(geo)
  process = os.popen(command)
  log = process.readline()
  process.close()

  m = re.search(r'(\/.*netman)', log)
  try:
    netman_path = m.group(0)
  except:
    netman_path = ''
  return os.path.dirname(os.path.dirname(netman_path))

def today():
  return datetime.now().strftime('%Y.%m.%d')

def yesterday():
  d = datetime.now() - timedelta(days = 1)
  return d.strftime('%Y.%m.%d')


class SqliteDB:
    def __init__(self, dbname = None, create = False):
        if(dbname != None):
            self.dbname = dbname
            self.create = create
            self.conn = None

    def openDB(self):
        if(self.conn == None):
            if(os.path.isfile(self.dbname) or self.create):
                self.conn = sqlite3.connect(self.dbname)
        return self.conn

    def closeDB(self):
        if(self.conn != None):
            self.conn.close()

    def connection(self):
        return self.openDB()

    def execute(self, sql):
        conn = self.openDB()
        cursor = conn.cursor()
        cursor.execute(sql)
        return cursor.fetchall()        


class TransferLogDBTable(SqliteDB):

    def __init__(self, dbName, create = False):
        ## Database definitions
        self.DATABASE_NAME = dbName

        self.CT_TRANSFER_LOG = """
            CREATE TABLE TRANSFER_LOG (
                ID INTEGER PRIMARY KEY AUTOINCREMENT, 
                MD5SUM TEXT, 
                OCURRENCE TIMESTAMP, 
                LOG_FILE TEXT, 
                MESSAGE TEXT
            );
            """
        self.TF_TRANSFER_LOG = [ 'ID', 'MD5SUM', 'OCURRENCE', 'LOG_FILE', 'MESSAGE' ]
        self.IN_TRANSFER_LOG = 'INSERT INTO TRANSFER_LOG(MD5SUM, OCURRENCE, LOG_FILE, MESSAGE) VALUES (?, ?, ?, ?);'
        self.SA_TRANSFER_LOG = 'SELECT {0} FROM TRANSFER_LOG'
        self.SF_TRANSFER_LOG = 'SELECT {0} FROM TRANSFER_LOG WHERE {1}'

        #if(sys.version_info[0] == 2 and sys.version_info[1] == 6):
        SqliteDB.__init__(self, self.DATABASE_NAME, create)
        #else:
        #    super(TransferLogDBTable, self).__init__(self.DATABASE_NAME)

        conn = self.openDB()
        if(conn != None):
            sql = "SELECT COUNT(1) FROM sqlite_master WHERE type='table' AND name='{0}';".format('TRANSFER_LOG')
            cursor = conn.cursor()
            cursor.execute(sql)        
            table_exists = int(cursor.fetchone()[0])        
            if(not bool(table_exists)):
                print('Tabela não existe! TRANSFER_LOG')
                self.connection().execute(self.CT_TRANSFER_LOG)
                self.connection().commit()
        else:
            raise Exception('Não foi possível abrir [{0}]'.format(self.DATABASE_NAME))
    
    def insertRow(self, MD5SUM = None, OCURRENCE = None, LOG_FILE = None, MESSAGE = None):
        conn = self.openDB()
        cursor = self.connection().cursor()
        sql = self.SF_TRANSFER_LOG.format(', '.join(self.TF_TRANSFER_LOG), "MD5SUM = '{0}'".format(MD5SUM))
        print(sql)
        cursor.execute(sql)
        result = cursor.fetchone()
        if(result == None):
            self.connection().execute(self.IN_TRANSFER_LOG, (MD5SUM, OCURRENCE, LOG_FILE, MESSAGE))
            self.connection().commit()

    def selectAllRows(self):
        conn = self.openDB()
        cursor = conn.cursor()
        sql = self.SA_TRANSFER_LOG.format(', '.join(self.TF_TRANSFER_LOG))
        cursor.execute(sql)
        return cursor.fetchall()

    def selectFilterRows(self, filter):
        conn = self.openDB()
        cursor = conn.cursor()
        sql = self.SF_TRANSFER_LOG.format(', '.join(self.TF_TRANSFER_LOG), filter)
        cursor.execute(sql)
        return cursor.fetchall()

    def lastInsertId(self):
        conn = self.openDB()
        cursor = conn.cursor()
        sql = 'select last_insert_rowid();'
        cursor.execute(sql)
        return cursor.fetchone()


class FileLogDBTable(SqliteDB):
    
    def __init__(self, dbName, create = False):
        ## Database definitions
        self.DATABASE_NAME = dbName
        
        self.CT_FILE_LOG = """
            CREATE TABLE FILE_LOG (
                ID INTEGER PRIMARY KEY AUTOINCREMENT, 
                PID INTEGER, 
                OCURRENCE TIMESTAMP, 
                LOG_FILE TEXT, 
                MESSAGE TEXT
            );
            """
        self.TF_FILE_LOG = [ 'ID', 'PID', 'OCURRENCE', 'LOG_FILE', 'MESSAGE' ]
        self.IN_FILE_LOG = 'INSERT INTO FILE_LOG(PID, OCURRENCE, LOG_FILE, MESSAGE) VALUES (?, ?, ?, ?);'
        self.UP_FILE_LOG = 'UPDATE FILE_LOG SET  {0} = ? WHERE PID = ?;'
        self.SA_FILE_LOG = 'SELECT {0} FROM FILE_LOG'
        self.SF_FILE_LOG = 'SELECT {0} FROM FILE_LOG WHERE {1}'
        
        SqliteDB.__init__(self, self.DATABASE_NAME, create)
        
        conn = self.openDB()
        if(conn != None):
            sql = "SELECT count(1) FROM sqlite_master WHERE type='table' AND name='{0}';".format('FILE_LOG')        
            cursor = conn.cursor()        
            cursor.execute(sql)
            table_exists = int(cursor.fetchone()[0])
            if(not bool(table_exists)):
                print('Tabela não existe! FILE_LOG')
                self.connection().execute(self.CT_FILE_LOG)
                self.connection().commit()
        else:
            raise Exception('Não foi possível abrir [{0}]'.format(self.DATABASE_NAME))

    def insertRow(self, PID = None, OCURRENCE = None, LOG_FILE = None, MESSAGE = None):
        conn = self.openDB()
        cursor = conn.cursor()
        sql = self.SF_FILE_LOG.format(', '.join(self.TF_FILE_LOG), "OCURRENCE = '{0}' AND LOG_FILE='{1}'".format(OCURRENCE, LOG_FILE))
        print('{0} => "{1}" "{2}" "{3}" "{4}" '.format(sql, PID, OCURRENCE, LOG_FILE, MESSAGE))
        cursor.execute(sql)
        result = cursor.fetchone()
        if(result == None):                
            sql = self.IN_FILE_LOG
            print('{0} => "{1}" "{2}" "{3}" "{4}" '.format(sql, PID, OCURRENCE, LOG_FILE, MESSAGE))
            conn.execute(sql, (PID, OCURRENCE, LOG_FILE, MESSAGE))
            conn.commit()
        #else:
        #    sql = self.UP_FILE_LOG.format('= ?, '.join(self.TF_FILE_LOG))
        #    self.connection().execute(sql, (PID, OCURRENCE, LOG_FILE, MESSAGE))

    def selectAllRows(self, fields = None):
        cursor = self.connection().cursor()
        sql = self.SA_FILE_LOG.format(', '.join(self.TF_FILE_LOG))
        cursor.execute(sql)
        return cursor.fetchall()

    def selectFilterRows(self, filter):
        cursor = self.connection().cursor()
        sql = self.SF_FILE_LOG.format(', '.join(self.TF_FILE_LOG), filter)
        cursor.execute(sql)
        return cursor.fetchall()


DAT_PATH = '.'
LOG_PATH = '.'
TWS_PATH = '.'

if(__name__ == '__main__'):
    ## Config
    if(len(sys.argv) == 2):
        GEO = sys.argv[1]
    else:
        raise Exception('Erro parametros incorretos!')

    DAT_PATH = datPath(GEO)
    LOG_PATH = logPath(GEO)
    TWS_PATH = twsPath(GEO)
    if(len(TWS_PATH) == 0):
        TWS_PATH = "/work/analises/20180801_BIGDATA/acsxp2/sp/tws"
    else:
        TWS_PATH = '{0}/stdlist/{1}'.format(TWS_PATH, yesterday())

    dbFilename = '{0}/sftppanel.{1}.{2}.db'.format(DAT_PATH, GEO, yesterday())
    transfer_log = TransferLogDBTable(dbFilename, True)
    file_log = FileLogDBTable(dbFilename, True)
    try:
        print('Verificando: {0}'.format(TWS_PATH))
        m = md5.new()
        for f in glob.glob('{0}/O*'.format(TWS_PATH)):
            print('Encontrado: {0}'.format(f))
            log_file_name = os.path.basename(f)
            fp = open(f)
            for line in fp:
                if(line.find('Baixou') >= 0 or line.find('Subiu') >= 0):
                    line = line.replace('\n', '')
                    hashname = log_file_name + ':' + line
                    m.update(hashname)
                    #print(hashname)
                    timestamp = re.search(r'([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}) ] - (.*)$', line)
                    if(len(timestamp.groups()) == 2):
                        data_hora = timestamp.group(1)
                        mensagem = timestamp.group(2)
                        print('{0}\n"{1}" "{2}" "{3}" "{4}"'.format(
                            transfer_log.IN_TRANSFER_LOG, 
                            m.hexdigest(), 
                            data_hora, 
                            log_file_name, 
                            mensagem
                            ))
                        transfer_log.insertRow(m.hexdigest(), data_hora, log_file_name, mensagem)
                        last_transfer_log_id = transfer_log.lastInsertId()
                        relog = re.search(r'(Subiu|Baixou): ([^ ]*)', mensagem)
                        filename = ''
                        try:
                            filename = relog.group(2)
                        except:
                            filename = ''
                        filename = os.path.basename(filename)

                        lmask = '{0}/*{1}_{2}.log'.format(promaxLogPath(GEO), yesterday().replace('.', ''), filename)
                        fmask = '{0}.*.{1}.log'.format(yesterday().replace('.', ''), filename)
                        fmask = '{0}/{1}'.format(LOG_PATH, fmask)
                        print('Mask de {0}'.format(fmask))
                        for log_promax_all in glob.glob(fmask):
                            print('Log de {0}'.format(log_promax_all))
                            fp = open(log_promax_all)
                            log = fp.readlines()
                            fp.close()
                            log = ''.join(log)
                            print('Mask de {0}'.format(lmask))
                            log_promax = glob.glob(lmask)
                            if(len(log_promax) > 0):
                                logprx = ''
                                for flogprx in log_promax:
                                    fp = open(flogprx)
                                    lines = fp.readlines()
                                    fp.close()
                                    logprx += ''.join(lines)
                                    logprx = logprx.decode('iso-8859-1')
                                    logprx = unicodedata.normalize('NFKD', logprx).encode('ascii','ignore')
                                file_log.insertRow(last_transfer_log_id[0], yesterday(), log_promax_all, log + '###PROMAX###\n' + logprx)
                            else:
                                file_log.insertRow(last_transfer_log_id[0], yesterday(), log_promax_all, log)

            fp.close()
    finally:
        transfer_log.closeDB()
        file_log.closeDB()