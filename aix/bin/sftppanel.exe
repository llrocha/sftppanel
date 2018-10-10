#! /Python/Python27/python.exe 
# -*- coding: utf-8 -*-
# /usr/bin/python

import os
import re
import cgi
import glob
import urllib
import sqlite3
from extract_logs import TransferLogDBTable, FileLogDBTable
from datetime import datetime

#Configuration
ips = {
    'ba': {'ip': '172.22.5.35', 'name': 'BA/NE', 'dbfiles': [] },
    'co': {'ip': '172.22.4.243', 'name': 'CO', 'dbfiles': []  },
    'mg': {'ip': '172.22.4.239', 'name': 'MG', 'dbfiles': []  },
    'no': {'ip': '172.22.4.248', 'name': 'NO', 'dbfiles': []  },
    'pr': {'ip': '172.22.4.249', 'name': 'PR/SPI', 'dbfiles': []  },
    'rj': {'ip': '172.22.4.246', 'name': 'RJ', 'dbfiles': []  },
    'sp': {'ip': '172.22.4.242', 'name': 'SPC', 'dbfiles': []  },
    'su': {'ip': '172.22.4.237', 'name': 'SUL', 'dbfiles': []  },
}

#http://172.22.4.239/cgi-bin/readtws.pl?i=acprxmg&o=dl&f=O16843180.0015&d=2018.08.08
readtws = 'http://{ip}/cgi-bin/readtws.pl?i={instancia}&o=dl&f={file}&d={data}'


MAX_DATES = 31
GEO_DEFAULT = 'ba'
DATA_DEFAULT = datetime.now().strftime('%Y.%m.%d')



class HTMLPanel():

  def __init__(self, geo, date):
    self.geo = geo
    self.date = date
    self.modal_states = {}

    self.MENU_ITEM = '<li class="nav-item"><b><a class="nav-link text-white" href="{root}?geo={geo}">{name}</a></b></li>'
    self.MENU_ACTIVE_ITEM = '<li class="nav-item active"><b><a class="nav-link text-warning" href="{root}?geo={geo}">{name}</a></b></li>'

    self.DATE_MENU = '<button class="dropdown-item" type="button"><a href="{root}?geo={geo}&data={data}">{data}</a></button>'
    self.TABLE_ROW_ERROR = """
      <tr class="text-danger">
        <th scope="row">{id}</th>
        <td>{geo}</td>        
        <td><a href="#" data-toggle="modal" data-target="#Modal_ID{id}" class="btn text-danger active" role="button">{message}</a></td>
      </tr>
    """
    self.TABLE_ROW_WARNING = """
      <tr class="text-warning">
        <th scope="row">{id}</th>
        <td>{geo}</td>
        <td><a href="#" data-toggle="modal" data-target="#Modal_ID{id}" class="btn text-warning active" role="button">{message}</a></td>
      </tr>
    """    
    self.TABLE_ROW_OK = """
      <tr class="text-success">
        <th scope="row">{id}</th>
        <td>{geo}</td>
        <td><a href="#" data-toggle="modal" data-target="#Modal_ID{id}" class="btn text-success active" role="button">{message}</a></td>
      </tr>
    """
    self.MODAL_TEMPLATE = """
    <!-- Modal {id} begin -->
    <div class="modal fade" id="Modal_ID{id}" tabindex="-1" role="dialog" aria-labelledby="Modal_ID{id}" aria-hidden="true">
      <div class="modal-dialog modal-dialog-centered modal-lg" role="document">
        <div class="modal-content">
          <div class="modal-header">
            <h5 class="modal-title" id="Modal_ID{id}">LOG ID {id}</h5>
            <button type="button" class="close" data-dismiss="modal" aria-label="Close">
              <span aria-hidden="true">&times;</span>
            </button>
          </div>
          <div class="modal-body">
            {log}
          </div>
          <div class="modal-footer">
            <button type="button" class="btn btn-secondary" data-dismiss="modal">Close</button>
          </div>
        </div>
      </div>
    </div>
    <!-- Modal {id} end -->
    """

    tf = open(self.getTemplateFile())
    self.template = tf.readlines()
    tf.close()

    self.dbFilename = ''

  def getTemplateFile(self):
    return 'sftppanel_template.html'

  def getModalLogFromDB(self):    
    html = ''
    try:
      self.dbFilename = '{0}/sftppanel.{1}.{2}.db'.format(datPath(), self.geo, self.date)
      if(os.path.exists(self.dbFilename)):
        file_log = FileLogDBTable(self.dbFilename)
        rows = file_log.selectAllRows()
        for row in rows:
          log = row[4]
          self.modal_states[row[1]] = not ('err' in log)
          log = log.split('\n')
          html += self.MODAL_TEMPLATE.format(
            id = row[1], 
            log = '<br>'.join(log)
            #log = row[4]
          )
    except:
      pass

    return html

  def getInfoFromDB(self):
    html = ''
    try:
      self.dbFilename = '{0}/sftppanel.{1}.{2}.db'.format(datPath(), self.geo, self.date)
      if(os.path.exists(self.dbFilename)):
        transfer_log = TransferLogDBTable(self.dbFilename)
        rows = transfer_log.selectAllRows()
        for row in rows:
          if(row[0] in self.modal_states):
            if(self.modal_states[row[0]]):
              html += self.TABLE_ROW_OK.format(
                id = row[0],
                geo = self.geo.upper(), 
                root = '', 
                timestamp = row[2], 
                filename = row[3], 
                message = row[4], 
                data = self.date
              )
            else:
              html += self.TABLE_ROW_ERROR.format(
                id = row[0],
                geo = self.geo.upper(), 
                root = '', 
                timestamp = row[2], 
                filename = row[3], 
                message = row[4], 
                data = self.date
              )
          else:
            html += self.TABLE_ROW_WARNING.format(
              id = row[0],
              geo = self.geo.upper(), 
              root = '', 
              timestamp = row[2], 
              filename = row[3], 
              message = row[4], 
              data = self.date
            )
    except:
      pass

    return html

  def getPanel(self):
    html = ''
    menu_itens = ''
    menu_dates = ''
    for geo in sorted(ips):
      if (self.geo == geo):
        menu_itens += self.MENU_ACTIVE_ITEM.format(root='', geo=geo, name=ips[geo]['name'])
        files = ips[geo]['dbfiles']
        count = 0
        for db in sorted(getDBFiles(geo), reverse = True):
          files.append(db)
          d = os.path.basename(db).replace('sftppanel.{0}.'.format(geo), '').replace('.db', '')
          menu_dates += self.DATE_MENU.format(root='', geo=geo, data=d)
          count += 1
          if(count >= MAX_DATES):
            break
      else:
        menu_itens += self.MENU_ITEM.format(root='', geo=geo, name=ips[geo]['name'])
  
    if(len(menu_dates) == 0):
      menu_dates = 'Sem datas dispon&iacute;veis'

    modal_info = self.getModalLogFromDB()
    table_rows = self.getInfoFromDB()
    for line in self.template:
      html += line.format(
        menu_geos = menu_itens, 
        menu_dates = menu_dates, 
        data = self.date, 
        table_rows = table_rows,
        modal_info = modal_info,
        root = ''
      )

    return html


#Functions
def datPath():
  return '/ac/promax/dat'

def getTemplateFile():
  return 'sftppanel_template.html'  

def getDBFiles(geo):
  result = []
  mask = '{0}/sftppanel.{1}.????.??.??.db'.format(datPath(), geo)
  for db in glob.glob(mask):
    result.append(db)
  return result
  
def valueFromCGI(param, default = ''):
  form = cgi.FieldStorage()
  result = form.getvalue(param)
  if(result == None):
    result = default
  return result

def today():
  return datetime.now().strftime('%Y.%m.%d')

def printEnv(messages):
  print('<!--')
  print('root={0}<br>'.format(valueFromCGI('root')))
  print('geo={0}<br>'.format(valueFromCGI('geo')))
  print('name={0}<br>'.format(valueFromCGI('name')))
  print('data={0}<br>'.format(valueFromCGI('data')))
  print('message={0}<br>'.format(valueFromCGI('message')))
  for message in messages:
    print('message={0}<br>'.format(message))
  print('-->')
#Main

def findDbFiles(geo):
  try:
    url = 'http://{ip}/cgi-bin/verifica_versao.exe?f=/{geo}/promax/dat/sftppanel.??.????.??.??.db'
    url = url.format(ip=ips[geo]['ip'], geo=geo)
    fh = urllib.urlopen(url)
    for line in fh:
      items = line.split()
      if(len(items) == 4):
        filename = '{0}/{1}'.format(datPath(), os.path.basename(items[3]))
        if(not os.path.exists(filename)):
          fp = open('{0}.log'.format(filename), 'w')          
          url = 'http://{ip}/database/{file}'.format(ip=ips[geo]['ip'], file=os.path.basename(filename))
          fp.write('Gravando: {0} de {1}'.format(filename, url))
          fp.close()
          urllib.urlretrieve (url, filename)
        #print('{0} -> {1}'.format(url, filename))
    fh.close()
  except Exception as e:
    print("Exception error({0})".format(e.message))

try:
  for geo in ips:
    findDbFiles(geo)
  
  panel = HTMLPanel(valueFromCGI('geo', GEO_DEFAULT), valueFromCGI('data', DATA_DEFAULT))

  print(panel.getPanel())
  messages = []
  messages.append(panel.dbFilename)
  printEnv(messages)
except Exception as e:
  print('Content-type: text/plain\n\n\n')
  print("Exception error({0})".format(e.message))
  


