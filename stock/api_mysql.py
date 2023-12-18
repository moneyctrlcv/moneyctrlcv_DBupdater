import pathlib
import json
import pymysql
from pymysql.cursors import DictCursor

# load DB settings
filepath = str(pathlib.Path(__file__).parent.resolve())
DB = json.load(open(filepath + '/DB.json'))

def mysql_create_session():
  conn = pymysql.connect(host=DB['host'], port=DB['port'],
                      user=DB['user'], passwd=DB['passwd'],
                      db=DB['db'], charset=DB['charset'])
  cur = conn.cursor(DictCursor)
  return conn, cur