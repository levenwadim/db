# Mini ORM/Data Mapper

import time, copy, pymysql, types
from datetime import datetime, date
from decimal import Decimal, ROUND_HALF_UP
from dbutils.pooled_db import PooledDB

class Model(dict):
  __tablename__ = None
  __db = None

  __dump = None

  __select_sql = ''

  def __init__(self, tablename, db, **args):
    self.__tablename__ = tablename
    self.__db = db

    if len(args) > 0:
      # Создаем копию начального объекта
      self.__dump = Model(tablename, db)
      for key, value in args.items():
        setattr(self.__dump, key, copy.deepcopy(value))

    for key, value in args.items():
      setattr(self, key, value)

  # Возвращаем копию начального объекта
  def __call__(self, **args): 
    dump = Model(self.__dump.__tablename__, self.__dump.__db)
    attr_list = self.get_attr_list_with_methods()
    for key in attr_list:
      if type(getattr(self.__dump, key)) is types.MethodType:
        object.__setattr__(dump, key, types.MethodType(object.__getattribute__(self.__dump, '__CALL__' + key), dump))
      else:
        attr = object.__getattribute__(self.__dump, key)
        object.__setattr__(dump, key, copy.deepcopy(attr))

        if type(attr._default) is types.LambdaType:
          object.__getattribute__(dump, key)._value = attr._default()

    for key, value in args.items():
      setattr(dump, key, value)

    return dump

  def __bool__(self):
    return True

  def __setattr__(self, key, value):
    if (
      not key.startswith('__') and 
      not key.startswith('_' + self.__class__.__name__) and
      not type(value) is Column
    ):
      attr = object.__getattribute__(self, key)
      if type(attr._type) is Enum:
        if type(value) is int:
          value = attr._type._enum(value)
        elif type(value) is str:
          value = attr._type._enum[value]
      elif type(attr._type) is DECIMAL:
        if type(value) is not Decimal:
          value = Decimal(str(value))
        value = value.quantize(Decimal('1.' + '0' * attr._type._rounded), ROUND_HALF_UP)

      attr._value = value
    else:
      object.__setattr__(self, key, value)
  def __getattribute__(self, key):
    attr = object.__getattribute__(self, key)
    if type(attr) is Column:
      return attr._value

    return attr

  def quote_col(self, name_col):
    return '`' + name_col + '`'
  def convert_val(self, name_col, value):
    attr = object.__getattribute__(self, name_col)

    attr_type = type(attr._type)
    if attr_type is type:
      attr_type = attr._type

    if attr_type is Integer:
      return str(value)
    elif attr_type is DECIMAL:
      if type(value) is not Decimal:
        value = Decimal(str(value))
      value = value.quantize(Decimal('1.' + '0' * attr._type._rounded), ROUND_HALF_UP)
    elif attr_type is Enum:
      if type(value) is int:
        value = attr._type._enum(value)
      value = value.name
    elif attr_type is DateTime and type(value) is datetime:
      value = value.strftime('%Y-%m-%d %H:%M:%S')
    elif attr_type is Date:
      value = value.strftime('%Y-%m-%d')

    return str(value)

  def get_attr_list(self):
    return [
      attr for attr in (set(dir(self)) - set(dir(Model))) 
      if type(getattr(self, attr)) is not types.MethodType and type(getattr(self, attr)) is not types.FunctionType
    ]
  def get_attr_list_with_methods(self):
    return set(dir(self)) - set(dir(Model))

  def get_primary_col(self):
    attr_list = self.get_attr_list()

    for attr in attr_list:
      if object.__getattribute__(self, attr).primary_key:
        return attr

    return None
  def get_attr_with_value_list(self):
    attr_list = self.get_attr_list()

    res_list = []
    for attr in attr_list:
      if object.__getattribute__(self, attr)._value is not None:
        res_list.append(attr)

    return res_list

  @staticmethod
  def get_sql_from_condition_attrs(condition_attrs):
    sql_condition = ''
    if condition_attrs.get('__condition'):
      sql_condition = condition_attrs['__condition']
      del condition_attrs['__condition']

    sql_params_condition = []
    if condition_attrs.get('__condition_params'):
      sql_params_condition = condition_attrs['__condition_params']
      del condition_attrs['__condition_params']

    return (sql_condition, sql_params_condition)


  # Генерируем SELECT запрос
  def generate_select_sql(self):
    self.__select_sql = (
      'SELECT ' + 
      ','.join([self.quote_col(key) for key in self.get_attr_list()]) + 
      ' FROM ' + self.quote_col(self.__tablename__))
  # Генерируем WHERE primary_key = "" LIMIT 1
  def generate_where_primary_key_sql(self, value=None):
    primary_key_col = self.get_primary_col()
    if primary_key_col is None:
      raise Exception('Не было найдено primary_key поле')

    if value is None:
      value = getattr(self, primary_key_col)

    return (
      ' WHERE ' +
      self.quote_col(primary_key_col)
      + '= %s LIMIT 1;',
      [
        self.convert_val(primary_key_col, value),
      ],
    )

  # Получаем кол-во строк в таблице
  def count(self, **condition_attrs):
    sql_condition, sql_params_condition = Model.get_sql_from_condition_attrs(condition_attrs)

    conn = self.__db.connection()
    cur = self.__db.execute('SELECT COUNT(1) FROM ' + self.quote_col(self.__tablename__) + sql_condition, conn,
                            sql_params_condition)

    res = cur.fetchone()

    cur.close()
    conn.close()
    
    return res['COUNT(1)']

  # Получаем True/False о наличии строки
  def exist(self, **condition_attrs):
    if condition_attrs.get('__condition') and ' LIMIT ' not in condition_attrs['__condition']:
      condition_attrs['__condition'] += ' LIMIT 1;'

    # TODO Возможно делается count по всей таблице, вместо проверки наличия одной любой строки согласно __condition
    count = self.count(**condition_attrs)
    return count > 0

  # Получаем строку из базы данных по primary_key
  def get(self, primary_key_value, **fetch_attrs):
    if self.__select_sql == '':
      self.generate_select_sql()

    orm = False
    if fetch_attrs.get('orm') is not None:
      orm = fetch_attrs['orm']
      del fetch_attrs['orm']

    if len(fetch_attrs) > 0:
      sql = (
        'SELECT ' + 
        ','.join([self.quote_col(key) for key, value in fetch_attrs.items()]) + 
        ' FROM ' + self.quote_col(self.__tablename__)
      )
    else:
      sql = self.__select_sql

    sql_condition, sql_params = self.generate_where_primary_key_sql(primary_key_value)
    sql += sql_condition

    conn = self.__db.connection()
    cur = self.__db.execute(sql, conn, sql_params)
    
    res = cur.fetchone()

    cur.close()
    conn.close()

    if not res:
      return None

    if orm:
      for key, value in res.items():
        setattr(self, key, value)

      return self

    return res

  # Получаем первую строку в таблице
  def first(self, **condition_attrs):
    if self.__select_sql == '':
      self.generate_select_sql()

    orm = False
    if condition_attrs.get('orm') is not None:
      orm = condition_attrs['orm']
      del condition_attrs['orm']

    sql_condition, sql_params_condition = Model.get_sql_from_condition_attrs(condition_attrs)

    sql_condition += ' LIMIT 1'

    conn = self.__db.connection()
    
    if len(condition_attrs) > 0:
      sql = (
        'SELECT ' + 
        ','.join([self.quote_col(key) for key, value in condition_attrs.items() if key != 'orm' and key != '__condition']) +
        ' FROM ' + self.quote_col(self.__tablename__)
      )

      cur = self.__db.execute(sql + sql_condition, conn, sql_params_condition)
    else:
      cur = self.__db.execute(self.__select_sql + sql_condition, conn, sql_params_condition)
    
    res = cur.fetchone()

    cur.close()
    conn.close()

    if not res:
      return None

    if orm:
      for key, value in res.items():
        setattr(self, key, value)

      return self

    return res

  # Получаем все строки из таблицы
  def all(self, **condition_attrs):
    if self.__select_sql == '':
      self.generate_select_sql()

    orm = False
    if condition_attrs.get('orm') is not None:
      orm = condition_attrs['orm']
      del condition_attrs['orm']

    sql_condition, sql_params_condition = Model.get_sql_from_condition_attrs(condition_attrs)

    if condition_attrs.get('offset') is not None and condition_attrs.get('limit') is not None:
      sql_condition += ' LIMIT ' + str(condition_attrs['offset']) + ',' + str(condition_attrs['limit'])
      del condition_attrs['offset']
      del condition_attrs['limit']

    if condition_attrs.get('offset'):
      raise Exception('Параметр offset не может передаваться без параметра limit')

    if condition_attrs.get('limit'):
      sql_condition += ' LIMIT ' + str(condition_attrs['limit'])
      del condition_attrs['limit']

    conn = self.__db.connection()
    if len(condition_attrs) > 0:
      sql = (
        'SELECT ' + 
        ','.join([self.quote_col(key) for key, value in condition_attrs.items() if key != 'orm' and key != '__condition']) +
        ' FROM ' + self.quote_col(self.__tablename__)
      )

      cur = self.__db.execute(sql + sql_condition, conn, sql_params_condition)
    else:
      cur = self.__db.execute(self.__select_sql + sql_condition, conn, sql_params_condition)
    
    res = cur.fetchall()

    cur.close()
    conn.close()

    if not res:
      return []

    if orm:
      row_list = []

      for row in res:
        obj = self.__call__()
        for key, value in row.items():
          setattr(obj, key, value)
        row_list.append(obj)

      return row_list

    return res

  # Обновление строки/строк
  def update(self, **condition_attrs):
    sql_condition, sql_params_condition = Model.get_sql_from_condition_attrs(condition_attrs)

    sql = (
      'UPDATE ' + self.quote_col(self.__tablename__) + ' SET ' +
      ','.join([self.quote_col(key) + '=%s' for key, value in condition_attrs.items()])
    )

    sql_params = [self.convert_val(key, value) for key, value in condition_attrs.items()]

    if sql_condition == '':
      sql_condition, sql_params_condition = self.generate_where_primary_key_sql()

    self.__db.query(sql + sql_condition, sql_params + sql_params_condition)

    for key, value in condition_attrs.items():
      setattr(self, key, value)
    
  # Удаление строки/строк
  def delete(self, **condition_attrs):
    sql_condition, sql_params_condition = Model.get_sql_from_condition_attrs(condition_attrs)

    # TODO Добавить обработку LIMIT и OFFSET

    if sql_condition == '':
      sql_condition, sql_params_condition = self.generate_where_primary_key_sql()

    self.__db.query('DELETE FROM ' + self.quote_col(self.__tablename__) + sql_condition, sql_params_condition)

  # Получаем объект запроса с условием согласно переданным полям
  def filter_by(self, **condition_attrs):
    query_list = []
    query_params_list = []

    limit_query = ''
    for key, value in condition_attrs.items():
      if key != 'orm' and key != 'limit':
        if type(value) is dict:
          value_key = list(value.keys())[0]
          query_list.append(self.quote_col(key) + ' ' + value_key + ' %s')
          query_params_list.append(self.convert_val(key, value[value_key]))
        else:
          query_list.append(self.quote_col(key) + ' = %s')
          query_params_list.append(self.convert_val(key, value))
      elif key == 'limit':
        limit_query += ' LIMIT ' + str(value)

    return Query(
      self, 
      ' WHERE ' +
      ' AND '.join(query_list) +
      limit_query,
      query_params_list
    )

  # Получаем объект запроса с сортировкой согласно переданным полям
  def order_by(self, **ordered_attrs):
    return Query(
      self, 
      ' ORDER BY ' +
      ', '.join([self.quote_col(key) + ' ' + value.upper() for key, value in ordered_attrs.items() if key != 'orm' and key != 'limit'])
    )
      
  # Добавляем строку в таблицу
  def append(self, **attrs):
    attr_list = self.get_attr_with_value_list()

    ignore_sql = ''
    if attrs.get('ignore') is not None and attrs['ignore']:
      ignore_sql = 'IGNORE '

    sql = (
      'INSERT ' + ignore_sql + 'INTO ' + 
      self.quote_col(self.__tablename__) + 
      '(' + 
      ','.join([self.quote_col(key) for key in attr_list]) + 
      ') VALUES '
    )

    sql_params = [self.convert_val(key, getattr(self, key)) for key in attr_list]
    sql += '(' + ','.join(['%s' for _ in attr_list]) + ')'

    conn = self.__db.connection()
    cur = self.__db.execute(sql, conn, sql_params)
    cur.close()
    conn.close()

    primary_key_col = self.get_primary_col()
    if primary_key_col is not None:
      setattr(self, primary_key_col, cur.lastrowid)

  # Добавляем метод объекта
  def add_method(self, func, func_name=None):
    if func_name is None:
      func_name = func.__name__

    object.__setattr__(self, func_name, types.MethodType(func, self))
    object.__setattr__(self.__dump, func_name, types.MethodType(func, self))
    object.__setattr__(self.__dump, '__CALL__' + func_name, func)
  # Добавляем статический метод
  def add_staticmethod(self, func, func_name=None):
    if func_name is None:
      func_name = func.__name__

    object.__setattr__(self, func_name, func)


class Query:
  def __init__(self, model, sql, sql_params=None):
    self.model = model
    self.sql = sql
    self.sql_params = sql_params

  def all(self, **fetch_attrs):
    return self.model.all(__condition=self.sql, __condition_params=self.sql_params, **fetch_attrs)

  def first(self, **fetch_attrs):
    return self.model.first(__condition=self.sql, __condition_params=self.sql_params, **fetch_attrs)

  def count(self):
    return self.model.count(__condition=self.sql, __condition_params=self.sql_params)

  def exist(self):
    return self.model.exist(__condition=self.sql, __condition_params=self.sql_params)

  def update(self, **upd_attrs):
    return self.model.update(__condition=self.sql, __condition_params=self.sql_params, **upd_attrs)

  def delete(self):
    return self.model.delete(__condition=self.sql, __condition_params=self.sql_params)

  def order_by(self, **ordered_attrs):
    self.sql += self.model.order_by(**ordered_attrs).sql
    return self


class Column:
  _value = None
  _default = None

  def __init__(self, _type, primary_key=False, nullable=True, default=None, foreign_key=None):
    self._type = _type

    self.primary_key = primary_key
    self.nullable = nullable

    if self.primary_key:
      self.nullable = False

    if foreign_key is not None:
      self.foreign_key = foreign_key

    if default is not None:
      self._default = default
      if type(default) is types.LambdaType:
        default = default()

      if type(_type) is DECIMAL:
        default = Decimal(str(default))
        default = default.quantize(Decimal('1.' + '0' * _type._rounded), ROUND_HALF_UP)

      self._value = default

class Integer:
  pass
class String:
  def __init__(self, length):
    self._length = length
class TEXT:
  pass
class Enum:
  def __init__(self, enum):
    self._enum = enum
class DECIMAL:
  def __init__(self, length, rounded):
    self._length = length
    self._rounded = rounded
class DateTime:
  pass
class Date:
  pass

class Db:
  # Объект DBUtils.PersistentDB для подключения к базе данных
  __conn = None
  
  def connection(self):
    return self.__conn.connection()

  # Инициализируем объект базы данных
  def __init__(self, host='127.0.0.1', name='name', user='root', passwd='', charset='utf8mb4', echo=False):
    self.host = host
    self.name = name
    self.user = user
    self.passwd = passwd
    self.charset = charset

    self.echo = echo

    self.__conn = PooledDB(
      creator=pymysql,
      maxconnections=6, mincached=2, maxcached=5, maxshared=3, blocking=True, maxusage=None, setsession=[], ping=0, # poolDB
      host=self.host, user=self.user, passwd=self.passwd, database=self.name, 
      charset=self.charset, use_unicode=True, cursorclass=pymysql.cursors.DictCursor
    )


  # Выполнение запроса к бд и возвращение курсора
  def query(self, sql, params=None):
    if params is None:
      params = []
    else:
      params = tuple(params)

    conn = self.connection()
    cur = conn.cursor()

    # Вывод выполняемых запросов
    if self.echo:
      print_sql = sql
      if len(params) > 0:
        print_sql = print_sql % params
      print(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f') + ' ' + print_sql)

    try:
      cur.execute(sql, params)
      conn.commit()
    except Exception as e:
      cur.close()
      conn.close()
      raise e

    cur.close()
    conn.close()
    return cur

  # Выполнение запрос к бд по переданному соединению (переданное соединение необходимо закрывать вне этой функции)
  def execute(self, sql, conn, params=None):
    if params is None:
      params = []
    else:
      params = tuple(params)

    cur = conn.cursor()

    # Вывод выполняемых запросов
    if self.echo:
      print_sql = sql
      if len(params) > 0:
        print_sql = print_sql % params
      print(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f') + ' ' + print_sql)

    try:
      cur.execute(sql, params)
      conn.commit()
    except Exception as e:
      cur.close()
      conn.close()
      raise e

    return cur