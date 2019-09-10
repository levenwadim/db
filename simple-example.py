import enum
from Db import Db, Model, Column, Integer, String, Enum, DECIMAL

# Инициализируем подключение к базе данных
db = Db(name='database', passwd='123456', echo=True)

# Используемый Enum в таблице ниже
class WhereEnum(enum.Enum):
  all = 0
  title = 1
  desc = 2

# Описание таблицы базы данных
Keyword = Model('keywords', db,
  id = Column(Integer, primary_key=True),
  word = Column(String(255), nullable=False),
  where = Column(Enum(WhereEnum), nullable=False, default=WhereEnum.all),
  budget = Column(DECIMAL(10, 2), nullable=False, default=0.0)
)

# SELECT `where`,`word`,`id`,`budget` FROM `keywords`
Keyword.all()

# SELECT `budget`,`id`,`where`,`word` FROM `keywords` WHERE `where` = "desc"
Keyword.filter_by(where=WhereEnum.desc).all()

# SELECT `where`,`budget`,`word`,`id` FROM `keywords` WHERE `id`=6 LIMIT 1
Keyword.get(6)

# INSERT INTO `keywords`(`where`,`budget`,`word`) VALUES ("all","0.00","test_word")
new_keyword = Keyword(word='test_word')
new_keyword.append()

# UPDATE `keywords` SET `where`="title" WHERE `where` = "desc"'
Keyword.filter_by(where=WhereEnum.desc).update(where=WhereEnum.title)