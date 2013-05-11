# coding: utf-8
"""
Библиотека для работы с clickatell.com HTTP API

"""

from urllib import urlencode
from urllib2 import urlopen


#
# Константы
#

# От кого SMS
FROM = 'Client'
# Базовый путь для запросов Clickatell API
BASE_URL = 'https://api.clickatell.com/http/'


#
# Исключения
#

class BaseClickatellError(Exception):
    """Базовый класс для исключительных ситуаций в модуле
    
    code - код ошибки
    description - описание ошибки
    
    """
    
    def __init__(self, code, description):
        self.code = code
        self.description = description
    
    def __str__(self):
        return '%s, %s' % (self.code, self.description)

class AuthError(BaseClickatellError):
    """Ошибка соединения
    
    code - код ошибки
    description - описание ошибки
    
    """
    pass
        

class NotImplementedError(BaseClickatellError):
    """Ошибка, метод не реализован"""
    pass

#
# Классы
#

class Clickatell:
    """API для работы с clickatell.com HTTP API
    
    Сообщения отправляются только в ASCII.
    
    TODO:
    Все методы API могуд получать доп. параметры, например, при отправке смс
    можно указать имя пользователя, пароль, и api_id. В данном случае нет
    необходимости делать предварительную авторизацию.
    
    """
    
    def __init__(self, api_id, user, password):
        self.api_id = api_id
        self.user = user
        self.password = password
        self.base_url = BASE_URL
        self.session_id = None
    
    @classmethod
    def connect(cls, api_id, user, password):
        """Создаем соединение с сервером
        
        Может вызвать исключение AuthError.
        
        """
        
        instance = cls(api_id, user, password)
        instance.auth()
        
        return instance
    
    @classmethod
    def connect_default(cls):
        """Создаем соединение с настройками по умолчанию
        
        TODO: а вообще надо ли? слишком специфично будет.
        
        """
        
        raise NotImplementedError()
    
    def query(self, command, **kwargs):
        """Отправка запроса на сервер
        
        Параметры в словаре kwargs.
        
        Возвращает обработаный результат.
        
        """
        
        return self.parse_reply(self.query_raw(command, **kwargs))
    
    def query_raw(self, command, **kwargs):
        """Создание URL и непосредственная отправка запроса
        
        Возвращает строку ответа сервера, без разбора.
        
        Может вызвать URLError.
        
        TODO: 
            - может вернуться многострочный ответ, если идет отправка на
              несколько телефонов и т.п.
        
        """
        
        url = self.build_url(command, **kwargs)
        return urlopen(url).read()
    
    def build_url(self, command, **kwargs):
        """Генерация URL для запроса"""
        
        url = '%s%s' % (self.base_url, command)
        if self.session_id is not None:
            kwargs.update({'session_id': self.session_id})
        query_string = urlencode(kwargs.items())
        return '%s?%s' % (url, query_string)
    
    def parse_reply(self, reply):
        """Обработка ответа с сервера
        
        Возвращает результат выполнения команды либо вызывает исключение
        
        по простому можно проверять только OK и ERR.
        по полной - еще и коды с описанием.
        при проверке покрытия только строка, без кода.
        
        """
        
        if reply[:3] == 'ERR':
            (code, description) = reply[5:].split(',')
            raise BaseClickatellError(code, description)
        
        return reply[4:]
    
    #
    # Команды API
    #
    
    def auth(self):
        """Авторизация на сервере
        
        Может вызвать AuthError
        
        """
        
        # отправляем запрос
        result = self.query_raw('auth', api_id = self.api_id, user = self.user,
            password = self.password)

        # TODO: вынести в parse_reply() ???
        # по простому можно проверять только OK и ERR
        # по полной - еще и коды с описанием
        if result[:2] == 'OK':
            self.session_id = result[4:]
        else:
            (code, description) = result[5:].split(',')
            raise AuthError(code, description)
    
    def ping(self, **kwargs):
        """Ping server
        
        Возвращает True или вызывает исключение

        """
        
        self.query('ping')

        return True
    
    def sendmsg(self, to, text, **kwargs):
        """Send a message
        
        Может вызвать исключение:
            - BaseClickatellErro
            - UnicodeEncodeError
        
        TODO: надо что-то с русским придумать
        
        """
        
        # пробуем получить ascii
        try:
            text = text.encode()
        except UnicodeEncodeError:
            raise
    
        if 'from' not in kwargs:
            kwargs['from'] = FROM
        result = self.query('sendmsg', to = to, text = text, **kwargs)
        
        return result
    
    def querymsg(self, apimsgid, **kwargs):
        """Query a message

        """
        
        raise NotImplementedError()
    
    

