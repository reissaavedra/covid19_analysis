import pandas as pd
import numpy as np
import psycopg2
import pandas.io.sql as sqlio
from tensorflow.keras.callbacks import Callback
import re
import nltk
nltk.download('stopwords')
from nltk.corpus import stopwords
from nltk.stem.porter import PorterStemmer
from collections import Counter


HOST = '127.0.0.1'
DATABASE_COVID = 'covid_tracking'
DATABASE_COAID = 'covid19_coaid'
USER = 'postgres'
PASSWORD = 'reyson07'

def get_connection_covid_tracking():
    return psycopg2.connect(host=HOST,
                            database=DATABASE_COVID,
                            user=USER,
                            password=PASSWORD)

def get_connection_covid_coaid():
    return psycopg2.connect(host=HOST,
                            database=DATABASE_COAID,
                            user=USER,
                            password=PASSWORD)

def get_df_sql(sql,conn):
    return sqlio.read_sql_query(sql, conn)

class PrintDots(Callback):
    #Imprimir puntos para monitorear el progreso del ajuste.
    def on_epoch_end(self, epoch, logs):
        logs = logs or {}
        value = logs.get('val_loss')
        if epoch % 10 == 0:
            print(' epochs = ', epoch, ' val_loss = ', value)
        print('*', end='')
        
class TerminateOnBaseline(Callback):
    # Callback que termina el entrenamiento cuando el valor monitoreado alcanza una línea de base especificada
    def __init__(self, monitor='val_loss', patience=10, factor=100.):
        super(TerminateOnBaseline, self).__init__()
        self.monitor = monitor
        self.baseline = np.Inf
        self.patience = patience
        self.wait = 0
        self.stopped_epoch = 0
        self.best_epoch = 0
        self.best = np.Inf
        self.best_weights = None
        self.factor = factor

    def on_epoch_end(self, epoch, logs=None):
        logs = logs or {}
        value = logs.get(self.monitor)
        if epoch == 0:
            self.baseline = value/self.factor
        if np.less(value, self.best):
            self.best = value
            self.best_epoch = epoch
            self.wait = 0
            # Registre los mejores pesos si los resultados son mejores (menos).
            self.best_weights = self.model.get_weights()
        else:
            self.wait += 1
        if value is not None:
            if value <= self.baseline and self.wait >= self.patience:
                self.stopped_epoch = epoch
                print(f'\n{epoch} : Alcanzó la línea base y finalizó el entrenamiento')
                self.model.stop_training = True
                print(f'Restoring model weights from the end of epoch: {str(self.best_epoch)}')
                self.model.set_weights(self.best_weights)
            elif self.wait >= self.patience:
                self.baseline *= 2.5
                self.wait = self.patience/2
                print('Línea base incrementada')
                
                
                
def get_regex_strings():
    return (
        # Números telefónicos:
        r"""
        (?:
          (?:            # (international)
            \+?[01]
            [\-\s.]*
          )?
          (?:            # (area code)
            [\(]?
            \d{3}
            [\-\s.\)]*
          )?
          \d{3}          # exchange
          [\-\s.]*
          \d{4}          # base
        )"""
        ,
        # Emoticonos:
        r"""
        (?:
          [<>]?
          [:;=8]                     # eyes
          [\-o\*\']?                 # optional nose
          [\)\]\(\[dDpP/\:\}\{@\|\\] # mouth
          |
          [\)\]\(\[dDpP/\:\}\{@\|\\] # mouth
          [\-o\*\']?                 # optional nose
          [:;=8]                     # eyes
          [<>]?
        )"""
        ,
        # HTML tags:
         r"""<[^>]+>"""
        ,
        # Twitter users:
        r"""(?:@[\w_]+)"""
        ,
        # Twitter hashtags:
        r"""(?:\#+[\w_]+[\w\'_\-]*[\w_]+)"""
        ,
        # Tipos de palabras restantes:
        r"""
        (?:[a-z][a-z'\-_]+[a-z])       # Words with apostrophes or dashes.
        |
        (?:[+\-]?\d+[,/.:-]\d+[+\-]?)  # Numbers, including fractions, decimals.
        |
        (?:[\w_]+)                     # Words without apostrophes or dashes.
        |
        (?:\.(?:\s*\.){1,})            # Ellipsis dots.
        |
        (?:\S)                         # Everything else that isn't whitespace.
        """
        )

def html2unicode(s):
    # Estos son para regularizar entidades HTML a Unicode:
    html_entity_digit_re = re.compile(r"&#\d+;")
    html_entity_alpha_re = re.compile(r"&\w+;")

    # Primero los dígitos:
    ents = set(html_entity_digit_re.findall(s))
    if len(ents) > 0:
        for ent in ents:
            entnum = ent[2:-1]
            try:
                entnum = int(entnum)
                s = s.replace(ent, unichr(entnum))
            except:
                pass

    # Ahora las versiones alfa:
    ents = set(html_entity_alpha_re.findall(s))
    ents = filter((lambda x : x != "&amp;"), ents)

    for ent in ents:
        entname = ent[1:-1]
        try:
            s = s.replace(ent, unichr(htmlentitydefs.name2codepoint[entname]))
        except:
            pass
        s = s.replace("&amp;", " and ")

    return s

def tokenize(s):
    # Tokenizing regex:
    word_re = re.compile(r"""(%s)""" % "|".join(get_regex_strings()), re.VERBOSE | re.I | re.UNICODE)

    # La cadena de emoticonos obtiene su propia regex para que podamos preservar el caso para ellos según sea necesario
    emoticon_re = re.compile(get_regex_strings()[1], re.VERBOSE | re.I | re.UNICODE)

    # Asegurar unicode:
    try:
        s = unicode(s)
    except UnicodeDecodeError:
        s = str(s).encode('string_escape')
        s = unicode(s)

    # Corregir entidades de caracteres HTML:
    s = html2unicode(s)

    # Tokenización:
    words = word_re.findall(s)

    # Es posible modificar el caso, pero evita cambiar emoticonos como: D por: d:
    words = map((lambda x : x if emoticon_re.search(x) else x.lower()), words)
    return words

def remove_pattern(input_txt, pattern):
    r = re.findall(pattern, input_txt)
    for i in r:
        input_txt = re.sub(i, '', input_txt)
    return input_txt

def get_data_corpus(data_remove):
    corpus = []
    ps = PorterStemmer()
    for i in range(0, len(data_remove)):
        tweet = data_remove[i][0]
        tweet = tweet.lower()
        tweet = re.sub('rt', '', tweet)
        tweet =  re.sub(r"http\S+", "", tweet)
        tweet = re.sub('[^a-zA-Z0-9]', ' ', tweet)   
        tweet = tweet.split(' ')
        tweet = [ps.stem(word) for word in tweet if not word in set(stopwords.words('english'))]
        tweet = ' '.join(tweet)
        corpus.append(tweet)
    return corpus






