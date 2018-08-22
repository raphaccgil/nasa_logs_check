#!/usr/bin/env python

from pyspark import SparkContext
import re
from datetime import datetime

sc = SparkContext("local", "First App")

def convertColumn(df, names, newType):
    for name in names:
        df = df.withColumn(name, df[name].cast(newType))
    return df

def myfun(s):
    date = s[0]
    return date

def host_info(s):
    return s[0]

def treat_data(s):
    time = s[3]
    error = s[9]
    return time, error

def collect_url(s):
    url = s[0]
    error = s[9]
    return url, error


def check_bytes(s):
    try:
        var = s[10]
        if var == "-":
            var = 0
        else:
            var = int(s[10])
    except:
        # Nesse caso existiu alguns dados com informacao estranha
        print(s)
        var = 0
    return var


def time_verification(s):
    date_collect = s[0]
    result = datetime.strptime(date_collect[:11], "%d/%b/%Y")
    return result

def collect_only_urls(s):
    return (s[0])

rdd1 = sc.textFile('/Users/raphacgil/Documents/Raphael/ML_training/SparkTr/NASA_access_log_Jul95')
rdd2 = sc.textFile('/Users/raphacgil/Documents/Raphael/ML_training/SparkTr/NASA_access_log_Aug95')

rdd = rdd1.union(rdd2)


def parse_log_nasa(str):

    ## Durante os testes foi encontrado logs com problemas, por isso realizei um filtro dos
    ## logs com problemas
    ## Utilizado biblioteca re devido ao entendimento de caracteres genericos e espacos em branco
    mtc_logs= re.match('^(\S+) (\S+) (\S+) \[(\S+) [-](\S+)\] "(\S+)\s*(\S+)\s*(\S+)\s*([/\w\.\s*]+)?\s*"* (\d{3}) (\S+)$',str)
    if mtc_logs is None:
        mtc_logs = re.match('^(\S+) (\S+) (\S+) \[(\S+) [-](\S+)\] "(\S+)\s*([/\w\.]+)>*([\w/\s\.]+)\s*(\S+)\s*(\d{3})\s*(\S+)$',str)
        if mtc_logs is None:
            mtc_logs = re.match('^(\S+) (\S+) (\S+) \[(\S+) [-](\S+)\] "(\S+)\s*(\S+)\s+(\S+)\s+<(\S+)>" (\S+) (\S+)$',str)
            if mtc_logs is None:
                print (str)
                return (None, 0)
            else:
                return (mtc_logs.groups(), 1)
        else:
            return (mtc_logs.groups(), 1)
    else:
        return (mtc_logs.groups(), 1)

data_clean_rdd = rdd.map(lambda line: parse_log_nasa(line))\
    .filter(lambda line: line[1] == 1)\
    .map(lambda line : line[0])
data_clean_rdd.cache()
## host
count1 = data_clean_rdd.map(lambda s: host_info(s))
## comando abaixo gera o contador de palavras, no caso host
count1_rep = count1.map(lambda word: (word, 1))\
    .reduceByKey(lambda a, b: a + b)
print("Numero de hosts unicos: {}".format(count1_rep.count()))  # numero de hosts unicos

# ver 404
count = data_clean_rdd.map(lambda s: treat_data(s))
## filtro somente dos logs com 404
table2 = count.filter(lambda x: '404' in x[1])
print("Quantidade de erros com 404: {}".format(table2.count())) # resposta de 404

# criar dataframe entre os dias
temp_time = table2.map(lambda x: time_verification(x))
## criacao de quantidade de amostragem no dia, utilizando a amostragem que esta com 404
result_time = temp_time.map(lambda line: (line, 1))\
    .reduceByKey(lambda a, b: a + b)
check = result_time.sortBy(lambda a: a[0])
val_total = check.count()
print('date samples')
print(check.take(val_total))

# ver os 5 URLS com valor 404
count2 = data_clean_rdd.map(lambda s: collect_url(s))
count2_1 = count2.filter(lambda x: '404' in x[1])
## criacao de contagem de URL e , ao mesmo tempo, verificar os 5 maiores
date2_val_treat = count2_1.map(lambda line: (line[0], 1))\
    .reduceByKey(lambda a, b: a + b)
output = date2_val_treat.takeOrdered(5, lambda x: -x[1])
count_val = count2_1.count()
print("Quantidade de erros com 404 confirmacao: {}".format(count_val)) # resposta de 404
print("5 URLS com maiores logs 404 sao: {}".format(output))

# quantidade acumulada de bytes em casos onde houver gasto em bytes
count3 = data_clean_rdd.map(lambda s: check_bytes(s))\
    .sum()
print("Soma dos bytes: {}".format(count3))



