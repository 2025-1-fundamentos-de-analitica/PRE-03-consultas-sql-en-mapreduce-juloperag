"""Taller evaluable"""

# pylint: disable=broad-exception-raised
# pylint: disable=import-error

from homework.mapreduce import run_mapreduce_job # type: ignore


#
# Columns:
# total_bill, tip, sex, smoker, day, time, size
#

#
# SELECT *, tip/total_bill as tip_rate
# FROM tips;
#
def mapper_query_1(sequence):
    """Mapper"""
    result = []

    for index, (_, row) in enumerate(sequence):
    #Ejemplo de interpretacion
    #sequence = [(10, 'a'), (20, 'b'), (30, 'c')]
    #for index, (_, row) in enumerate(sequence):
    #   print(f"index: {index}, row: {row}")
    #Consola: 
    #index: 0, row: a
    #index: 1, row: b
    #index: 2, row: c
        if index == 0:
            #Agregar una colunma nueva con el "nombre"
            result.append((index, row.strip() + ",tip_rate"))
        else:
            #Almacena cada elementos de la fila en una lista 
            row_values = row.strip().split(",")
            total_bill = float(row_values[0])
            tip = float(row_values[1])
            tip_rate = tip / total_bill
            #Se contruye otra fila con los elementos "row", "tip_rate"
            result.append((index, row.strip() + "," + str(tip_rate)))
    return result


def reducer_query_1(sequence):
    """Reducer"""
    return sequence


#
# SELECT *
# FROM tips
# WHERE time = 'Dinner';
#
def mapper_query_2(sequence):
    """Mapper"""
    result = []
    for index, (_, row) in enumerate(sequence):
        if index == 0:
            result.append((index, row.strip()))
        else:
            row_values = row.strip().split(",")
            if row_values[5] == "Dinner":
                result.append((index, row.strip()))
    return result


def reducer_query_2(sequence):
    """Reducer"""
    return sequence

#
# SELECT *
# FROM tips
# WHERE time = 'Dinner' AND tip > 5.00;
#
def mapper_query_3(sequence):
    """Mapper"""
    result = []
    for index, (_, row) in enumerate(sequence):
        if index == 0:
            result.append((index, row.strip()))
        else:
            row_values = row.strip().split(",")
            if row_values[5] == "Dinner" and float(row_values[1]) > 5.00:
                result.append((index, row.strip()))
    return result


def reducer_query_3(sequence):
    """Reducer"""
    return sequence

#
# SELECT *
# FROM tips
# WHERE size >= 5 OR total_bill > 45;
#
def mapper_query_4(sequence):
    """Mapper"""
    result = []
    for index, (_, row) in enumerate(sequence):
        if index == 0:
            result.append((index, row.strip()))
        else:
            row_values = row.strip().split(",")
            if int(row_values[6]) >= 5 or float(row_values[0]) > 45:
                result.append((index, row.strip()))
    return result


def reducer_query_4(sequence):
    """Reducer"""
    return sequence

#
# SELECT sex, count(*)
# FROM tips
# GROUP BY sex;
#
def mapper_query_5(sequence):
    """Mapper"""
    result = []
    for index, (_, row) in enumerate(sequence):
        if index == 0:
            continue
        row_values = row.strip().split(",")
        result.append((row_values[2], 1))
    return result

def reducer_query_5(sequence):
    """Reducer"""
    counter = dict()
    for key, value in sequence:
        if key not in counter:
            counter[key] = 0
        counter[key] += value
    return list(counter.items())

#
# ORQUESTADOR:
#
def run():
    """Orquestador"""
    #Corremos el run mapreduce 1
    run_mapreduce_job(
        mapper=mapper_query_1,
        reducer=reducer_query_1,
        input_directory="files/input",
        output_directory="files/query_1",
    )   
    #Corremos el run mapreduce 2
    run_mapreduce_job(
        mapper=mapper_query_2,
        reducer=reducer_query_2,
        input_directory="files/input",
        output_directory="files/query_2",
    )   
    #Corremos el run mapreduce 3
    run_mapreduce_job(
        mapper=mapper_query_3,
        reducer=reducer_query_3,
        input_directory="files/input",
        output_directory="files/query_3",
    )    
    #Corremos el run mapreduce 4
    run_mapreduce_job(
        mapper=mapper_query_4,
        reducer=reducer_query_4,
        input_directory="files/input",
        output_directory="files/query_4",
    )
    #Corremos el run mapreduce 5
    run_mapreduce_job(
        mapper=mapper_query_5,
        reducer=reducer_query_5,
        input_directory="files/input",
        output_directory="files/query_5",
    )

if __name__ == "__main__":
    #Corre run
    run()