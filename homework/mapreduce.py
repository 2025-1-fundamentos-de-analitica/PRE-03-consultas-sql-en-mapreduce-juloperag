"""Map/Reduce immplementation"""

# pylint: disable=broad-exception-raised

import fileinput
import glob
import os.path

#La convencion de "_" indica que es una funcion privada
def _load_input(input_directory):
    sequence = []
    files = glob.glob(f"{input_directory}/*")
    with fileinput.input(files=files) as f:
        for line in f:
            sequence.append((fileinput.filename(), line))
    return sequence


def _shuffle_and_sort(sequence):
    return sorted(sequence, key=lambda x: x[00])


def _create_ouptput_directory(output_directory):
    if os.path.exists(output_directory):
        for file in glob.glob(f"{output_directory}/*"):
            os.remove(file)
        os.rmdir(output_directory)
    os.makedirs(output_directory)


def _save_output(output_directory, sequence):
    with open(f"{output_directory}/part-00000", "w", encoding="utf-8") as f:
        for key, value in sequence:
            f.write(f"{key}\t{value}\n")


def _create_marker(output_directory):
    """Create Marker"""
    with open(f"{output_directory}/_SUCCESS", "w", encoding="utf-8") as f:
        f.write("")


#
# Escriba la función job, la cual orquesta las funciones anteriores.

#En esta parte esta la estructura de un MAPREDUCE: "Mapper", "Reducer", archivo de entrada y salida
def run_mapreduce_job(mapper, reducer, input_directory, output_directory):
    """Job"""
    #Se le el archivo y se crea una secuencia donde cada fila corresponde a una fila del cvs
    sequence = _load_input(input_directory)
    #Se aplica el mapper
    sequence = mapper(sequence)
    #Se ordena
    sequence = _shuffle_and_sort(sequence)
    #Se aplica el reducer(En este caso no se devuelve lo mismo)
    sequence = reducer(sequence)
    #Creacion de carpetas y se guarda la nueva sequence modificada.
    _create_ouptput_directory(output_directory)
    _save_output(output_directory, sequence)
    _create_marker(output_directory)
