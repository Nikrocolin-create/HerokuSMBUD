import random


def serialize_person(person):
    return {
        'taxCode': person['taxCode'],
        'name': person['name'],
        'surname': person['surname']
    }


def serialize_place(place):
    return {
        'name': place['name'],
        'x': random.uniform(-1, 1) * 180.0,
        'y': random.uniform(-1, 1) * 180.0
    }


def serialize_green(green_pass):
    return {
        'date_start': str(green_pass['date1']),
        'date_end': str(green_pass['date2']),
        'type': green_pass['type']
    }


def serialize_infection(infection):
    return {
        'date': infection['date_of_infection']
    }
