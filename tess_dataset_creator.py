import csv
import requests
from datetime import date
import argparse
import os
import datetime
import pytz
from tzwhere import tzwhere


parser = argparse.ArgumentParser()
parser.add_argument("--date")
args = parser.parse_args()

list_photometers = []

header = {}
header_keys = []
header_list = []

file_path = os.getenv('BASE_STORAGE_PATH', "")

with open("header.dat") as header_file:
    header_keys.append("definition")
    header[header_keys[0]] = header_file.readline().rstrip()

    header_keys.append("definition_url")
    header[header_keys[1]] = header_file.readline().rstrip()

    header_keys.append("number_of_lines")
    header[header_keys[2]] = header_file.readline().rstrip()

    header_keys.append("data_license")
    header[header_keys[3]] = header_file.readline().rstrip()

    header_keys.append("device_type")
    header[header_keys[4]] = header_file.readline().rstrip()

    header_keys.append("instrument_id")
    header[header_keys[5]] = header_file.readline().rstrip()

    header_keys.append("data_supplier")
    header[header_keys[6]] = header_file.readline().rstrip()

    header_keys.append("location_name")
    header[header_keys[7]] = header_file.readline().rstrip()

    header_keys.append("position")
    header[header_keys[8]] = header_file.readline().rstrip()

    header_keys.append("local_timezone")
    header[header_keys[9]] = header_file.readline().rstrip()

    header_keys.append("time_synchronization")
    header[header_keys[10]] = header_file.readline().rstrip()

    header_keys.append("stationary_position")
    header[header_keys[11]] = header_file.readline().rstrip()

    header_keys.append("fixed_direction")
    header[header_keys[12]] = header_file.readline().rstrip()

    header_keys.append("number_of_channels")
    header[header_keys[13]] = header_file.readline().rstrip()

    header_keys.append("filters_per_channel")
    header[header_keys[14]] = header_file.readline().rstrip()

    header_keys.append("measurement_direction")
    header[header_keys[15]] = header_file.readline().rstrip()

    header_keys.append("field_of_view")
    header[header_keys[16]] = header_file.readline().rstrip()

    header_keys.append("number_of_fields_per_line")
    header[header_keys[17]] = header_file.readline().rstrip()

    header_keys.append("mac")
    header[header_keys[18]] = header_file.readline().rstrip()

    header_keys.append("firmware_version")
    header[header_keys[19]] = header_file.readline().rstrip()

    header_keys.append("cover_offset_value")
    header[header_keys[20]] = header_file.readline().rstrip()

    header_keys.append("zero_point")
    header[header_keys[21]] = header_file.readline().rstrip()

    header_keys.append("comment1")
    header[header_keys[22]] = header_file.readline().rstrip()

    header_keys.append("comment2")
    header[header_keys[23]] = header_file.readline().rstrip()

    header_keys.append("comment3")
    header[header_keys[24]] = header_file.readline().rstrip()

    header_keys.append("comment4")
    header[header_keys[25]] = header_file.readline().rstrip()

    header_keys.append("comment5")
    header[header_keys[26]] = header_file.readline().rstrip()

    header_keys.append("comment6")
    header[header_keys[27]] = header_file.readline().rstrip()

    header_keys.append("comment7")
    header[header_keys[28]] = header_file.readline().rstrip()

    header_keys.append("blank_line1")
    header[header_keys[29]] = header_file.readline().rstrip()

    header_keys.append("blank_line2")
    header[header_keys[30]] = header_file.readline().rstrip()

    header_keys.append("blank_line3")
    header[header_keys[31]] = header_file.readline().rstrip()

    header_keys.append("fields")
    header[header_keys[32]] = header_file.readline().rstrip()

    header_keys.append("fields_units")
    header[header_keys[33]] = header_file.readline().rstrip()

    header_keys.append("end")
    header[header_keys[34]] = header_file.readline().rstrip()

    header_list = map(lambda x:{x,header[x]},header_keys)


def fill_header(csvfile, tess):
    for key in header_keys:
        value = header[key]
        if key == "instrument_id":
            value = value + " " + tess["name"]
        elif key == "data_supplier":
            value = value + " "+ tess["tester"]
        elif key == "location_name":
            value = value + " " + tess["city"]+" / "+ tess["country"] + " - " + tess["place"]
        elif key == "position":
            value = value + " " +str(tess["latitude"]) + " , " + str(tess["longitude"])
        elif key == "local_timezone":
            value = value + " " + tess["local_timezone"]
        elif key == "stationary_position":
            value = value + " " + tess["mov_sta_position"]
        elif key == "filters_per_channel":
            value = value + " " + tess["filters"]
        elif key == "mac":
            value = value + " " +tess["mac"]
        elif key == "zero_point":
            value = value + " " +tess["zero_point"]

        csvfile.write(value.encode('utf-8')+"\n")


def get_observations(tess, current, timezone):
    #Observations interval set up from 12:00 hours of the previous day to 12:00 of the present day,
    #both of them in local time

    #Convert local time to UTC time of the present day
    current_time_naive = datetime.datetime.strptime(str(current.date())+"T12:00:00Z","%Y-%m-%dT%H:%M:%SZ")
    current_time_local = timezone.localize(current_time_naive)
    current_time_utc = current_time_local.astimezone(pytz.utc)
    #print current_time_utc

    #Convert local time to UTC time of the previous day
    previous_time_utc = current_time_utc-datetime.timedelta(1)


    url = "http://api.stars4all.eu/photometers/"+tess["name"]+"/observations/latest_values?begin="+datetime.datetime.strftime(previous_time_utc,"%Y-%m-%dT%H:%M:%SZ")+"&end="+datetime.datetime.strftime(current_time_utc,"%Y-%m-%dT%H:%M:%SZ")+"&count=5000"
    #print url
    request = requests.get(url)
    return request.status_code, request.json()

def get_photometers():
    url_photometers = "http://api.stars4all.eu/photometers"
    request = requests.get(url_photometers)
    return request.status_code, request.json()

def gettimezone(latitude, longitude):
    tz = tzwhere.tzwhere()
    timezone_str = tz.tzNameAt(latitude, longitude)
    return pytz.timezone(timezone_str)

def getlocaltime(utc_time, tz):
    utc_aware = pytz.utc.localize(utc_time)
    return datetime.datetime.strftime(utc_aware.astimezone(tz),"%Y-%m-%dT%H:%M:%S")

if args.date:
    current_date = args.date
else:
    current_date = datetime.datetime.utcnow()

previous_date = current_date-datetime.timedelta(1)

print "Date set up to "+str(current_date)

print "Geting list of photometers ..."

status, list_photometers = get_photometers()

reference_time = datetime.datetime.strptime("12:00:00", "%X").time()

if status==200:
    for tess in list_photometers:
        print "Processing tess "+tess["name"]
        if "latitude" in tess and "longitude" in tess:
            #print str(tess["latitude"])+" "+str(tess["longitude"])
            try:
                timezone = gettimezone(tess["latitude"], tess["longitude"])
                local_time = getlocaltime(current_date, timezone)
                if datetime.datetime.strptime(local_time,"%Y-%m-%dT%H:%M:%S").time() > reference_time:
                    file_path = file_path + tess["name"]
                    if not os.path.isdir(file_path):
                        os.mkdir(file_path)

                    file_path = file_path + "/" + str(current_date.year)
                    if not os.path.isdir(file_path):
                        os.mkdir(file_path)

                    file_path = file_path + "/" + str(current_date.month)
                    if not os.path.isdir(file_path):
                        os.mkdir(file_path)

                    file = file_path + "/" + str(current_date.year) + "_" + str(current_date.month) + "_" + str(
                        current_date.day) + "_" + tess["name"] + ".dat"

                    csvfile = open(file, "w")
                    writer = csv.writer(csvfile, delimiter=';')
                    fill_header(csvfile, tess)
                    print "Requesting observations for photometer " + tess["name"]
                    status, observations = get_observations(tess, current_date, timezone)

                    for observation in observations:
                        local_time = getlocaltime(
                            datetime.datetime.strptime(observation["tstamp"], "%Y-%m-%dT%H:%M:%SZ"), timezone)
                        writer.writerow((observation["tstamp"][:-1], local_time, observation["tamb"],
                                         observation["tsky"], observation["freq"], observation["mag"]))
                else:
                    print "The update time has not been reached (12:00 in local time)"
            except AttributeError as att_error:
                print "Problem with the timezone of "+tess["name"]
                print att_error
            except TypeError as type_error:
                print "Problem with the format of (lat,long) in " + tess["name"]
                print type_error

print "End"