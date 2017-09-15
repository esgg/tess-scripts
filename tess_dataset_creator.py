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

file_path = os.getenv('BASE_STORAGE_PATH', "")

with open("header.dat") as header_file:
    header["definition"] = header_file.readline().rstrip()
    header["definition_url"] = header_file.readline().rstrip()
    header["number_of_lines"] = header_file.readline().rstrip()
    header["data_license"] = header_file.readline().rstrip()
    header["device_type"] = header_file.readline().rstrip()
    header["instrument_id"] = header_file.readline().rstrip()
    header["definition"] = header_file.readline().rstrip()
    header["data_supplier"] = header_file.readline().rstrip()
    header["location_name"] = header_file.readline().rstrip()
    header["position"] = header_file.readline().rstrip()
    header["local_timezone"] = header_file.readline().rstrip()
    header["time_synchronization"] = header_file.readline().rstrip()
    header["stationary_position"] = header_file.readline().rstrip()
    header["fixed_direction"] = header_file.readline().rstrip()
    header["number_of_channels"] = header_file.readline().rstrip()
    header["filters_per_channel"] = header_file.readline().rstrip()
    header["measurement_direction"] = header_file.readline().rstrip()
    header["field_of_view"] = header_file.readline().rstrip()
    header["Number_of_fields_per_line"] = header_file.readline().rstrip()
    header["mac"] = header_file.readline().rstrip()
    header["firmware_version"] = header_file.readline().rstrip()
    header["cover_offset_value"] = header_file.readline().rstrip()
    header["zero_point"] = header_file.readline().rstrip()
    header["comment1"] = header_file.readline().rstrip()
    header["comment2"] = header_file.readline().rstrip()
    header["comment3"] = header_file.readline().rstrip()
    header["comment4"] = header_file.readline().rstrip()
    header["comment5"] = header_file.readline().rstrip()
    header["comment6"] = header_file.readline().rstrip()
    header["comment7"] = header_file.readline().rstrip()
    header["blank_line1"] = header_file.readline().rstrip()
    header["blank_line2"] = header_file.readline().rstrip()
    header["blank_line3"] = header_file.readline().rstrip()
    header["fields"] = header_file.readline().rstrip()
    header["fields_units"] = header_file.readline().rstrip()
    header["end"] = header_file.readline().rstrip()


def fill_header(csvfile, tess):
    for key, value in header.iteritems():
        if key == "instrument_id":
            value = value + tess["name"]
        elif key == "owner":
            value = value + tess["owner"]
        elif key == "location":
            value = value + tess["location"]
        elif key == "latitude":
            value = value + tess["longitude"]
        elif key == "latitude":
            value = value + tess["longitude"]

        csvfile.write(value+"\n")


def get_observations(tess, current, timezone):
    #Observations interval set up from 12:00 hours of the previous day to 12:00 of the present day,
    #both of them in local time

    #Convert local time to UTC time of the present day
    current_time_naive = datetime.datetime.strptime(str(current.date())+"T12:00:00Z","%Y-%m-%dT%H:%M:%SZ")
    current_time_local = timezone.localize(current_time_naive)
    current_time_utc = current_time_local.astimezone(pytz.utc)
    print current_time_utc

    #Convert local time to UTC time of the previous day
    previous_time_utc = current_time_utc-datetime.timedelta(1)


    url = "http://api.stars4all.eu/photometers/"+tess["name"]+"/observations/latest_values?begin="+datetime.datetime.strftime(previous_time_utc,"%Y-%m-%dT%H:%M:%SZ")+"&end="+datetime.datetime.strftime(current_time_utc,"%Y-%m-%dT%H:%M:%SZ")+"&count=5000"
    print url
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
        if "latitude" in tess and "longitude" in tess:
            print str(tess["latitude"])+" "+str(tess["longitude"])
            try:
                timezone = gettimezone(tess["latitude"], tess["longitude"])
                local_time = getlocaltime(current_date, timezone)
                if local_time.time() > reference_time:
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
                    print "Creating file " + file

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

            except AttributeError:
                print "Problem with the timezone of "+tess["name"]
            break

print "End"