import sys

import Dbconnection as db
import threading
import time
from twilio.rest import Client
import googlemaps
from datetime import datetime

USERS_DATA = "users"
CRIME_DATA = "crimedata"
previous_records = 0
polling_threads = dict()


class pollingThread (threading.Thread):
    def __init__(self, threadID, name, table_name, threshold, stop_var):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.stop_var = stop_var
        self.table_name = table_name
        self._stop = threading.Event()
        self.self_thread = False
        self.threshold = threshold


    def is_not_safe(self, latlonglist, crimedata, dist):
        # Google Api key fpr Distance calculation to be provided here.
        gmaps = googlemaps.Client(key='')
        result = gmaps.distance_matrix(origins=latlonglist, destinations=crimedata, mode='walking')
        result_length = len(result['rows'][0]['elements'])
        for i in range(0, result_length):
            distance = result['rows'][0]['elements'][i]['distance']['value']
            if distance <= dist:
                return True
        return False


    def send_message(self, latlonglist, contact, type):
        message = None
        # Twilio API Account SID and Auth token to update here
        ACCOUNT_SID = "" 
        AUTH_TOKEN = ""
        client = Client(ACCOUNT_SID, AUTH_TOKEN)
        number_string = "+1" + str(contact)
        if type == "time":
            message = client.messages.create(
                body="Lost contact with your friend " + self.name + ". Last recorded location is http://maps.google.com/maps?z=20&q=" +
                     str(latlonglist[0]) + "," + str(latlonglist[1]), to=number_string, from_="+14806668794")
        else:
            message = client.messages.create(
                body="Regained contact with your friend " + self.name + ". Last recorded location is http://maps.google.com/maps?z=20&q=" +
                     str(latlonglist[0]) + "," + str(latlonglist[1]), to=number_string, from_="+14806668794")

        if message is None:
            print "error in sending message"
        else:
            print "message delivered"


    def run(self):
        time.sleep(3)
        while self.stop_var:
            crimedatalist = []
            latlong = db.get_latest_user_location(self.table_name)
            if len(latlong) > 0:
                latlonglist = [float(latlong[0][0]), float(latlong[0][1])]
                record_time = latlong[0][2]
                current_time = datetime.now()
                crimedata = db.get_all_records(CRIME_DATA)
                for data in crimedata:
                    crimedatalist.append(tuple([float(data[1]), float(data[2])]))
                latlongtuple = tuple(latlonglist)
                timediff = (current_time - record_time).total_seconds()
                if timediff > 30 and self.is_not_safe(latlongtuple, crimedatalist, self.threshold):
                    if self.self_thread:
                        continue
                    else:
                        contacts = db.get_emergency_contacts(self.threadID)
                        self.send_message(latlonglist, contacts, "time")
                        self.self_thread = True

                elif self.is_not_safe(latlongtuple, crimedatalist, self.threshold):
                    if self.self_thread:
                        contacts = db.get_emergency_contacts(self.threadID)
                        self.send_message(latlonglist, contacts, "time")
                        self.self_thread = False
                    else:
                        db.set_alert_flag(self.threadID)
                else:
                    if self.self_thread:
                        contacts = db.get_emergency_contacts(self.threadID)
                        self.send_message(latlonglist, contacts, "return")
                        self.self_thread = False
                    else:
                        time.sleep(3)
                        db.deset_alert_flag(self.threadID)
                        continue
            time.sleep(3)



    def stop(self):
        self.stop_var = False

    def stopped(self):
        return self._stop.isSet()


def main_thread():
    global previous_records, polling_threads
    db.initiate()
    while True:
        records_count = db.get_records_count(USERS_DATA)
        if records_count > previous_records:
            records = db.get_topic_n_records(USERS_DATA, records_count - previous_records)
            for record in records:
                db.create_users_navigation_table(str.lower(str(record[5])) + str(record[0]))
                polling_navigation_table(str(record[0]), str(record[1]), str(str.lower(record[5])) + str(record[0]), record[9])
                previous_records = records_count

        users_data  = db.get_all_records(USERS_DATA);
        for user in users_data:
            key = str.lower(str(user[5])) + str(user[0])
            if user[7] == "FALSE":
                if key in polling_threads:
                    polling_threads[key].stop()
                    polling_threads.pop(key, None)
            else:
                if key not in polling_threads:
                    thread_temp = pollingThread(str(user[0]), str.lower(str(user[5])), key, user[9], True)
                    thread_temp.start()
                    polling_threads[key] = thread_temp


def polling_navigation_table(userid, name, table_name, threshold):
    global polling_threads
    thread_temp = pollingThread(userid, name, table_name, threshold, True)
    thread_temp.start()
    polling_threads[table_name] = thread_temp


if __name__ == '__main__':
    try:
        main_thread()
    except (KeyboardInterrupt, SystemExit):
        for key, value in polling_threads.iteritems():
            value.stop()
        sys.exit()
