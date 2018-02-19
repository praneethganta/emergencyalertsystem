import psycopg2 as pg


connection = None


def get_existing_connection():
    global connection
    if connection is None:
        connection = get_connection()
    else:
        return connection


def initiate():
    global connection
    connection = pg.connect(database="savior", user="postgres", password="postgres", host="127.0.0.1", port="5432")


def get_connection():
    global connection
    connection = pg.connect(database="savior", user="postgres", password="postgres", host="127.0.0.1", port="5432")
    return connection


def get_records_count(table_name):
    cursor = connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM %s" % table_name)
    count = cursor.fetchone()[0]
    cursor.close()
    return count


def get_all_records(table_name):
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM %s" % table_name)
    result_set = cursor.fetchall()
    return result_set


def get_topic_n_records(table_name, n):
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM %s ORDER BY createdate DESC LIMIT %s" % (table_name, n))
    records = cursor.fetchall()
    return records


def create_users_navigation_table(table_name):
    cursor = connection.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS %s (latitude numeric, longitude numeric, "
                   "entrytime timestamp default current_timestamp)"
                   % table_name)
    #cursor.execute("INSERT INTO %s (latitude, longitude) VALUES (33.429973, -111.937889)" % table_name)
    connection.commit()
    cursor.close()


def get_latest_user_location(table_name):
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM %s ORDER BY entrytime DESC LIMIT 1" % table_name)
    return cursor.fetchall()


def set_alert_flag(userid):
    cursor = connection.cursor()
    cursor.execute("UPDATE users SET alertflag = TRUE WHERE userid = %s" % userid)
    connection.commit()
    cursor.close()

def deset_alert_flag(userid):
    cursor = connection.cursor()
    cursor.execute("UPDATE users SET alertflag = FALSE WHERE userid = %s" % userid)
    connection.commit()
    cursor.close()


def get_emergency_contacts(userid):
    cursor = connection.cursor()
    cursor.execute("SELECT emergencycontactoneno FROM users WHERE userid = %s" %userid)
    contacts = cursor.fetchone()[0]
    return contacts


def get_user_status(userid):
    cursor = connection.cursor()
    cursor.execute("SELECT activeflag FROM users WHERE userid = %s" % userid)
    status = cursor.fetchone()[0]
    return status

def test_method():
    print "in test method"


