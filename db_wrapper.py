import pandas as pd
import psycopg2
from psycopg2 import sql
from datetime import datetime
from collections import OrderedDict

'''
    db_wrapper is a simple database wrapper class adapted from the fraud case 
    study essentially it is comprised of an __init__ function that creates the 
    psycopg2 connection to the database and get_ and set_ functions that 
    retrieve or insert/update data in the database, respectively. 
'''

class db_wrapper(object):
    def __init__(self, dbhost = 'database_host', 
                       dbname = 'database_name', 
                       dbuser = 'username',
                       dbpass = 'password'):
        ''' each instantiation of this class creates a database connection for '''
        self.conn = psycopg2.connect(host=dbhost, dbname=dbname, user=dbuser, password=dbpass)
        #self.web_columns = []

    def __exit__(self):
        self.conn.close

    def _check_object_id_in_db(self, object_id):
        ''' checks to see if object_id is in database '''
        cur = self.conn.cursor()
        #cur.execute( sql.SQL("SELECT count(*) FROM events WHERE object_id = {0}")
        #                    .format(sql.Literal(object_id)))
        cur.execute('''SELECT count(*) FROM events WHERE object_id = %(oid)s;''', {'oid': object_id})
        count = cur.fetchall()
        cur.close()
        return count[0][0] > 0

    def _do_insert(self, eventdata):
        '''
        inserts eventdata into events

        Args:
            eventdata: an OrderedDict of json webdata with db column names
        Returns:
            int of rows updated, should be 1 for success
        Raises:
            Exception: Raises a generic exception.
        '''
        cur = self.conn.cursor()
        insert_sql = sql.SQL('INSERT INTO events ({}) VALUES ('{}')').format(
                sql.SQL(', ').join(map(sql.Identifier, eventdata.keys()) ),
                sql.SQL('\', \'').join(sql.Placeholder() * len(eventdata)))
        cur.execute( insert_sql.as_string(self.conn) % tuple(eventdata.values()) )
        cur.close()
        self.conn.commit()
        return cur.rowcount

    def add_event(self, webdatajson):
        '''
        add new event to db from web stream

        Args:
            webdatajson: json blob
        Returns:
            int of rows updated, should be 1 for success
        Raises:
            Exception: Raises a generic exception.
        '''
        try:
            if len(webdatajson) != 43:
                raise Exception('web data not expected length')
            # check if object_id is in database, return 0 if it is.
            object_id = webdatajson['object_id']
            if self._check_object_id_in_db(object_id):
                print('  object id {0} already in db - not adding'.format(object_id))
                return 0
            date_columns = ['approx_payout_date', 'event_created', 'event_end', 'event_published', 'event_start', 'user_created']
            numeric_columns = ['event_published', 'venue_latitude', 'venue_longitude']
            values = [str(webdatajson[x])[:50] for x in webdatajson]
            values = map(lambda x: x.replace('\'',''), values)
            # create dictionary
            webdata = OrderedDict(zip(self.web_columns, values))
            # format dates as datetimes
            for date in date_columns:
                webdata[date] = datetime.fromtimestamp(int(float(webdata[date])))
            # fix numeric columns
            for num in numeric_columns:
                if webdata[num] is None:
                    webdata[num] = 0
            return self._do_insert(webdata)
        except Exception as e:
            print('error in add_event, going to rollback\n',e)
            self.conn.rollback()

    def get_new_events(self):
        '''
        returns all new events for which predictions have not been made

        Args:
            none
        Returns:
            pd.DataFrame of rows needing a prediction made, if there's an error
            it will return an empty dataframe
        Raises:
            Exception: Raises a generic exception.
        '''
        try:
            if self.conn.closed:
                print('error- db connection lost')
            cur = self.conn.cursor()
            cur.execute('''SELECT * FROM events WHERE prediction IS NULL;''')
            print('    cursor status: ', cur.statusmessage)
            records = cur.fetchall()
            cur.close()
            db_columns = ['eventid','prediction','pred_prob','disposition'] + self.web_columns
            df = pd.DataFrame(records, columns=db_columns)
            df['listed'] = df['listed'].apply(lambda x: 'y' if True else 'n' )
            return df
        except Exception as e:
            print('exception in new events :', e)
        return pd.DataFrame()   # if all else fails: return an empty dataframe 
    
    def set_prediction(self, eventid, prediction, pred_prob):
        '''
        update the prediction for an event

        Args:
            eventid: eventid of the event entry to update
            prediction: new prediction to insert
            pred_prob: probability of the prediction
        Returns:
            rowcount affected.
        Raises:
            Exception: Raises a generic exception.
        '''
        try:
            cur = self.conn.cursor()
            cur.execute( sql.SQL("UPDATE events SET prediction = \'{1}\', pred_prob = {2} \
                                WHERE eventid = {0}".format(str(eventid), prediction, pred_prob) ) )
            cur.close()
            self.conn.commit()
            return cur.rowcount
        except Exception as e:
            print('exception in set_prediction', e)
        
    def get_event_list(self, all=False):
        '''
        returns list of events for dashboard

        Args:
            all: boolean indicating whether to return all events or just those 
            needing a prediction made
        Returns:
            rowcount affected.
        Raises:
            Exception: Raises a generic exception.
        '''
        try:
            cur = self.conn.cursor()
            db_cols = ['eventid','prediction','pred_prob','disposition','name','org_name','sale_duration2', 'num_payouts']
            if all:
                sqlstr = 'SELECT {0} FROM events ORDER BY eventid DESC;'.format(', '.join(db_cols))
            else:
                sqlstr = 'SELECT {0} FROM events WHERE disposition IS NULL \
                            ORDER BY eventid DESC;'.format(', '.join(db_cols))
            cur.execute(sqlstr)
            records = cur.fetchall()
            cur.close()
            return pd.DataFrame(records, columns=db_cols)
        except Exception as e:
            print('exception in event_list ', e)
        return pd.DataFrame()   # if all else fails: return an empty dataframe 
    
    def set_disposition(self, eventid, disposition):
        '''
        update the disposition for an event

        Args:
            eventid: eventid of the item needing update
            disposition: new dispostion to insert
        Returns:
            rowcount affected.
        Raises:
            Exception: Raises a generic exception.
        '''
        try:
            cur = self.conn.cursor()
            cur.execute( sql.SQL("UPDATE events SET disposition = \'{1}\' WHERE eventid = {0}".format(str(eventid), disposition) ) )
            cur.close()
            self.conn.commit()
            return cur.rowcount
        except Exception as e:
            print('exception in set_disposition', e)

