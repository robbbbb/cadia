import pickle
from pagination import Pagination
from random import randint
from threading import Thread
from time import sleep
from utils import clickhouse_conn, postgres_conn, array_cols, bool_cols, all_cols, memcached_connect
from base64 import b64encode, b64decode

from datetime import datetime

search_threads = {}

class Search:
    def __init__(self, id=None):
        self.criteria = []
        self._is_empty = False
        self.id = id
        if id:
            self.load_from_db()
        self.from_id = None
    
    def add(self, col, match, value):
        if col not in all_cols:
            raise ValueError
        if match not in [ 'exact', 'startswith', 'substring', 'not' ]:
            raise ValueError
        self.criteria.append((col,match,value))
        # we're a different search now. new id will be assigned upon save
        self.from_id = self.id
        self.id = None

    def remove(self, index):
        self.criteria.pop(index)
        self.id = None # we're a different search now. new id will be assigned upon save

    def update(self, index, col, match, value):
        self.criteria[index] = (col, match, value) # update the selected index in search criteria
        self.id = None # we're a different search now. new id will be assigned upon save

    def escape(self, value):
        return value # TODO escape it

    def progress(self):
        db = postgres_conn()
        cur = db.cursor()
        cur.execute("select progress from searches where id=%s", (self.id,))
        progress = cur.fetchone()
        return progress[0]

    # Generate SQL for this search
    # Returns sql and dict of params
    # Assumes col names are already validated, done in the add method above
    def sql(self):
        sql = []
        params = {}
        for id,c in enumerate(self.criteria):
            col, match, value = c
            #print("col: %s match: %s value: %s" % (col,match,value))

            # make parameter name and placeholder - eg param_1 and %(param_1)s
            param_name = "param_%d" % id
            param_placeholder = "%%(%s)s" % param_name

            if col in array_cols:
                if match == 'exact':
                    sql.append("has(%s,%s)" % (col, param_placeholder ))
                    params[param_name] = self.escape(value)
                elif match == 'not':
                    sql.append("not has(%s,%s)" % (col, param_placeholder ))
                    params[param_name] = self.escape(value)
                elif match == 'startswith':
                    sql.append("arrayExists( n -> n like %s, %s)" % (param_placeholder, col))
                    params[param_name] = self.escape(value + '%') # TODO different escape needed for like?
                elif match == 'substring':
                    sql.append("arrayExists( n -> n like %s, %s)" % (param_placeholder, col))
                    params[param_name] = self.escape('%' + value + '%') # TODO different escape needed for like?
                else:
                    raise ValueError
            elif col in bool_cols:
                # bool col, only one way to match that
                if value not in ( 'true', 'false' ):
                    raise ValueError
                sql.append("%s = %s" % (col, value))
            else:
                # string col
                if match == 'exact':
                    sql.append("%s=%s" % (col, param_placeholder))
                    params[param_name] = self.escape(value)
                elif match == 'not':
                    sql.append("%s!=%s" % (col, param_placeholder))
                    params[param_name] = self.escape(value)
                elif match == 'startswith':
                    sql.append("%s like %s" % (col, param_placeholder))
                    params[param_name] = self.escape(value + '%') # TODO different escape needed for like?
                elif match == 'substring':
                    sql.append("%s like %s" % (col, param_placeholder))
                    params[param_name] = self.escape('%' + value + '%') # TODO different escape needed for like?
                else:
                    raise ValueError

        return ' AND '.join(sql), params

    def wait(self):
        # Clean up threads
        if self.id not in search_threads:
            return
        search_threads[self.id].join()
        if self.id in search_threads:
            # TODO potential race condition? multiple threads may call join and hit this at the same time
            del search_threads[self.id]
        while self.progress is None or self.progress() < 100:
            # TODO need some way to report errors that happened in that background thread
            # TODO add a time limit
            print("Waiting for search...")
            sleep(0.1)

    # Search and return results
    # TODO option for which cols
    def results(self, pagination=Pagination()):
        if self.id is None:
            self.start_search()
        self.wait()
        params = {}
        params['limit'] = pagination.limit
        params['offset'] = pagination.offset
        params['order_by'] = pagination.order_by
        params['order'] = pagination.order
        params['search_results_id'] = self.search_results_id
        ch = clickhouse_conn()
        cols = ','.join(all_cols)
        sql = "select %s from search_results final where search_id=%%(search_results_id)d order by %s %s limit %%(limit)d offset %%(offset)d" % (cols, params['order_by'], params['order'])
        rows = ch.execute(sql, params)
        rows = [ dict(zip(all_cols, row)) for row in rows ]

        # Add tags here
        db = postgres_conn()
        cur = db.cursor()
        cur.execute("select search_id, tags.id, name, colour, icon from tags join searches on (tags.search_id=searches.id)")
        # Iterate for each tag, over each row in the results
        for tag_row in cur:
            s = Search(tag_row[0])
            for result_row in rows:
                if s.match(result_row):
                    if 'tags' not in result_row:
                        result_row['tags'] = []
                    result_row['tags'].append({ 'id' : tag_row[1], 'name' : tag_row[2], 'colour' : tag_row[3], 'icon' : tag_row[4] })

        #print(rows)
        return rows

    def match(self, row):
        # does row match this search?
        # Currently we only support AND, so this works by returning false for any failed match, and defaulting to true
        for id,c in enumerate(self.criteria):
            col, match, value = c
            #print("col: %s match: %s value: %s" % (col,match,value))

            if col in array_cols:
                if match == 'exact':
                    if value not in row[col]:
                        return False
                elif match == 'not':
                    if value in row[col]:
                        return False
                elif match == 'startswith':
                    if not any( s.startswith(value) for s in row[col] ):
                        return False
                elif match == 'substring':
                    if not any( value in s for s in row[col] ):
                        return False
                else:
                    raise ValueError
            elif col in bool_cols:
                # bool col, only one way to match that
                if value not in ( 'true', 'false' ):
                    raise ValueError
                if ( value == 'false' and row[col] ) or ( value == 'true' and not row[col] ):
                    return False
            else:
                # string col
                if match == 'exact':
                    if row[col] != value:
                        return False
                elif match == 'not':
                    if row[col] == value:
                        return False
                elif match == 'startswith':
                    if not row[col].startswith(value):
                        return False
                elif match == 'substring':
                    if not value in row[col]:
                        return False
                else:
                    raise ValueError
        return True

    def summary(self):
        self.wait()
        result = {}
        ch = clickhouse_conn()
        params = { 'search_results_id' : self.search_results_id }
        rows = ch.execute("select count(*) from search_results final where search_id=%(search_results_id)d", params)
        result['count'] = rows[0][0]
        rows = ch.execute("select registered,count(*) from search_results final where search_id=%(search_results_id)d group by registered order by registered", params)
        result['registered'] = rows
        arr_sum_sql = ','.join("sum(visits_arr_12m[%d])" % (i+1) for i in range(12))
        rows = ch.execute("select [ %s ] from search_results final where search_id=%%(search_results_id)d" % arr_sum_sql, params)
        result['visits_arr_12m'] = rows[0][0]
        result['visits_12m'] = sum(result['visits_arr_12m'])

        return result
    
    def row_count(self):
        self.wait()
        ch = clickhouse_conn()
        params = { 'search_results_id' : self.search_results_id }
        rows = ch.execute("select count(*) from search_results final where search_id=%(search_results_id)d", params)
        return rows[0][0]

    def cache_results(self,refresh=False):
        ch = clickhouse_conn()
        where, params = self.sql()
        cols = ','.join(all_cols)
    
        db = postgres_conn()
        cur = db.cursor()

        # TODO get current value of search_results_id, save it to clean up later
        cur.execute("UPDATE searches SET timestamp = %(now)s, progress=0, search_results_id=nextval('search_results_id_seq') WHERE id = %(id)s RETURNING search_results_id",{'id': self.id, 'now': datetime.now()})
        search_results_id = cur.fetchone()[0]
        db.commit()
        print("search_results_id is ", search_results_id)
        self.search_results_id = search_results_id

        if self.from_id:
            progress = ch.execute_with_progress("insert into search_results(search_id, %s) select %d as search_id, %s from (select * from search_results final where search_id=%s and %s)" % (cols, search_results_id, cols, self.from_id, where), params)
        else:
            progress = ch.execute_with_progress("insert into search_results(search_id, %s) select %d as search_id, %s from domains final where %s" % (cols, search_results_id, cols, where), params, settings={'do_not_merge_across_partitions_select_final':1})

        # Loop to track progress
        for item in progress:
            if isinstance(item, tuple) and len(item) == 2:
                num_rows, total_rows = item
                if total_rows:
                    percent = round((num_rows / total_rows) * 100, 2)
                    cur.execute(
                        "UPDATE searches SET progress = %(progress)s WHERE id = %(id)s",
                        {'progress': percent, 'id': self.id}
                    )
                    db.commit()

        # TODO see above about capturing old value of search_results_id, clean up here if set
        #if refresh:
            # Purge old results first
            #ch.execute("alter table search_results delete where search_id=%(id)d", { 'id' : self.search_results_id })
    
                   
    def refresh(self):
        self.run(refresh=True)

    def start_search(self): # TODO rename to save_and_run or something?
        crit = pickle.dumps(self.criteria)
        # TODO enable the code below to find if the same search already exists and re-use it
        #      - could store a hash and index it for a more efficient lookup
        #ch = clickhouse_conn()
        # TODO DOES NOT WORK, clickhouse driver does not understand binary string in params
        #crit = str(b64encode(pickle.dumps(self.criteria)))
        #print(crit)
        #rows = ch.execute("select id from searches where criteria=%(crit)s", { 'crit' : crit })
        #if rows:
        #    # TODO check date the results were generated. refresh if too old
        #    self.id = rows[0][0]

        db = postgres_conn()
        cur = db.cursor()
        cur.execute("insert into searches (criteria,search_results_id) values (%s,nextval('search_results_id_seq')) returning id", (crit,))
        db.commit()
        self.id = cur.fetchone()[0]
        self.run()

    def run(self,refresh=False):
        # Run the search in a background thread
        print("Running search...")
        thread = Thread(target=self.cache_results,args=(refresh,))
        thread.daemon = True
        thread.start()
        search_threads[self.id] = thread
    
    def is_empty_result(self):
        return self._is_empty

    def load_from_db(self):
        db = postgres_conn()
        cur = db.cursor()
        cur.execute("SELECT criteria,search_results_id FROM searches WHERE id=%s", (self.id,))
        result = cur.fetchone()
        if result and result[0]:
            self.criteria = pickle.loads(result[0])
            self.search_results_id = result[1]
        else:
            self._is_empty = True
        
    def describe(self):
        # Plain text description of this search to show in UI
        return ','.join( [ ' '.join(c) for c in self.criteria ] )

    def bar_chart(self, col):
        ch = clickhouse_conn()
        params = { 'id' : self.id }
        # TODO make this more generic
        if col == 'ns':
            # if ns_domain in criteria, add like match for it to filter out irrelevant ns
            print(self.criteria)
            ns_filter = [ c[2] for c in self.criteria if c[0] == 'ns_domains' ] # TODO add method to get filter
            if ns_filter:
                where = f"where ns like '%%.{ns_filter[0]}'" #TODO sqli
            else:
                where = ""
            results = ch.execute("""select ns,count(*) from (select arrayJoin(ns) as ns from search_results where search_id=%%(id)d and ns is not null) %s group by 1 order by 2 desc limit 100""" % where, params)
        elif col in array_cols:
            results = ch.execute(f"""select {col},count(*) from (select arrayJoin({col}) as {col} from search_results where search_id=%(id)d and {col} is not null) group by 1 order by 2 desc limit 100""", params)
        elif col in all_cols:
            results = ch.execute(f"""select {col},count(*) from search_results where search_id=%(id)d and {col} is not null group by 1 order by 2 desc limit 100""", params)
        else:
            raise ValueError("Don't know how to make chart for that column")
        labels = [ "<empty>" if row[0] == '' else row[0] for row in results ]
        datapoints = [ row[1] for row in results ]
        return labels, datapoints

    def timestamp(self):
        ch = clickhouse_conn()
        db = postgres_conn()
        cur = db.cursor()
        cur.execute("select timestamp from searches where id=%s", (self.id,))
        
        # calculate time difference
        time_difference = datetime.now() - cur.fetchone()[0]
        # Extract days, hours, minutes, and seconds
        days = time_difference.days

        # Used divmod for clean division and remainder calculation.
        hours, remainder = divmod(time_difference.seconds, 3600)
        minutes, seconds = divmod(remainder, 60)

        if days:
            result = f"{days} days ago"
        elif hours:
            result = f"{hours} hours ago"
        elif minutes:
            result = f"{minutes} minutes ago"
        else:
            result = f"{seconds} seconds ago"
        
        return result
