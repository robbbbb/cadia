from flask import Flask, request, render_template, redirect, request, url_for, abort, Response, jsonify, abort
from markupsafe import escape
from utils import clickhouse_conn, all_cols, normalize
from search import Search
from pagination import Pagination
from errors import Errors
from domain import Domain, History
from stats import Stats
from ns_domain import NSDomain
from reports import Reports
from tag import Tag

from utils import kafka_result_producer
from result import Result

import csv
from io import StringIO

app = Flask(__name__)
app.jinja_env.filters['zip'] = zip # add zip() function to use in templates

@app.route("/")
def index():
    return render_template('index.html')


@app.route("/s/<int:id>")
def search(id):
    search = Search(id)
    # TODO 404 for non-existant search id
    # pagination = Pagination.parse(request.args.get('p',None))
    # summary = search.summary()
    # pagination.rows = summary['count']
    # results = search.results(pagination)
    # , results=results, summary=summary, pagination=pagination)
    if search.progress() is None:
        search.run()

    if search.is_empty_result():
        abort(404)
    
    return render_template('search.html', search=search)

@app.route("/s/<int:id>/progress_log")
def progress(id):
    search = Search(id)
    progress = search.progress()
    return render_template('part/progress_bar.html', progress=progress, search=search)

@app.route("/s/<int:id>/summary")
def search_summary(id):
    search = Search(id)
    summary = search.summary()
    timestamp = search.timestamp()
    tag = Tag(search_id=id)
    return render_template('part/summary.html', search=search, summary=summary, timestamp=timestamp, tag=tag)


@app.route("/s/<int:id>/results")
def search_results(id):
    search = Search(id)
    pagination = Pagination.parse(request.args.get('p', None))
    pagination.rows = search.row_count()
    results = search.results(pagination)
    return render_template('part/results.html', search=search, results=results, pagination=pagination, path=request.path)


@app.route("/s/new", methods=['POST', 'GET'], defaults={'id': None})
@app.route("/s/<int:id>/filter", methods=['POST', 'GET'])
def filter(id):
    search = Search(id) if id else Search()
    if request.method == 'POST':
        col = request.form['col']
        match = request.form['match']
        value = request.form['value']
    else:
        # GET
        col = request.args['col']
        match = request.args['match']
        value = request.args['value']
    search.add(col, match, value)
    search.start_search()
    return redirect(url_for('search', id=search.id))

# TODO should be DELETE method on /filter/x if possible


@app.route("/s/<int:id>/remove-filter/<int:index>", methods=['POST'])
def remove_filter(id, index):
    search = Search(id)
    search.remove(index)
    if not search.criteria:
        return redirect(url_for('index'))
    search.start_search()
    return redirect(url_for('search', id=search.id))


@app.route("/s/<int:id>/refresh")
def refresh(id):
    search = Search(id)
    search.refresh()
    return redirect(url_for('search', id=id))


@app.route("/s/<int:id>/chart/<string:col>")
def chart(id, col):
    if not col in all_cols:
        abort(400)
    search = Search(id)
    labels, datapoints = search.bar_chart(col)
    return render_template('chart.html', search=search, labels=labels, datapoints=datapoints, col=col)

# Preset searches used as starting points from index page


@app.route("/preset/<string:name>")
def preset(name):
    search = Search()
    if name == 'abovens':
        search.add('ns_domains', 'exact', 'abovedomains.com')
    elif name == 'available':
        search.add('registered', 'exact', 'false')
    elif name == 'google':
        search.add('domain', 'startswith', 'google.')
    elif name == 'bodisns':
        search.add('ns_domains', 'exact', 'bodis.com')
    elif name == 'trellianmx':
        search.add('mx', 'exact', 'mx.trellian.com')
    else:
        abort(404)

    search.start_search()
    return redirect(url_for('search', id=search.id))


@app.route("/domain/<string:name>")
def domain(name):
    domain = Domain(name)
    history = History(name)
    if not domain.exists():
        abort(404)
    return render_template('domain.html', domain=domain.data, history=history.data)


@app.route("/errors")
def errors():
    return render_template('errors.html', errors=Errors().recent_errors())

@app.route("/errors/latest")
def errors_latest():
    return render_template('errors_latest.html', errors=Errors().latest())

@app.route("/error-details/<uuid:uuid>")
def error_details(uuid):
    return render_template('error_details.html', error=Errors().error_details(uuid))


@app.route("/stats/top_n/<string:col>", defaults={'count': 10})
@app.route("/stats/top_n/<string:col>/<int:count>")
def stats_top_n(col, count):
    if not col in all_cols:
        abort(400)
    return render_template('part/stat.html', col=col, results=Stats().top_n(col, count))


@app.route("/stats/total_domains")
def stats_total_domains():
    return format_number(Stats().total_domains())

# template for the statistics page
@app.route("/stats")
def statistics():
    return render_template('part/statistics.html')

@app.route("/s/<int:id>/export_csv")
def export_csv(id) -> Response:
    search = Search(id)
    results = search.results(Pagination(limit=5000000,order_by='domain'))
    
    # Create an in-memory CSV file
    output = StringIO()
    writer = csv.writer(output)
    
    # Write header (keys of the first row)
    if results:
        writer.writerow(results[0].keys())
        # Write data rows
        for row in results:
            writer.writerow(row.values())
    
    # Get the CSV data from StringIO
    output.seek(0)
    
    # Send the CSV data directly as a download
    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename=export_{id}.csv"}
    )


'''copy_domain
params: id
description: copy all domains from search results to clipboard
returns: list of domains
'''
@app.route("/s/<int:id>/copy_domain")
def copy_domain(id) -> Response:
    search = Search(id)
    results = search.results(Pagination(limit=5000000,order_by='domain',order='asc'))
    results = [row['domain'] for row in results]
    return jsonify(results)



@app.route("/stats/dns_scrapes_1d")
def dns_scrapes_1d():
    return format_number(Stats().dns_scrapes_1d())


@app.template_filter('format_number')
def format_number(number):
    if number is None:
        return None
    if number > 1_000_000_000:
        return "%.2fB" % (number / 1_000_000_000)
    if number > 1_000_000:
        return "%.2fM" % (number / 1_000_000)
    return format(number, ',d')


@app.template_filter('format_date')
def format_date(dateortime):
    if dateortime is None:
        return None
    return dateortime.strftime('%Y-%m-%d')


@app.route("/report/ns_domain")
def ns_domain():
    return render_template('ns_domain_results.html', results=NSDomain.ns_domain_report())

@app.route("/report/scraped")
def scraped_report():
    return render_template('scrape_report.html', results=Reports.scraped())


@app.route("/s/<int:id>/edit", methods=['GET', 'POST'])
def edit_search(id):
    search = Search(id)
    if request.method == 'POST':
        col = request.form['col']
        match = request.form['match']
        value = request.form['value']
        index = int(request.form.get('criteria-index', 0))
        search.update(index, col, match, value)
        search.start_search()
        return redirect(url_for('search', id=search.id)) 
    return render_template('edit_search.html', search=search)

@app.route("/tag/<int:id>")
def tag(id):
    return render_template('tag.html', tag=Tag(id))


@app.route("/import_domain", methods=["GET", "POST"])
def import_domain():
    result_producer = kafka_result_producer()
    domain = ""
    content = ""
    normalize_domain = ""
    valid_domain = 0
    invalid_domain = 0

    # get the iterate
    # check if the domain is valid
    # get the normalize function 
    # put in the producer
    # add count the inserted and invalid
    # call the producer flush()
    # return the result

    if request.method == "POST":
        if "csvFile" not in request.files:
            return "No file uploaded", 400

        file = request.files["csvFile"]
        
        if file.filename == "":
            return "No selected file", 400

        if file:
            content = file.read().decode("utf-8")
            for domain in content.splitlines():
                domain = domain.strip()
                normalize_domain = normalize(domain)
                if normalize_domain:
                    result_producer.send("results", key=domain, value=Result(domain=normalize_domain,modified_by='csv_import') )
                    valid_domain += 1
                else:
                    invalid_domain += 1
                    continue
            result_producer.flush()
    return render_template("import_domain.html", valid_domain=valid_domain, 
                           invalid_domain=invalid_domain, content=content)
    
