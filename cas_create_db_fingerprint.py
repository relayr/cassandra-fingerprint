#!/usr/bin/env python3
""" Fingerprint generator"""

import argparse
import json
import logging

import cassandra
import cassandra.cluster
from cassandra.query import dict_factory


def escape_row(value):
    try:
        json.dumps(value)
        return value
    except TypeError:
        return str(value)


def get_table_data_from_template(session, table, table_template):
    """Return sample rows present in table_template"""
    logging.debug("Loading data for queries from template")
    row_list = table_template.get("data")
    if row_list is None:
        return None
    current_data = []
    if row_list:
        for row_data in row_list:
            query = row_data["query"]
            result = session.execute(query)
            current_data.append({
                "query": query,
                "data": [escape_row(r) for r in result]
            })
    else:
        current_data = get_table_data_from_db(session, table, 1)
    logging.info(current_data)
    return current_data


def build_response_for_row(session, table, row):
    """Store query and result into a dictionary"""
    table_name = table.keyspace_name + "." + table.name
    pk = [p.name for p in table.primary_key]
    predicate = " and ".join(["%s = %%(%s)s" % (n, n) for n in pk])
    query = "SELECT * FROM {} WHERE {}".format(
        table_name,
        predicate
        )
    logging.debug(query)
    params = {k: row[k] for k in pk}
    logging.debug(params)
    future = session.execute_async(query, params)
    bounded_query = future.message.query
    logging.debug(bounded_query)
    rows2 = future.result()
    return {
        "query": bounded_query,
        "data": [escape_row(r) for r in rows2]
        }


def get_table_data_from_db(session, table, sample_size):
    """Extract sample_size rows from database"""
    logging.debug("Loading sample rows from database")
    table_name = table.keyspace_name + "." + table.name
    try:
        rows = session.execute("SELECT * FROM " + table_name + " LIMIT %s",
                               [sample_size])
    except cassandra.ReadFailure:
        logging.warning("Cannot acquire data from %s. Too many tombstones?",
                        table_name)
        return None
    data = []
    for row in rows:
        data.append(build_response_for_row(session, table, row))
    return data


def get_table_fingerprint(session, table, sample_size, table_template,
                          transient_tables):
    """ Extract table definition and some data into dictionary"""
    logging.info("Generating fingerprint of table %s", table.name)
    fingerprint = {}
    fingerprint["schema"] = table.export_as_string()
    if table_template:
        data = get_table_data_from_template(session, table, table_template)
    elif table.keyspace_name + "." + table.name in transient_tables:
        logging.info("Skipping generation of data fingerprint")
        data = None
    else:
        data = get_table_data_from_db(session, table, sample_size)
    if data is not None:
        fingerprint["data"] = data
    return fingerprint


def generate_fingerprint_for_kp(session, keyspace, sample_size, ksp_template,
                                transient_tables):
    """ Prepare fingerprint of a keyspace"""
    logging.info("Generating fingerprint of keyspace %s", keyspace.name)
    fingerprint = {}
    for table in keyspace.tables.values():
        table_template = ksp_template.get(table.name)
        fingerprint[table.name] = get_table_fingerprint(session,
                                                        table,
                                                        sample_size,
                                                        table_template,
                                                        transient_tables
                                                        )
    return fingerprint


def execute_custom_queries(session, custom_queries):
    """prepare a list of results of custom queries"""
    query_results = []
    for query in custom_queries:
        logging.debug(query)
        result_set = session.execute(query)
        query_results.append({
                             "query": query,
                             "data": [escape_row(r) for r in result_set]
                             })
    return query_results


def generate_fingerprint(host, sample_size, template_report, transient_tables,
                         custom_queries):
    """generate fingerprint for database"""
    fingerprint = {}
    cluster = cassandra.cluster.Cluster(host)
    session = cluster.connect()
    session.row_factory = dict_factory
    user_keyspaces = (k for k in cluster.metadata.keyspaces.values()
                      if not k.name.startswith('system'))
    template_keyspaces = template_report.get("keyspaces", {})
    for space in user_keyspaces:
        ksp_template = template_keyspaces.get(space.name, {})
        fingerprint[space.name] = generate_fingerprint_for_kp(session, space,
            sample_size, ksp_template, transient_tables)
    fingerprint = {"keyspaces": fingerprint}
    CUSTOM_QUERIES = "custom_queries"
    if custom_queries:
        fingerprint[CUSTOM_QUERIES] = execute_custom_queries(session,
                                                             custom_queries)
    if template_report and CUSTOM_QUERIES in template_report:
        queries = [q["query"] for q in template_report[CUSTOM_QUERIES]]
        fingerprint[CUSTOM_QUERIES] = execute_custom_queries(session, queries)
    return fingerprint


def run():
    parser = argparse.ArgumentParser(
        description='Prepare database fingerprint')
    parser.add_argument('-d', dest='loglevel', action='store_const',
                        const=logging.DEBUG, default=logging.INFO,
                        help='enable debug')
    sp = parser.add_subparsers(title="subcommands", dest="command")
    sp.required = True
    parser_t = sp.add_parser('template',
                             help='create a fingerprint from template')
    parser_t.add_argument('fingerprint', type=str,
                          help='use old fingerprint as template for a new one'
                          )
    parser_c = sp.add_parser('fingerprint', help='create a fingerprint')
    parser_c.add_argument('-s', '--sample-size', type=int, default=10,
                          help="number of sampled rows of a table")
    parser_c.add_argument('--query', dest='custom_queries', action="append",
                          metavar='QUERY',
                          help='''execute custom query and append it to a fingerprint.
                          Order of result rows should not change between
                          executions. Could be set multiply times.
                          The queries are executed in order they are specified.
                          ''')
    parser_c.add_argument('-t',  '--transient-table', dest='transient_tables',
                          action='append', default=[],
                          help='Skip data collection for a table. Could be' +
                          ' set multiply times. Format: <keyspace>.<table>'
                          )
    parser.add_argument('host', type=str, help='address of cassandra cluster',
                        default='127.0.0.1', nargs='?')
    args = parser.parse_args()
    logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s',
                        level=args.loglevel)
    if args.command == 'template':
        with open(args.fingerprint, 'r') as fp:
            template_report = json.load(fp)
            fingerprint = generate_fingerprint([args.host], None,
                                               template_report,
                                               [],
                                               None)
    else:
        fingerprint = generate_fingerprint([args.host], args.sample_size,
                                           {},
                                           args.transient_tables,
                                           args.custom_queries
                                           )
    print(json.dumps(fingerprint))


if __name__ == '__main__':
    run()
