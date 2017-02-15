#!/usr/bin/python3
# -*- coding: utf-8 -*-

import abc
import argparse
import configparser
import csv
import os
import paramiko
import sys
from datetime import datetime
from influxdb import InfluxDBClient
from requests.exceptions import ConnectionError


def timestamp_ms():
    return int((datetime.utcnow() - datetime(1970, 1, 1)).total_seconds() * 1000)


class ConfigFile(object):
    """ Extract useful information from the configuration file """

    def __init__(self, file: str):
        if not os.path.isfile(file):
            print('ERROR: The configuration file must be a file (captain obvious)')
            sys.exit(1)

        try:
            self._conf = configparser.ConfigParser()
            self._conf.read(file)

        except configparser.ParsingError:
            print('ERROR: The format of the configuration file is incorrect')
            sys.exit(1)

    def get_influxdb(self):
        """ Return information about the database instance """

        database = \
            {
             'address': self._conf['INFLUXDB']['address'],
             'username': self._conf['INFLUXDB']['username'] if self._conf['INFLUXDB']['username'] else None,
             'password': self._conf['INFLUXDB']['password'] if self._conf['INFLUXDB']['password'] else None,
             'database': self._conf['INFLUXDB']['database'] if self._conf['INFLUXDB']['database'] else 'svc2influxdb'
            }
        return database

    def get_svc(self):
        """ Return information about the IBM SVC equipments defined in the configuration file """
        equipment = {}
        for section in self._conf.sections():
            if section == 'INFLUXDB':
                continue

            equipment['tags'] = {'svc': section}
            equipment['address'] = self._conf[section]['address']
            equipment['login'] = self._conf[section]['login']
            equipment['password'] = self._conf[section]['password']

            for item in self._conf[section]:
                if item not in ['name', 'address', 'login', 'password']:
                    equipment['tags'][item] = self._conf[section][item]

            yield equipment


class SeriesBuilder(object):
    """ Abstract class used to build the time series for InfluxDB """

    __metaclass__ = abc.ABCMeta

    def __init__(self, fixed_time=None):
        self._command = None
        self._extras_tags = {}
        self._fixed_time = fixed_time
        self._measurements = []
        self._tags = []

    def _build_series(self, measurement: str, tags: dict, value: str, prefix: str):
        new_series = {'measurement': '%s_%s' % (prefix, measurement),
                      'tags': {},
                      'fields': {'value': int(value)}}

        for key, value in tags.items():
            new_series['tags'][key] = value

        if self._fixed_time:
            new_series['time'] = int(self._fixed_time)

        return new_series

    def add_extras_tags(self, tags: dict):
        self._extras_tags = tags

    def parse(self, data: dict, prefix):
        merged_tags = self._extras_tags.copy()
        series = []

        for measurement in self._measurements:
            if measurement in data:
                merged_tags.update({tag: data[tag] for tag in self._tags})
                series.append(self._build_series(measurement=measurement,
                                                 tags=merged_tags,
                                                 value=data[measurement],
                                                 prefix=prefix))

        return series


class PoolSeriesBuilder(SeriesBuilder):
    """ Concrete class used to defined which measurements needs to be collected in the SVC's pool """

    def __init__(self, fixed_time=None):
        super(PoolSeriesBuilder, self).__init__(fixed_time)
        self._measurements = ['capacity',
                              'virtual_capacity',
                              'compression_compressed_capacity',
                              'compression_uncompressed_capacity',
                              'overallocation',
                              'vdisk_count',
                              'compression_virtual_capacity',
                              'free_capacity',
                              'real_capacity',
                              'used_capacity']

        self._tags = ['name', 'id']


class VolumeSeriesBuilder(SeriesBuilder):
    """ Concrete class used to defined which measurements needs to be collected in the SVC's volumes """

    def __init__(self, fixed_time=None):
        super(VolumeSeriesBuilder, self).__init__(fixed_time)
        self._measurements = ['capacity',
                              'virtual_capacity',
                              'used_capacity',
                              'real_capacity',
                              'free_capacity',
                              'uncompressed_used_capacity']

        self._tags = ['name', 'id', 'vdisk_UID']


class SSHCollector(object):
    """ Abstract class used to collect information over a SSH connection """

    __metaclass__ = abc.ABCMeta

    def __init__(self, **kwargs):
        self._builder = None
        self._address = kwargs['address']
        self._user = kwargs['login']
        self._password = kwargs['password']

        self._client = paramiko.SSHClient()
        self._client.load_system_host_keys()
        self._client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        try:
            self._client.connect(hostname=self._address, username=self._user, password=self._password)
        except paramiko.ssh_exception.AuthenticationException:
            print('ERROR: Authentication Error on SVC %s' % self._address)
            sys.exit(1)
        except TimeoutError:
            print('ERROR: Timeout connection on SVC %s' % self._address)
            sys.exit(1)

    def _send_command(self, command: str):
        stdin, stdout, stderr = self._client.exec_command(command)
        return stdout

    def add_series_builder(self, builder):
        self._builder = builder

    def collect(self):
        raise NotImplementedError


class PoolSSHCollector(SSHCollector):
    """ Concrete class specialized in the data collection of the SVC's pool """

    def __init__(self, **kwargs):
        super(PoolSSHCollector, self).__init__(**kwargs)

    def collect(self):
        stdout = self._send_command('lsmdiskgrp -bytes -delim ,')
        reader = csv.DictReader(stdout)
        return [self._builder.parse(line, 'pool') for line in reader]


class VolumeSSHCollector(SSHCollector):
    """ Concrete class specialized in the data collection of the SVC's volume """

    def __init__(self, **kwargs):
        super(VolumeSSHCollector, self).__init__(**kwargs)

    def _get_volume_details(self, identifier: str):
        stdout = self._send_command('lsvdisk -bytes -delim , %s' % identifier)
        reader = csv.reader(stdout)
        return {line[0]: line[1] for line in reader if line}

    def collect(self):
        stdout = self._send_command('lsvdisk -bytes -delim ,')
        reader = csv.DictReader(stdout)

        volume_details = []
        for line in reader:
            volume_details.append(self._get_volume_details(line['id']))

        return [self._builder.parse(line, 'volume') for line in volume_details]


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='SVC metrics collector for InfluxDB')
    parser.add_argument('config', type=str, help='The configuration file')
    parser.add_argument('-f', '--fixed', action="store_true", default=False, help='Use a same collect time for all SVC')
    args = parser.parse_args()

    configuration = ConfigFile(args.config).get_influxdb()
    client = InfluxDBClient(host=configuration['address'],
                            username=configuration['username'],
                            password=configuration['password'])

    # if the argument 'fixed' is used we use a same timestamp when all the measurements will be insert
    now = timestamp_ms()
    pool_series_builder = PoolSeriesBuilder(fixed_time=now) if args.fixed else PoolSeriesBuilder()
    volume_series_builder = VolumeSeriesBuilder(fixed_time=now) if args.fixed else VolumeSeriesBuilder()

    series = []
    for svc in ConfigFile(args.config).get_svc():
        pool_series_builder.add_extras_tags(svc['tags'])
        volume_series_builder.add_extras_tags(svc['tags'])

        svc_pool = PoolSSHCollector(**svc)
        svc_volume = VolumeSSHCollector(**svc)

        svc_pool.add_series_builder(pool_series_builder)
        svc_volume.add_series_builder(volume_series_builder)

        series += svc_pool.collect()
        del svc_pool
        series += svc_volume.collect()
        del svc_volume

    try:
        client.create_database(configuration['database'])
    except ConnectionError:
        print('ERROR: Cannot access to the InfluxDB database')
        sys.exit(1)

    # All the series are inserted into the database at the end of the batch to be sure we have a consistent batch
    # with all the measurements en equipments.
    for serie in series:
        client.write_points(database=configuration['database'], points=serie, time_precision='ms')
