#!/usr/local/bin/python3

import configparser
import subprocess
import shlex
import os
from datetime import datetime
import time
import logging
import sys


class MysqlDumper:
    """

    Class for using mysqldump utility.

    """

    def __init__(self, conf='/home/mysql-backup/slave_dump_backup_script/bck.conf'):

        con = configparser.ConfigParser()
        con.read(conf)
        bolme = con.sections()

        DB = bolme[0]
        self.mysql = con[DB]['mysql']
        self.mycnf = con[DB]['mycnf']
        self.myuseroption = con[DB]['useroption']


        BCK = bolme[1]
        self.backupdir = con[BCK]['backupdir']
        self.full_dir = self.backupdir + '/dumps'
        self.backup_tool = con[BCK]['backup_tool']

        if not (os.path.exists(self.full_dir)):
            print('Full directory is not exist. Creating full backup directory...')
            os.makedirs(self.backupdir + '/dumps')
            print('Created')


    def clean_full_backup_dir(self):

        # Deleting full backup after taking new full backup

        dir_length = len(os.listdir(self.full_dir))

        if dir_length > 1:
            for i in os.listdir(self.full_dir):
                rm_dir = self.full_dir + '/' + i
                if i != max(os.listdir(self.full_dir)):
                    #shutil.rmtree(rm_dir)
                    os.remove(rm_dir)


    def take_dump_backup(self):

        # Defining file name based on datetime

        now = datetime.now().replace(second=0, microsecond=0)
        now = str(now)
        date1 = now[:10] + '_' + now[11:16]
        file_name = date1

        # Taking Full backup
        command = '%s %s  --result-file=%s/%s.sql' % (self.backup_tool, self.myuseroption, self.full_dir, file_name)
        #arg = shlex.split(command)
        command = shlex.split(command)

        try:
            #fb = subprocess.Popen(args, shell=True, stdout=subprocess.PIPE)
        #    print(str(fb.stdout.read()))
            #return str(fb.stdout.read())
            return subprocess.call(command)
        except Exception as err:
            err_message = "Error Occured", err
            return err_message

        #print(command)


    def all_procedures(self):
        # Create logger

        logger = Logger()
        log_it = logger.return_logger()

        # Starting Timing

        start = time.time()

        # Keep datetime when  backup starts
        now = datetime.now().replace(second=0, microsecond=0)
        now = str(now)
        date1 = now[:10] + '_' + now[11:16]

        # Calling Backup function
        log_it.info("############################################")
        log_it.info('Backup started at %s', date1)
        log_it.info(self.take_dump_backup())

        # Stopping timing
        end = time.time()

        # Keep datetime when backup stops
        now = datetime.now().replace(second=0, microsecond=0)
        now = str(now)
        date1 = now[:10] + '_' + now[11:16]

        log_it.info('Backup stopped at %s', date1)

        # Measuring time takes to take a backup

        #print(end-start)
        estimated_time = (end-start)/60
        log_it.info('Backup time %s', str(estimated_time))
        log_it.info("############################################")

        # Cleaning old backup

        self.clean_full_backup_dir()

        # Exiting and killing all subprocess

        sys.exit()



class Logger:
    """

    Class for logging backup process

    """

    def __init__(self):

        self.logger = logging.getLogger('MYSQLDUMP')
        self.logger.setLevel(logging.INFO)

        # create a file handler

        self.handler = logging.FileHandler('/home/mysql-backup/backup_dir/mysqldump_backup.log')
        self.handler.setLevel(logging.INFO)

        # create a logging format

        self.formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', "%d-%m-%Y %H:%M:%S")
        self.handler.setFormatter(self.formatter)

        # add the handlers to the logger

        self.logger.addHandler(self.handler)


    def return_logger(self):

        return self.logger




x = MysqlDumper()
x.all_procedures()
#x.take_dump_backup()

# y = Logger()
# log_it = y.return_logger()
#
# log_it.info("Fuck Emin")
