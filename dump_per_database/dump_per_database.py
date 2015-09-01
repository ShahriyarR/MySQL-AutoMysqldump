#!/opt/Python-3.3.2/bin/python3

import configparser
import subprocess
import shlex
import os
from datetime import datetime
import time
import logging
import sys
import re


class MysqlDumper:
    """

    Class for using mysqldump utility.

    """

    def __init__(self, conf='/home/MySQL-AutoMysqldump/dump_per_database/bck.conf'):

        con = configparser.ConfigParser()
        con.read(conf)
        bolme = con.sections()

        DB = bolme[0]
        self.mysql = con[DB]['mysql']
        self.mycnf = con[DB]['mycnf']
        self.myuseroption = con[DB]['useroption']

        self.password_reg = re.search(r'\-\-password\=(.*)[\s]--single', self.myuseroption)
        self.user_reg = re.search(r'\-\-user\=(.*)[\s]--password', self.myuseroption)

        self.password = self.password_reg.group(1)
        self.user = self.user_reg.group(1)



        BCK = bolme[1]
        self.backupdir = con[BCK]['backupdir']
        self.full_dir = self.backupdir + '/per_database'
        self.backup_tool = con[BCK]['backup_tool']


        if not (os.path.exists(self.full_dir)):
            print('Full directory is not exist. Creating full backup directory...')
            os.makedirs(self.backupdir + '/per_database')
            print('Created')
        else:
            print("Full Directory exists")

        self.database_names = ['test',
                               'mysql']

        for i in self.database_names:
            if not os.path.exists(self.full_dir+'/'+i):
                print("Folders for database not exist. Will create...")
                os.makedirs(self.full_dir+'/'+i)
                print("Created")



    def clean_per_database_dir(self, database_name):

        # Deleting old backups in per_database directories after taking new one

        dir_length = len(os.listdir(self.full_dir+'/'+database_name))

        if dir_length > 1:
            for x in os.listdir(self.full_dir+'/'+database_name):
                rm_file = self.full_dir+'/'+database_name+'/'+x
                if x != max(os.listdir(self.full_dir+'/'+database_name)):
                    os.remove(rm_file)





    def take_dump_backup(self, database_name):

        # Defining file name based on datetime

        now = datetime.now().replace(second=0, microsecond=0)
        now = str(now)
        date1 = now[:10] + '_' + now[11:16]
        file_name = date1 + '_' + database_name

        # Taking Full backup
        command = "%s --user=%s --password='%s'  --databases %s --result-file=%s/%s.sql" % \
                                                                    (self.backup_tool,
                                                                     self.user,
                                                                     self.password,
                                                                     database_name,
                                                                     self.full_dir+'/'+database_name, file_name)
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



        # Taking Database names for Per-Database backup
        for i in self.database_names:


            log_it.info("############################################")
            log_it.info('Backup started at %s for %s', date1, i)

            # Calling Backup function
            db_name = "Backing up %s" % i
            print(db_name)
            log_it.info(self.take_dump_backup(i))

            # Stopping timing
            end = time.time()

            # Keep datetime when backup stops
            now = datetime.now().replace(second=0, microsecond=0)
            now = str(now)
            date1 = now[:10] + '_' + now[11:16]

            log_it.info('Backup stopped at %s for %s', date1, i)

            # Measuring time takes to take a backup

            #print(end-start)
            estimated_time = (end-start)/60
            log_it.info('Backup time %s', str(estimated_time))
            log_it.info("############################################")

            # Cleaning old backup

            self.clean_per_database_dir(i)

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

        self.handler = logging.FileHandler('/home/MySQL-AutoMysqldump/backup_dir/per_database/mysqldump_backup.log')
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
