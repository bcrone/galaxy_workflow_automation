import os
import sys
import string
import re
import shutil
import random
import time
import signal
import subprocess
import glob
import logging
from datetime import datetime
import smtplib
import ConfigParser
import sets
from bioblend.galaxy import GalaxyInstance
from bioblend.galaxy.histories import HistoryClient
from automation_functions import *

def main():
	# Config file
	config = ConfigParser.SafeConfigParser()
	if len(sys.argv) == 2:
		config.readfp(open(sys.argv[1]))
	else:
		config.readfp(open('kidneyseq.cfg'))

	# Get API Key to check history status
	api_key = _get_api_key(config.get('Globals', 'APIKey'))
	galaxy_host = config.get('Globals', 'GalaxyHost')
	galaxy_config = config.get('Globals', 'GalaxyConfig')
	
	# Setup logger
	logger= logging.getLogger(sys.argv[0])
	fh = logging.FileHandler(config.get('Globals', 'DownloadLogFile'))
	formatter = logging.Formatter(
		'%(asctime)s - %(name)s - %(levelname)s - %(message)s'
	)
	fh.setFormatter(formatter)
	logger.addHandler(fh)
	logger.setLevel(logging.INFO)
	logger.info('Starting %s...' % sys.argv[0])
	logger.info('Log file is %s' % config.get('Globals', 'DownloadLogFile'))

	# Check for finished KS runs to download
	already_run_f_name = config.get('Globals', 'AlreadyRunList')
	already_downloaded_f_name = config.get('Globals', 'AlreadyDownloaded')

	# Check for already downloaded samples
	try:
		logger.info('Already downloaded file is %s ' % already_downloaded_f_name)
		with open(already_downloaded_f_name) as f:
			already_downloaded_dirs = [z.strip() for z in f.readlines()]
	except IOError:
		logger.error(
			'Cannot find already-downloaded file. Check AlreadyDownloaded in '
			'kidneyseq.cfg.'
			)
		raise

	# Get list of samples to download
	try:
		logger.info('Already run file is %s ' % already_run_f_name)
		with open(already_run_f_name) as f:
			already_run_dirs = [z.strip() for z in f.readlines()]
	except IOError:
		logger.error(
			'Cannot find already-run file. Check AlreadyRunList in '
			'kidneyseq.cfg.'
			)
		raise

	# Check dirs in already run list
	for d in already_run_dirs:
		# Run has already downloaded, but still in need to download list
		if d in already_downloaded_dirs:
			logger.warn('Directory %s in already-downloaded list. Removing from list and skipping.' % d)
			update_lists(already_run_f_name, already_downloaded_f_name, d, logger)
			continue
		# Download logic here
		logger.info('Running download for sample run %s' % d)
		# Check history status
		(all_successful, all_running, all_failed, all_except, all_waiting, upload_history) = check_histories(d, api_key, galaxy_host, logger)
		logger.info('Ready to download: %s - Histories running: %s - Histories failed: %s - Histories waiting: %s' % 
			(len(all_successful), len(all_running), len(all_failed), len(all_waiting)))
		# Skip download if not all histories have completed
		if (len(all_running) > 0 or len(all_failed) > 0 or len(all_waiting) > 0):
			logger.info('Not all histories ready for download. Will try again later.')
		# Only download when all histories in JSON have completed
		else:
			ret_code = download_histories(d, galaxy_config, logger)
			logger.info('Galaxy download returned with code %s for %s' % (ret_code, d))
			update_lists(already_run_f_name, already_downloaded_f_name, d, logger)

# Pull API key from API key file
def _get_api_key(file_name):
    fh = open(file_name)
    api = fh.readline().strip('\n')
    return api

# Group histories on Galaxy by status (successful, running, failed, except, waiting)
def check_histories(run, api_key, host, logger):
	galaxy_instance = GalaxyInstance(host, key=api_key)
	history_client = HistoryClient(galaxy_instance)	
	history_json_d = run + '/output'
	histories = read_all_histories(history_json_d, logger)
	(all_successful, all_running, all_failed, all_except, all_waiting, upload_history) = get_history_status(histories, history_client, logger)
	return (all_successful, all_running, all_failed, all_except, all_waiting, upload_history)

# Download completed histories
def download_histories(run, config, logger):
	history_json_d = run + '/output'
	download_d = run + '/results'
	command_string = 'python history_utils.py %s download -d -o %s -i %s' % (history_json_d, download_d, config)
	ret_code = subprocess.call(command_string, shell=True)
	return ret_code

# Update to_download and done lists
def update_lists(already_run_file, already_downloaded_file, run, logger):

	logger.info('Updating run lists')

	# Read to_download and downloaded files to lists
	# Find intersect between lists - these have downloaded
	with open(already_run_file, 'r') as f:
		run_list = f.read().splitlines()
	with open(already_downloaded_file, 'r') as f:
		downloaded_list = f.read().splitlines()
	intersect = list(set(run_list) & set(downloaded_list))

	# Reopen files for writeback/appending
	already_run_f = open(already_run_file, 'w')
	already_downloaded_f = open(already_downloaded_file, 'a')

	# Check to see if run is in to_download list (it is - file run is pulled from)
	for line in run_list:
		# Remove run from to_download list
		if line == run:
			if line in intersect:
				logger.warn('Directory %s already-downloaded. Skipping.' % line)
				continue
			logger.info('Adding directory %s to already-downloaded' % line)
			already_downloaded_f.write(line+'\n')
		else:
			logger.info('%s not downloaded yet. Writing to run file.' % line)
			already_run_f.write(line+'\n')

	already_run_f.close()
	already_downloaded_f.close()


if __name__ == '__main__':
	sys.exit(main())