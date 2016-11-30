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

def main():
	# Config file
	config = ConfigParser.SafeConfigParser()
	if len(sys.argv) == 2:
		config.readfp(open(sys.argv[1]))
	else:
		config.readfp(open('kidneyseq.cfg'))

	# Setup logger	
	logger = logging.getLogger(sys.argv[0])
	fh = logging.FileHandler(config.get('Globals', 'WorkflowLogFile'))
	formatter = logging.Formatter(
		'%(asctime)s - %(name)s - %(levelname)s - %(message)s'
	)
	fh.setFormatter(formatter)
	logger.addHandler(fh)
	logger.setLevel(logging.INFO)
	logger.info('Starting %s...' % sys.argv[0])
	logger.info('Log file is %s' % config.get('Globals', 'WorkflowLogFile'))

	# Check for eligible KS samples
	input_ks_dir_name  = config.get('Globals', 'InputKSDirectory')
	already_run_f_name = config.get('Globals', 'AlreadyRunList')
	already_downloaded_f_name = config.get('Globals', 'AlreadyDownloaded')
	# Check for already run samples, but still need to download
	try:
		logger.info('Already run file is %s ' % already_run_f_name)
		with open(already_run_f_name) as f:
			already_run_dirs = [z.strip() for z in f.readlines()]
	except IOError:
		logger.error(
			'Cannot find already-run file. Check AlreadyRunList in '
			'kidseq.cfg.'
			)
		raise
	# Check for already run samples that have downloaded
	try:
		logger.info('Already downloaded file is %s ' % already_downloaded_f_name)
		with open(already_downloaded_f_name) as f:
			already_downloaded_dirs = [z.strip() for z in f.readlines()]
	except IOError:
		logger.error(
			'Cannot find already-downloaded file. Check Already downloaded in '
			'kidseq.cfg.'
			)
		raise
	# Check for samples in root sample directory
	try:
		input_ks_dirs = os.listdir(input_ks_dir_name)
	except OSError:
		logger.error(
			'Cannot find read directory. Check InputKSDirectory in '
			'kidseq.cfg.'
			)
		raise
	# Check through sample directories under root directory
	for d in input_ks_dirs:
		d = os.path.join(input_ks_dir_name, d)
		# Directory has run, but needs to download
		if d in already_run_dirs:
			logger.warn('Directory %s in already-run list, but needs to download. Skipping.' % d)
			continue
		logger.info('Processing %s...' % d)
		# Directory has run and downloaded
		if d in already_downloaded_dirs:
			logger.warn('Directory %s in already-downloaded list. Skipping.' % d)
			continue

		if config.get('find_eligible_runs', 'Locked') == 'True':
			logger.warn('Lock set. Not able to process %s.' % d)
			continue
		config.set('find_eligible_runs', 'Locked', 'True')

		looks_like_ks_dir = has_correct_ks_dir_form(d, logger)
		if not looks_like_ks_dir:
			logger.info(
				'Does not look like KidneySeq directory. Adding %s to already-downloaded file (%s).' %
				(d, already_downloaded_f_name)
			)
			with open(already_downloaded_f_name, 'a') as f:
				f.write('%s\n' % d)
			continue

		[results_path, output_path] = set_output_results_dir(d, logger)
		ret_code = galaxy_workflow_runner(d, output_path, logger)
		logger.info('Galaxy returned code %s for path %s' % (ret_code, d))

		with open(already_run_f_name, 'a') as f:
			f.write('%s\n' % d)

		config.set('find_eligible_runs', 'Locked', 'False')

def set_output_results_dir(input_dir, logger):
	results_path = input_dir + '/results'
	output_path = input_dir + '/output'
	logger.info('Results path is %s' % results_path)
	logger.info('Output path is %s' % output_path)
	if not os.path.exists(results_path):
		try:
			os.makedirs(results_path)
			logger.info('Results directory %s created' % results_path)
		except IOError:
			logger.error('Failed to create results directory %s' % results_path)
	else:
		logger.warn('Results directory %s already exists' % results_path)
	if not os.path.exists(output_path):
		try:
			os.makedirs(output_path)
			logger.info('Output directory %s created' % output_path)
		except IOError:
			logger.error('Failed to create output directory %s' % output_path)
	else:
		logger.warn('Output directory %s already exists' % output_path)
	return [results_path, output_path]


def galaxy_workflow_runner(input_dir, output_dir, logger):

	galaxy_command_string = 'python workflow_runner.py %s -o %s -i %s' % (
		input_dir, output_dir, config.get('Globals','GalaxyConfig'))
	logger.info('Attempting Galaxy run for %s. Galaxy logs written to %s' % (input_dir, output_dir))
	ret_code = subprocess.call(galaxy_command_string, shell=True)

	return ret_code


def has_correct_ks_dir_form(dir_name, logger):

	# directory has form yyyymmdd-KSxx (xx is 2-digit number)
	dir_name = os.path.basename(dir_name)
	dir_regex = re.compile('[0-9]{8}-KS[0-9]{2}')
	match = dir_regex.match(dir_name)
	if match:
		looks_like_ks_dir = True
		logger.info('Directory %s looks like a KidneySeq directory.' % dir_name)
	else:
		looks_like_ks_dir = False
		logger.info('Directory %s does not look like a KidneySeq directory.' % dir_name)
	return looks_like_ks_dir



if __name__ == '__main__':
	sys.exit(main())