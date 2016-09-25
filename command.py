#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''doopla

Usage:
doopla [<jobid>]
doopla -h | --help
doopla --version

Options:
-h --help       Show this screen.
--version       Show version.
'''

from __future__ import unicode_literals, print_function
from docopt import docopt
from doopla.scrapper import ScrapperHadoopV1, ScrapperHadoopV2, NoJobsForUser
from colorama import init
from colorama import Fore, Back, Style

from pygments import highlight
from pygments.lexers import PythonTracebackLexer
from pygments.formatters import TerminalFormatter

import ConfigParser
import os


__version__ = "0.3.0"
__author__ = "Miguel Cabrera"
__license__ = "MIT"


def read_config():
	config = ConfigParser.ConfigParser()
	loc = os.path.expanduser("~")
	try:
		with open(os.path.join(loc, ".doopla")) as source:
			config.readfp(source)
	except IOError:
		print("Cannot read configuration files. Exiting")
		exit(-1)

	return config


def print_output(string):
	if string:
		print(highlight(string, PythonTracebackLexer(), TerminalFormatter(bg='dark')))


def main():
	init()
	args = docopt(__doc__, version=__version__)

	config = read_config()

	user = config.get('main', "hadoop_user")
	http_user = config.get('main', "http_user")
	http_passwd = config.get('main', "http_password")
	webui_url = config.get('main', "webui_url")
	hadoop_version = config.get('main', 'hadoop_version', '2')

	if hadoop_version == '1':
		Scrapper = ScrapperHadoopV1
	else:
		Scrapper = ScrapperHadoopV2

	sc = Scrapper(webui_url, user, http_user, http_passwd)


	try:
		mapper, reducer = sc.fetch_output(args['<jobid>'])

		print("\nStdout for Jobid:" + Fore.BLUE + "  {} ".format(sc.jobid))
		print(Fore.RESET + Back.RESET + Style.RESET_ALL)

		print("Mapper::Stdout \n")
		print_output(mapper[0])
		print("Mapper::Stderr \n")
		print_output(mapper[1])

		print("Reducer::Stdout \n")
		print_output(reducer[0])
		print("Reducer::Stderr \n")
		print_output(reducer[1])

	except NoJobsForUser:
		print("No failed jobs for user {}".format(user))

if __name__ == '__main__':
	main()
