import requests
import bs4
import random

from requests.auth import HTTPBasicAuth


#  Helper methods for BS4
def html_table_to_dict_list(table, only_text=False):
	items = []
	headings = [th.get_text().lower() for th in table.find("tr").find_all("th")]
	for row in table.find_all("tr")[1:]:
		item = [td for td in row.find_all("td")]
		if only_text:
			item = [i.get_text() for i in item]
		if len(item) > 1:
			items.append(dict(zip(headings, item)))
	return items


class NoJobsForUser(Exception):
	pass


class Scrapper(object):
	""" Main class for Doopla
	"""

	def __init__(self, web_ui_url, hadoop_user, user, passwd):
		super(Scrapper, self).__init__()
		self._auth = HTTPBasicAuth(user, passwd)
		self._hadoop_user = hadoop_user
		self._web_ui_url = web_ui_url
		print(self._web_ui_url)
		self._jobtracker_url = "{}/jobtracker.jsp".format(self._web_ui_url)
		self._jobdetail_template = "{}/jobdetails.jsp?jobid=".format(self._web_ui_url) + "{}"

	def fetch_html(self, url):
		r = requests.get(url, verify=False, auth=self._auth)
		soup = bs4.BeautifulSoup(r.text, 'html.parser')
		return soup

	def scrap_last_failed_job_id(self, ):
		"""Get's the last failed job id for the user"""
		html = self.fetch_html(self._jobtracker_url)
		table = html.find(id='failed_jobs').find_next_siblings()[0]
		jobs = []

		headings = [th.get_text().lower() for th in table.find("tr").find_all("td")]

		# Get the table data from Hadoop
		for row in table.find_all("tr")[1:]:
			job = [td.get_text() for td in row.find_all("td")]
			if len(job) > 1:
				job = job[:-2]
				jobs.append(dict(zip(headings, job)))

		target = None
		for j in jobs[::-1]:
			if j['user'] == self._hadoop_user:
				target = j['jobid']
				break

		return target

	def scrap_failure_output(self, jobid):
		# Get mapper failure if any

		def build_url(jobid, kind, cause):
			template = "{}/jobfailures.jsp?jobid={}&kind={}&cause={}"
			return template.format(self._web_ui_url, jobid, kind, cause)

		err_url_map = build_url(jobid, kind="map", cause="failed")
		err_url_red = build_url(jobid, kind="reduce", cause="failed")

		map_out_url = self.scrap_output_url(err_url_map)
		red_out_url = self.scrap_output_url(err_url_red)

		map_output = ("NOT FAILED MAPPPER TASKS :)", "")
		red_output = ("NOT FAILED REDUCER TASKS :)", "")

		if map_out_url:  # No failed tasks for mappers
			map_output = self.scrap_output_from_attempt(map_out_url)
		if red_out_url:
			red_output = self.scrap_output_from_attempt(red_out_url)

		return map_output, red_output

	def scrap_output_url(self, url):
		"""
		From the tables of failures for a particular job get
		one at random and obtain the url.
		returns the url to the output of that attempt
		"""
		html = self.fetch_html(url)
		table = html.find('table')

		if not table:
			return None

		items = html_table_to_dict_list(table)
		url = None

		no_items = len(items)

		if no_items > 0:
			i = random.randint(0, no_items - 1)
			url = items[i]['logs'].contents[0].attrs['href']
			print("Scrapping Job Attempt URL: {}".format(url))

		return url

	def scrap_output_from_attempt(self, url):
		""" returns the stdout and stderr output from the error url"""
		def clean_output(output):
			lines = output.split('\n')
			if len(lines) > 3:
				lines = lines[2:]
				return "\n".join(lines)
			return output

		html = self.fetch_html(url)
		output = html.find_all('pre')
		stderr, stdout = None, None
		print(url)
		if len(output) >= 2:
			stdout = clean_output(output[0].get_text())
			stderr = clean_output(output[1].get_text())

		return stdout, stderr

	def fetch_output(self, jobid=None):
		jobid = jobid or self.scrap_last_failed_job_id()

		if not jobid:
			raise NoJobsForUser()

		self.jobid = jobid

		print("Obtaining failing output for: {}".format(jobid))

		return self.scrap_failure_output(jobid)
