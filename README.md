doopla
===============================
H(ad)oopla!
A Python script to fetch the output of failed Python Hadoop streaming jobs. It scraps
the hadoop web interface and gets a random failed mapper and reducer task. It outputs it with
code highlighting for easy reading.


    doopla -h

    Usage:
    doopla [<jobid>]
    doopla -h | --help
    doopla --version

    Options:
    -h --help       Show this screen.
    --version       Show version.


Features
--------
* Automatically get the last failed job for a user
* Code highlighting via `Pygments`.

Install
------
Two options for installing:

*Via Pip::*

    pip install doopla

*git clone and setup.py*:

    git clone git@github.com:trustyou/doopla.git
    cd doopla
    python setup.py install

Usage
-----
Before using `doopla` please create a file in your home directory called `.doopla` and add
the follwoing:


    [main]
    hadoop_version: <HADOOP_VERSION> # either 1 or 2 - defaults to 2
    hadoop_user: <HADOOP_USER>
    hadoop_url: <HADOOP_URL> # For Hadoop 2.x use the Job history URL
    http_user: <USER>
    http_password: <THE_PASSWORD>

Replace `HADOOP_URL` for the HTTP URL of your the Hadoop Web interface. Replace `HADOOP_USER` for your hadoop user (or the one you want to check) and the  `HTTP_PASSWORD` for the http password you normally use to log into the web interface.

The is simple a mather of executing


    $ doopla

It will search for the most recently failed job and get the output.

Or

    $ doopla JOB_ID

If you want to get the output of a specific job.

You can also add `2>/dev/null` if you want to shut down the HTTPS certificate warnings.

Screenshot
----------

![alt text](https://www.dropbox.com/s/at10xpaut2xz2iw/sample.png?raw=1)



Development
-----------
This is a 4 hours hack while skipping lunch and waiting for a job to finish so it is in alpha
stage and it is full of bugs. So feel free to create pull requests if you see something
that can be improved.


Requirements
------------
- Python >= 2.6 or >= 3.3
- Colorama
- BeautifulSoup
- Requests
- Pygments

License
-------

MIT licensed. See the bundled `LICENSE <https://github.com/mfcabrera/doopla/blob/master/LICENSE>`_ file for more details.
