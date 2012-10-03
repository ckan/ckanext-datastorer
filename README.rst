CKAN Datastorer Extension
=======================

The CKAN Datastorer Extension provides a Celery task for automatically
 saving CKAN resources that link to csv and excel files into the datastore.


Installation
------------

After activating your pyenv, install the sources via pip::

    $ (pyenv) pip install -e git+git://github.com/okfn/ckanext-datastorer.git#egg=ckanext-datastorer

Install the requirements::

    $ (pyenv) pip install -r ckanext-datastorer/pip-requirements.txt

Add the datastorer plugin to your configuration ini file::

    ckan.plugins = datastorer <rest of plugins>...

Start the celery daemon.  This can be done in development by::

    paster celeryd # this is assuming a development.ini file

In production the daemon should be run with a different ini file and be run as an init script.
The simplest way to do this is to install supervisor::

    apt-get install supervisor

You can use this file as a template and add it to /etc/supservisor/conf.d:

    https://github.com/okfn/ckan/blob/master/ckan/config/celery-supervisor.conf

Paster Command
--------------

A paster command is available, that lets you archive all resources or just those belonging to a specific package. The command is as follows::

	paster datastorer update [package-id]


Developers
----------

You can run the test suite from the ckanext-datastorer directory.
The tests require nose, so install it first if you have not already
done so:

::

   $ pip install nose

To run the tests, you will need to be running a CKAN instance, and provide
the API key of a sysadmin user on the tests configuration file located on::

    ckanext/datastorer/tests/tests_config.cfg

**Note:** Make sure that celery is not running during the tests. Otherwise strange errors will occur!

Then, run nosetests from the ckanext-datastorer directory

::

   $ nosetests ckanext/datastorer/tests