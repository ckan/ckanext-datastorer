from setuptools import setup, find_packages
import sys, os

version = '0.0'

setup(
	name='ckanext-webstorer',
	version=version,
	description="Tasks that upload data to the webstore.",
	long_description="""\
	""",
	classifiers=[], # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
	keywords='',
	author='CKAN team.',
	author_email='ckan-dev@okfn.org',
	url='https://github.com/okfn/ckenext-webstorer',
	license='AGPL',
	packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
	namespace_packages=['ckanext', 'ckanext.webstorer'],
	include_package_data=True,
	zip_safe=False,
    install_requires = [
        'messytables>=0.1.1',
        'celery>=2.4.2',
        'kombu-sqlalchemy==1.1.0',
    ],
	entry_points=\
	"""
    [paste.paster_command]
    webstorer = ckanext.webstorer.commands:Webstorer

    [ckan.plugins]
    webstorer = ckanext.webstorer.plugin:WebstorerPlugin

    [ckan.celery_task]
    tasks = ckanext.webstorer.celery_import:task_imports
	""",
)
