from setuptools import setup, find_packages
import sys, os

version = '0.0'

setup(
	name='ckanext-datastorer',
	version=version,
	description="Tasks that upload data to the webstore.",
	long_description="""\
	""",
	classifiers=[], # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
	keywords='',
	author='CKAN team.',
	author_email='ckan-dev@okfn.org',
	url='https://github.com/okfn/ckenext-datastorer',
	license='AGPL',
	packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
	namespace_packages=['ckanext', 'ckanext.datastorer'],
	include_package_data=True,
	zip_safe=False,
    install_requires = [
        'messytables>=0.2',
        'celery>=2.4.2',
        'kombu==2.1.3',
        'kombu-sqlalchemy==1.1.0',
        'ckanext-archiver>=0.0',
    ],
	entry_points=\
	"""
    [paste.paster_command]
    datastorer = ckanext.datastorer.commands:Webstorer

    [ckan.plugins]
    datastorer = ckanext.datastorer.plugin:WebstorerPlugin

    [ckan.celery_task]
    tasks = ckanext.datastorer.celery_import:task_imports
	""",
)
