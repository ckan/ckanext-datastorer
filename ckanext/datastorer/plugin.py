from pylons import config
from ckan import model
from ckan.model.types import make_uuid
from ckan.plugins import (SingletonPlugin, implements,
                          IDomainObjectModification,
                          IResourceUrlChange, IConfigurable)
from ckan.logic import get_action
from ckan.lib.celery_app import celery
import ckan.lib.helpers as h
from ckan.lib.dictization.model_dictize import resource_dictize
import json
from datetime import datetime
from logging import getLogger


logger = getLogger('ckanext_datastorer')


class DatastorerPlugin(SingletonPlugin):
    """
    Registers to be notified whenever CKAN resources are created or their
    URLs change, and will create a new ckanext.datastorer celery task to
    put the resource in the datastore.
    """
    implements(IDomainObjectModification, inherit=True)
    implements(IResourceUrlChange)
    implements(IConfigurable, inherit=True)

    def notify(self, entity, operation=None):
        if not isinstance(entity, model.Resource):
            return
        if operation:
            if operation == model.domain_object.DomainObjectOperation.new:
                self._create_datastorer_task(entity)
        else:
            # if operation is None, resource URL has been changed, as the
            # notify function in IResourceUrlChange only takes 1 parameter
            self._create_datastorer_task(entity)

    def configure(self, config):
        
        try:
            sample_size = int(config.get('ckanext.datastorer.sample_size'))
        except:
            self.sample_size = None
        else:
            self.sample_size = sample_size 

    def _get_site_url(self):
        try:
            return h.url_for_static('/', qualified=True)
        except AttributeError:
            return config.get('ckan.site_url', '')

    def _create_datastorer_task(self, resource):
        user = get_action('get_site_user')({'model': model,
                                            'ignore_auth': True,
                                            'defer_commit': True}, {})

        context = {
            'site_url': self._get_site_url(),
            'apikey': user.get('apikey'),
            'site_user_apikey': user.get('apikey'),
            'username': user.get('name'),
        }
        
        if self.sample_size:
            context['sample_size'] = self.sample_size
        
        data = resource_dictize(resource, {'model': model})

        task_id = make_uuid()
        datastorer_task_status = {
            'entity_id': resource.id,
            'entity_type': u'resource',
            'task_type': u'datastorer',
            'key': u'celery_task_id',
            'value': task_id,
            'last_updated': datetime.now().isoformat()
        }
        archiver_task_context = {
            'model': model,
            'user': user.get('name'),
        }
        get_action('task_status_update')(archiver_task_context,
                                         datastorer_task_status)
        celery.send_task("datastorer.upload",
                         args=[json.dumps(context), json.dumps(data)],
                         task_id=task_id)
