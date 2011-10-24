from ckan import model
from ckan.plugins import SingletonPlugin, implements, IDomainObjectModification, IResourceUrlChange
from celery.execute import send_task

class WebstorerPlugin(SingletonPlugin):
    """
    Registers to be notified whenever CKAN resources are created or their URLs change,
    and will create a new ckanext.archiver celery task to archive the resource.
    """
    implements(IDomainObjectModification, inherit=True)
    implements(IResourceUrlChange)
    implements(IConfigurable)

    def notify(self, entity, operation=None):
        if not isinstance(entity, model.Resource):
            return

    def configure(self, config):
        self.site_url = config.get('ckan.site_url')

        if operation:
            if operation == model.DomainObjectOperation.new:
                send_task("archiver.update", [entity.id])

