import requests
from xml.etree import ElementTree

class Pyregrine(object):
    """
    A Python wrapper for Apache Falcon's RESTful API. Uses the external library 'Requests'. 
    
    @author: James Barney
    @version: 0.1
    @since: 6 DECEMBER 2014
    """
    
    def __init__(self, host="http://127.0.0.1", port="15000", path="api/"):
        self.host = host
        self.port = port
        self.path = path
        self.master_url = self.host+":"+self.port+"/"+self.path
        self.credentials = {"user.name":"admin", "password":"admin"}
        
        self.request = requests.Session()
    
    ##### Generic request methods below ########
    def _get(self, path, params):
        return self.request.get(self.master_url+path, params=params)

    
    def _post(self, path, params):
        return self.request.post(self.master_url+path, params=params)

    def _delete(self, path, params):
        return self.request.delete(self.master_url+path, params=params)

    #### Admin Resources ####
    def get_stack(self):
        """
        GET    api/admin/stack    Get stack of the server
        """
        return self._get("admin/stack", self.credentials)
    
    def get_version(self):
        """
        GET    api/admin/version    Get version of the server
        """
        return self._get("admin/version", self.credentials)
    
    def get_config(self, config_type):
        """
        GET    api/admin/config/:config-type    Get configuration information of the server
        :config-type can be build, deploy, startup or runtime.
        """
        return self._get("admin/config/"+config_type, self.credentials)
    
    #### Entity Resources ####
    def validate_entity(self, entity_type):
        """
        POST    api/entities/validate/:entity-type    Validate the entity
        :entity-type can be cluster, feed or process.

        """
        return self._post("entities/validate/"+entity_type, self.credentials)

    
    def submit_entity(self, entity_type):
        """
        POST    api/entities/submit/:entity-type    Submit the entity
        :entity-type can be cluster, feed or process.

        """
        return self._post("entities/submit/"+entity_type, self.credentials)
    
    def update_entity(self, entity_type, entity_name, effective=None):
        """
        POST    api/entities/update/:entity-type/:entity-name    Update the entity
        :entity-type can be cluster, feed or process.
        :entity-name is name of the feed or process.
        :effective is optional effective time

        """
        if effective is None:
            return self._post("entities/update/"+entity_type+"/"+entity_name, self.credentials)
        else:
            return self._post("entities/update/"+entity_type+"/"+entity_name+"?"+effective, self.credentials)

    
    def submit_and_schedule_entity(self, entity_type):
        """
        POST    api/entities/submitAndSchedule/:entity-type    Submit & Schedule the entity
        :entity-type can either be a feed or a process.

        """
        return self._post("entities/submitAndSchedule/"+entity_type, self.credentials)
    
    def schedule_entity(self, entity_type, entity_name):
        """
        POST    api/entities/schedule/:entity-type/:entity-name    Schedule the entity

        """
        return self._post("entities/schedule/"+entity_type+"/"+entity_name, self.credentials)
    
    def suspend_entity(self, entity_type, entity_name):
        """
        POST    api/entities/suspend/:entity-type/:entity-name    Suspend the entity

        """
        return self._post("entities/suspend/"+entity_type+"/"+entity_name, self.credentials)
    
    def resume_entity(self, entity_type, entity_name):
        """
        POST    api/entities/resume/:entity-type/:entity-name    Resume the entity

        """
        return self._post("entities/resume/"+entity_type+"/"+entity_name, self.credentials)
    
    def delete_entity(self, entity_type, entity_name):
        """
        DELETE    api/entities/delete/:entity-type/:entity-name    Delete the entity

        """
        return self._delete("entities/delete/"+entity_type+"/"+entity_name, self.credentials)


    def get_entity_status(self, entity_type, entity_name):
        """
        GET    api/entities/status/:entity-type/:entity-name    Get the status of the entity

        """
        return self._get("entities/status/"+entity_type+"/"+entity_name, self.credentials)

    
    def get_entity_definition(self, entity_type, entity_name):
        """
        GET    api/entities/definition/:entity-type/:entity-name    Get the definition of the entity

        """
        return self._get("entities/definition/"+entity_type+"/"+entity_name, self.credentials)
    
    def get_entity_list(self, entity_type):
        """
        GET    api/entities/list/:entity-type    Get the list of entities

        """
        return self._get("entities/list/"+entity_type, self.credentials)
    
    def get_entity_summary(self, entity_type, cluster):
        """
        GET    api/entities/summary/:entity-type/:cluster    Get instance summary of all entities

        """
        return self._get("entities/summary/"+entity_type+"/"+cluster, self.credentials)

    
    def get_entity_dependencies(self, entity_type, entity_name):
        """
        GET    api/entities/dependencies/:entity-type/:entity-name    Get the dependencies of the entity

        """
        return self._get("entities/dependencies/"+entity_type+"/"+entity_name, self.credentials)

    
    #### Feed and Process Resources ####
    def get_running_instances(self, entity_type, entity_name):
        """
        GET    api/instance/running/:entity-type/:entity-name    List of running instances.

        """
        return self._get("instance/running/"+entity_type+"/"+entity_name, self.credentials)
    
    def get_list_instances(self, entity_type, entity_name):
        """
        GET    api/instance/list/:entity-type/:entity-name    List of instances

        """
        return self._get("instance/running/"+entity_type+"/"+entity_name, self.credentials)
    
    def get_status_instance(self, entity_type, entity_name):
        """
        GET    api/instance/status/:entity-type/:entity-name    Status of a given instance

        """
        return self._get("instance/status/"+entity_type+"/"+entity_name, self.credentials)
    
    def kill_instance(self, entity_type, entity_name):
        """
        POST    api/instance/kill/:entity-type/:entity-name    Kill a given instance

        """
        return self._post("instance/kill/"+entity_type+"/"+entity_name, self.credentials)
    
    def suspend_instance(self, entity_type, entity_name):
        """
        POST    api/instance/suspend/:entity-type/:entity-name    Suspend a running instance

        """
        return self._post("instance/suspend/"+entity_type+"/"+entity_name, self.credentials)
    
    def resume_instance(self, entity_type, entity_name):
        """
        POST    api/instance/resume/:entity-type/:entity-name    Resume a given instance

        """
        return self._post("instance/resume/"+entity_type+"/"+entity_name, self.credentials)
    
    def rerun_instance(self, entity_type, entity_name):
        """
        POST    api/instance/rerun/:entity-type/:entity-name    Rerun a given instance

        """
        return self._post("instance/rerun/"+entity_type+"/"+entity_name, self.credentials)
    
    def get_instance_logs(self, entity_type, entity_name):
        """
        GET    api/instance/logs/:entity-type/:entity-name    Get logs of a given instance

        """
        return self._get("instance/logs/"+entity_type+"/"+entity_name, self.credentials)
    
    def get_instance_summary(self, entity_type, entity_name):
        """
        GET    api/instance/summary/:entity-type/:entity-name    Return summary of instances for an entity

        """
        return self._get("instance/summary/"+entity_type+"/"+entity_name, self.credentials)

    
    #### Linage Graph Resources ####
    def serialize_graph(self, entity_type, entity_name):
        """
        GET    api/graphs/lineage/serialize    dump the graph

        """
        return self._get("graphs/lineage/"+entity_type+"/"+entity_name, self.credentials)

    
    def get_all_vertices(self):
        """
        GET    api/graphs/lineage/vertices/all    get all vertices

        """
        return self._get("graphs/lineage/vertices/all", self.credentials)

    
    def get_kv_vertex(self, key, value):
        """
        GET    api/graphs/lineage/vertices?key=:key&value=:value    get all vertices for a key index

        """
        return self._get("graphs/lineage/vertices?key="+key+"&value="+value, self.credentials)
    
    def get_vertex(self, vertex_id):
        """
        GET    api/graphs/lineage/vertices/:id    get the vertex with the specified id

        """
        return self._get("graphs/lineage/vertices/"+vertex_id, self.credentials)
    
    def get_vertex_properties(self, vertex_id, relationships=False):
        """
        GET    api/graphs/lineage/vertices/properties/:id?relationships=:true    get the properties of the vertex with the specified id

        """
        return self._get("graphs/lineage/vertices/properties/"+vertex_id+"?relationships="+relationships, self.credentials)

    
    def get_adjacent_vertices(self, vertex_id, direction):
        """
        GET    api/graphs/lineage/vertices/:id/:direction    get the adjacent vertices or edges of the vertex with the specified direction

        """
        return self._get("graphs/lineage/vertices/"+vertex_id+"/"+direction, self.credentials)
    
    def get_all_edges(self):
        """
        GET    api/graphs/lineage/edges/all    get all edges

        """
        return self._get("graphs/lineage/vertices/edges/all", self.credentials)

    
    def get_edge(self, vertex_id):
        """
        GET    api/graphs/lineage/edges/:id    get the edge with the specified id
        """
        return self._get("graphs/lineage/vertices/"+vertex_id, self.credentials)
    
    
if __name__ == "__main__":
    peregrine = Pyregrine()
    
    feeds = peregrine.get_entity_list("feed")
    print feeds.text

    tree = ElementTree.fromstring(feeds.text)
    
    print tree.findall('.//')
    
    for elem in tree.findall(".//"):
        print elem.tag
        print elem.text
        print

    clusters= peregrine.get_entity_list("cluster")
    
    tree = ElementTree.fromstring(clusters.text)
    
    print tree.findall('.//')
    
    for elem in tree.findall(".//"):
        print elem.tag
        print elem.text
        print
    