### ===========================================================================
### This file comes from the OSM archived, and remasterd for the purposes of
### this project. See the original here:
### https://github.com/openstreetmap/svn-archive/blob/main/applications/utils/import/bulk_upload_06/bulk_upload.py
### ===========================================================================

import xml.etree.ElementTree as xmlet
import httplib2
import pickle
import os
import sys
import pygraph
import logging

USER_AGENT = f"bulk_upload.py Python/{sys.version.split()[0]}"

OSM_API_HOST = os.getenv("OSM_API_HOST")
HEADERS = {
    "User-Agent": USER_AGENT,
}

logger = logging.getLogger(__name__)


class XMLException(Exception):
    pass


class APIError(Exception):
    pass


class ImportProcessor:
    currentChangeset = None
    idMap = None

    def __init__(self, user, password, idMap, tags={}):
        self.httpObj = httplib2.Http()
        self.httpObj.add_credentials(user, password)
        self.idMap = idMap
        self.tags = tags
        self.createChangeset()

    def parse(self, infile):
        relationStore = {}
        relationSort = False

        osmData = xmlet.parse(infile)
        osmRoot = osmData.getroot()
        if osmRoot.tag != "osm":
            raise XMLException("Input file must be a .osm XML file (JOSM-style)")

        # Add a very loud warning to people who try to force osmChange files through
        for zomgOsmChange in ("add", "delete", "modify"):
            for arglh in osmRoot.getiterator(zomgOsmChange):
                raise XMLException(
                    f"You are processing an osmChange file with a <osm> root element. OSM FILES HOWEVER DO NOT HAVE <{zomgOsmChange}> ELEMENTS. YOU ARE PROBABLY TRYING TO UPLOAD A OSM CHANGE FILE DIRECTLY *DON'T DO THIS* IT WILL BREAK THINGS ON THE SERVER AND TOM HUGHES WILL EAT YOUR FAMILY (YES REALLY)"
                )

        for elem in osmRoot.getiterator("member"):
            if elem.attrib["type"] == "relation":
                relationSort = True
                break

        for type in ("node", "way"):
            for elem in osmRoot.getiterator(type):
                # If elem.id is already mapped we can skip this object
                #
                id = elem.attrib["id"]
                if self.idMap[type].has_key(id):
                    continue
                #
                # If elem contains nodes, ways or relations as a child
                # then the ids need to be remapped.
                if elem.tag == "way":
                    count = 0
                    for child in elem.getiterator("nd"):
                        count = count + 1
                        if count > 2000:
                            raise XMLException(
                                f"node count >= 2000 in <{elem.attrib['id']}>"
                            )
                        if child.attrib.has_key("ref"):
                            old_id = child.attrib["ref"]
                            if self.idMap["node"].has_key(old_id):
                                child.attrib["ref"] = self.idMap["node"][old_id]

                self.addToChangeset(elem)

        for elem in osmRoot.getiterator("relation"):
            if relationSort:
                relationStore[elem.attrib["id"]] = elem
            else:
                if self.idMap["relation"].has_key(elem.attrib["id"]):
                    continue
                else:
                    self.updateRelationMemberIds(elem)
                    self.addToChangeset(elem)

        if relationSort:
            gr = pygraph.digraph()
            gr.add_nodes(relationStore.keys())
            for id in relationStore:
                for child in relationStore[id].getiterator("member"):
                    if child.attrib["type"] == "relation":
                        gr.add_edge(id, child.attrib["ref"])

            # Tree is unconnected, hook them all up to a root
            gr.add_node("root")
            for item in gr.node_incidence.iteritems():
                if not item[1]:
                    gr.add_edge("root", item[0])
            for relation in gr.traversal("root", "post"):
                if relation == "root":
                    continue
                r = relationStore[relation]
                if self.idMap["relation"].has_key(r.attrib["id"]):
                    continue
                self.updateRelationMemberIds(r)
                self.addToChangeset(r)

        self.currentChangeset.close()  # (uploads any remaining diffset changes)

    def updateRelationMemberIds(self, elem):
        for child in elem.getiterator("member"):
            if child.attrib.has_key("ref"):
                old_id = child.attrib["ref"]
                old_id_type = child.attrib["type"]
                if self.idMap[old_id_type].has_key(old_id):
                    child.attrib["ref"] = self.idMap[old_id_type][old_id]

    def createChangeset(self):
        self.currentChangeset = Changeset(
            tags=self.tags, idMap=self.idMap, httpObj=self.httpObj
        )

    def addToChangeset(self, elem):
        if elem.attrib.has_key("action"):
            action = elem.attrib["action"]
        else:
            action = "create"

        try:
            self.currentChangeset.addChange(action, elem)
        except ChangesetClosed:
            self.createChangeset()
            self.currentChangeset.addChange(action, elem)


class IdMap:
    # Default IdMap class, using a Pickle backend, this can be extended
    # - if ids in other files need replacing, for example
    idMap = {"node": {}, "way": {}, "relation": {}}

    def __init__(self, filename=""):
        self.filename = filename
        self.load()

    def __getitem__(self, item):
        return self.idMap[item]

    def load(self):
        try:
            with open(self.filename, "r") as f:
                self.idMap = pickle.load(f)
        except Exception:
            pass

    def save(self):
        with open(f"{self.filename}.tmp", "w") as f:
            pickle.dump(self.idMap, f)
        try:
            os.remove(self.filename)
        except Exception:
            pass
        os.rename(f"{self.filename}.tmp", self.filename)


class ChangesetClosed(Exception):
    pass


class Changeset:
    id = None
    tags = {}
    currentDiffSet = None
    opened = False
    closed = False

    itemcount = 0

    def __init__(self, tags={}, idMap=None, httpObj=None):
        self.id = None
        self.tags = tags
        self.idMap = idMap
        self.httpObj = httpObj

        self.createDiffSet()

    def open(self):
        createReq = xmlet.Element("osm", version="0.6")
        change = xmlet.SubElement(createReq, "changeset")
        for tag in self.tags:
            xmlet.SubElement(change, "tag", k=tag, v=self.tags[tag])

        xml = xmlet.tostring(createReq)
        resp, content = self.httpObj.request(
            f"{OSM_API_HOST}/api/0.6/changeset/create", "PUT", xml, headers=HEADERS
        )
        if resp.status != 200:
            raise APIError(f"Error creating changeset: {str(resp.status)}")
        self.id = content
        logger.info(f"Created changeset: {self.id}")
        self.opened = True

    def close(self):
        if not self.opened:
            return
        self.currentDiffSet.upload()

        resp, content = self.httpObj.request(
            f"{OSM_API_HOST}/api/0.6/changeset/{self.id}/close",
            "PUT",
            headers=HEADERS,
        )
        if resp.status != 200:
            logger.info(f"Error closing changeset {str(self.id)}:{str(resp.status)}")
        logger.info(f"Closed changeset: {self.id}")
        self.closed = True

    def createDiffSet(self):
        self.currentDiffSet = DiffSet(self, self.idMap, self.httpObj)

    def addChange(self, action, item):
        if not self.opened:
            self.open()  # So that a changeset is only opened when required.
        if self.closed:
            raise ChangesetClosed
        item.attrib["changeset"] = self.id
        try:
            self.currentDiffSet.addChange(action, item)
        except DiffSetClosed:
            self.createDiffSet()
            self.currentDiffSet.addChange(action, item)

        self.itemcount += 1
        if self.itemcount >= self.getItemLimit():
            self.currentDiffSet.upload()
            self.close()

    def getItemLimit(self):
        # This is actually dictated by the API's capabilities call
        return 50000


class DiffSetClosed(Exception):
    pass


class DiffSet:
    itemcount = 0
    closed = False

    def __init__(self, changeset, idMap, httpObj):
        self.elems = {
            "create": xmlet.Element("create"),
            "modify": xmlet.Element("modify"),
            "delete": xmlet.Element("delete"),
        }
        self.changeset = changeset
        self.idMap = idMap
        self.httpObj = httpObj

    def __getitem__(self, item):
        return self.elems[item]

    def addChange(self, action, item):
        if self.closed:
            raise DiffSetClosed
        self[action].append(item)

        self.itemcount += 1
        if self.itemcount >= self.getItemLimit():
            self.upload()

    def upload(self):
        if not self.itemcount or self.closed:
            return False

        xml = xmlet.Element("osmChange")
        for elem in self.elems.values():
            xml.append(elem)
        logger.info(f"Uploading to changeset {self.changeset.id}")

        xmlstr = xmlet.tostring(xml)

        resp, content = self.httpObj.request(
            f"{OSM_API_HOST}/api/0.6/changeset/{self.changeset.id}/upload",
            "POST",
            xmlstr,
            headers=HEADERS,
        )
        if resp.status != 200:
            logger.info(f"Error uploading changeset: {str(resp.status)}")
            logger.info(content)
            exit(-1)
        else:
            self.processResult(content)
            self.idMap.save()
            self.closed = True

    def processResult(self, content):
        """
        Uploading a diffset returns a <diffResult> containing elements
        that map the old id to the new id
        Process them.
        """
        diffResult = xmlet.fromstring(content)
        for child in diffResult.getchildren():
            id_type = child.tag
            old_id = child.attrib["old_id"]
            if child.attrib.has_key("new_id"):
                new_id = child.attrib["new_id"]
                self.idMap[id_type][old_id] = new_id
            else:
                # (Object deleted)
                self.idMap[id_type][old_id] = old_id

    def getItemLimit(self):
        # This is an arbitrary self-imposed limit (that must be below the changeset limit)
        # so to limit upload times to sensible chunks.
        return 1000


class BulkUpload:
    """
    Bulk uploads a changeset to the OSM server.
    API v0.6 compatible.
    """

    def __init__(self, input_path, comment, user, password):
        self.input_path = input_path
        self.comment = comment
        self.user = user
        self.password = password
        idMap = IdMap(input_path + ".db")
        tags = {"created_by": USER_AGENT, "comment": comment}
        importProcessor = ImportProcessor(user, password, idMap, tags)
        importProcessor.parse(input_path)
